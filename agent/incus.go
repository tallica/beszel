package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/henrygd/beszel/agent/utils"
	"github.com/henrygd/beszel/internal/entities/container"
)

const incusTimeoutMs = 2100

// incusManager collects Incus instance stats from the Incus API.
//
// Each poll makes two requests, both covering every project: the instance list
// (for the UUID, status and image) and /1.0/metrics (for CPU, memory and network).
type incusManager struct {
	client            *http.Client
	mu                sync.Mutex
	excludeContainers []string
	numCPU            int // host CPUs (set from systemDetails.Threads); CPU % is a share of the whole host, like Docker's

	// Previous counter samples per cache time, keyed by instance key (project/name).
	prevSamples map[uint16]map[string]incusSample
}

// incusSample holds the cumulative counters read for an instance at one point in time.
type incusSample struct {
	cpuSeconds float64
	sent, recv uint64
	readTime   time.Time
}

// incusResponse is the Incus REST API response envelope.
type incusResponse[T any] struct {
	StatusCode int `json:"status_code"`
	Metadata   T   `json:"metadata"`
}

// incusInstance represents an entry from GET /1.0/instances?recursion=1.
type incusInstance struct {
	Name    string            `json:"name"`
	Project string            `json:"project"`
	Status  string            `json:"status"`
	Type    string            `json:"type"`
	Config  map[string]string `json:"config"`
	// LastUsedAt is set when the instance starts; freezing, exec and
	// snapshots don't change it.
	LastUsedAt time.Time `json:"last_used_at"`
}

// incusInstanceMetrics holds the values read from /1.0/metrics for one instance.
type incusInstanceMetrics struct {
	cpuSeconds   float64
	memTotal     uint64
	memFree      uint64
	inactiveFile uint64
	cached       uint64 // page cache including shmem; only used for VMs
	shmem        uint64
	isVM         bool
	sent, recv   uint64
	bootTime     float64 // unix seconds when the instance started
}

// incusIdleCPUModes are CPU modes that aren't instance activity. Containers
// only report user and system; VMs report every mode through incus-agent.
var incusIdleCPUModes = map[string]bool{"idle": true, "iowait": true, "steal": true}

// incusKey identifies an instance across projects.
func incusKey(project, name string) string {
	return project + "/" + name
}

// displayName returns the instance name, prefixed with its project outside the default project.
func (inst *incusInstance) displayName() string {
	if inst.Project == "" || inst.Project == "default" {
		return inst.Name
	}
	return incusKey(inst.Project, inst.Name)
}

// containerId returns an ID that is unique across hosts, built from volatile.uuid.
// The "incus_" prefix tells the UI the row is an Incus instance.
func (inst *incusInstance) containerId() string {
	id := strings.ReplaceAll(inst.Config["volatile.uuid"], "-", "")
	if len(id) < 12 {
		// No UUID (shouldn't happen): fall back to a hash of project and name.
		h := fnv.New64a()
		h.Write([]byte(incusKey(inst.Project, inst.Name)))
		id = fmt.Sprintf("%016x", h.Sum64())
	}
	return "incus_" + id[:12]
}

func (im *incusManager) shouldExclude(inst *incusInstance) bool {
	for _, pattern := range im.excludeContainers {
		if match, _ := path.Match(pattern, inst.Name); match {
			return true
		}
		if match, _ := path.Match(pattern, inst.displayName()); match {
			return true
		}
	}
	return false
}

// getIncusStats returns stats for running and frozen Incus instances in all projects.
func (im *incusManager) getIncusStats(cacheTimeMs uint16) ([]*container.Stats, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	instances, err := im.getInstances()
	if err != nil {
		return nil, err
	}
	metrics, err := im.getMetrics()
	if err != nil {
		return nil, err
	}
	readTime := time.Now()

	prev := im.prevSamples[cacheTimeMs]
	if prev == nil {
		prev = make(map[string]incusSample)
		im.prevSamples[cacheTimeMs] = prev
	}
	seen := make(map[string]struct{}, len(instances))

	stats := make([]*container.Stats, 0, len(instances))
	for i := range instances {
		inst := &instances[i]
		// Metrics only cover running and frozen instances.
		if inst.Status != "Running" && inst.Status != "Frozen" {
			continue
		}
		if im.shouldExclude(inst) {
			slog.Debug("Excluding Incus instance", "name", inst.displayName())
			continue
		}
		key := incusKey(inst.Project, inst.Name)
		seen[key] = struct{}{}

		m := metrics[key]
		if m == nil {
			// Started between the two requests, or a VM without incus-agent.
			slog.Debug("No Incus metrics for instance", "name", inst.displayName())
			m = &incusInstanceMetrics{}
		}
		sample := incusSample{cpuSeconds: m.cpuSeconds, sent: m.sent, recv: m.recv, readTime: readTime}
		cpuPct, sentBps, recvBps := im.calculateRates(inst.displayName(), prev[key], sample)
		prev[key] = sample

		usedMem := m.usedMemory()
		if usedMem == 0 && m.memTotal > 0 {
			slog.Debug("Unexpected Incus memory values", "name", inst.displayName(),
				"total", m.memTotal, "free", m.memFree, "inactiveFile", m.inactiveFile, "cached", m.cached, "shmem", m.shmem)
		}

		stats = append(stats, &container.Stats{
			Name:      inst.displayName(),
			Id:        inst.containerId(),
			Image:     incusImageLabel(inst.Config),
			Type:      "incus",
			Status:    m.status(inst, readTime),
			Health:    container.DockerHealthNone,
			Cpu:       utils.TwoDecimals(cpuPct),
			Mem:       utils.BytesToMegabytes(float64(usedMem)),
			Bandwidth: [2]uint64{sentBps, recvBps},
			// TODO(0.19+): stop populating NetworkSent/NetworkRecv (deprecated in 0.18.3)
			NetworkSent: utils.BytesToMegabytes(float64(sentBps)),
			NetworkRecv: utils.BytesToMegabytes(float64(recvBps)),
		})
	}

	// Forget instances that are gone, stopped or excluded, in every cache time.
	for _, samples := range im.prevSamples {
		for key := range samples {
			if _, ok := seen[key]; !ok {
				delete(samples, key)
			}
		}
	}

	return stats, nil
}

// calculateRates returns CPU % (share of the host) and network bytes per second
// between two samples. The first sample, or a counter that went backwards, gives zero.
func (im *incusManager) calculateRates(name string, prev, cur incusSample) (cpuPct float64, sentBps, recvBps uint64) {
	if prev.readTime.IsZero() {
		return 0, 0, 0
	}
	elapsed := cur.readTime.Sub(prev.readTime)
	if elapsed <= 0 {
		return 0, 0, 0
	}

	if cur.cpuSeconds >= prev.cpuSeconds && im.numCPU > 0 {
		cpuPct = (cur.cpuSeconds - prev.cpuSeconds) / (elapsed.Seconds() * float64(im.numCPU)) * 100
		cpuPct = min(cpuPct, 100)
	}

	ms := uint64(elapsed.Milliseconds())
	if ms == 0 {
		return cpuPct, 0, 0
	}
	if cur.sent >= prev.sent {
		sentBps = (cur.sent - prev.sent) * 1000 / ms
		if sentBps > maxNetworkSpeedBps {
			slog.Warn("Bad network sent delta", "instance", name)
			sentBps = 0
		}
	}
	if cur.recv >= prev.recv {
		recvBps = (cur.recv - prev.recv) * 1000 / ms
		if recvBps > maxNetworkSpeedBps {
			slog.Warn("Bad network recv delta", "instance", name)
			recvBps = 0
		}
	}
	return cpuPct, sentBps, recvBps
}

// status returns the instance status in Docker's format ("Up 5 minutes",
// "Up 5 minutes (Paused)" when frozen), so the UI shows and sorts both engines
// the same way. The start time is incus_boot_time_seconds, or the instance's
// last_used_at on Incus 6.0, which doesn't report it. Without either it falls
// back to Incus's status.
func (m *incusInstanceMetrics) status(inst *incusInstance, now time.Time) string {
	started := inst.LastUsedAt
	if m != nil && m.bootTime > 0 {
		started = time.Unix(0, int64(m.bootTime*float64(time.Second)))
	}
	if started.Unix() <= 0 {
		return inst.Status
	}
	status := "Up " + humanDuration(now.Sub(started))
	if inst.Status == "Frozen" {
		status += " (Paused)"
	}
	return status
}

// humanDuration formats a duration like Docker's container status
// (HumanDuration in github.com/docker/go-units).
func humanDuration(d time.Duration) string {
	if seconds := int(d.Seconds()); seconds < 1 {
		return "Less than a second"
	} else if seconds == 1 {
		return "1 second"
	} else if seconds < 60 {
		return fmt.Sprintf("%d seconds", seconds)
	} else if minutes := int(d.Minutes()); minutes == 1 {
		return "About a minute"
	} else if minutes < 60 {
		return fmt.Sprintf("%d minutes", minutes)
	} else if hours := int(d.Hours() + 0.5); hours == 1 {
		return "About an hour"
	} else if hours < 48 {
		return fmt.Sprintf("%d hours", hours)
	} else if hours < 24*7*2 {
		return fmt.Sprintf("%d days", hours/24)
	} else if hours < 24*30*2 {
		return fmt.Sprintf("%d weeks", hours/24/7)
	} else if hours < 24*365*2 {
		return fmt.Sprintf("%d months", hours/24/30)
	}
	return fmt.Sprintf("%d years", int(d.Hours())/24/365)
}

// usedMemory returns memory in use without reclaimable page cache.
//
// For containers it matches Docker's figure (cgroup usage minus inactive_file);
// MemTotal - MemFree is the cgroup's memory.current. For VMs the values come
// from the guest's /proc/meminfo, where active page cache would also count, so
// all page cache except shmem (tmpfs, which is in use) is left out, close to
// the guest's own "used". Without incus-agent the values come from QEMU, with
// no cache figures, so a VM shows all memory it has touched on the host.
func (m incusInstanceMetrics) usedMemory() uint64 {
	if m.memTotal <= m.memFree {
		return 0
	}
	used := m.memTotal - m.memFree
	reclaimable := m.inactiveFile
	if m.isVM {
		reclaimable = 0
		if m.cached > m.shmem {
			reclaimable = m.cached - m.shmem
		}
	}
	if used <= reclaimable {
		return 0
	}
	used -= reclaimable
	if used > maxMemoryUsage {
		return 0
	}
	return used
}

// getInstances lists instances in all projects.
func (im *incusManager) getInstances() ([]incusInstance, error) {
	resp, err := im.client.Get("http://incus/1.0/instances?recursion=1&all-projects=true")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("incus instances: %s", resp.Status)
	}
	var listResp incusResponse[[]incusInstance]
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return nil, err
	}
	return listResp.Metadata, nil
}

// getMetrics reads /1.0/metrics, keyed by instance key (project/name).
func (im *incusManager) getMetrics() (map[string]*incusInstanceMetrics, error) {
	resp, err := im.client.Get("http://incus/1.0/metrics")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("incus metrics: %s", resp.Status)
	}
	return parseIncusMetrics(resp.Body)
}

// parseIncusMetrics parses the Prometheus text format returned by /1.0/metrics,
// keeping only the per-instance series beszel uses.
func parseIncusMetrics(r io.Reader) (map[string]*incusInstanceMetrics, error) {
	result := make(map[string]*incusInstanceMetrics)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, labels, value, ok := parsePrometheusLine(line)
		if !ok {
			continue
		}
		switch name {
		case "incus_boot_time_seconds", "incus_cpu_seconds_total", "incus_memory_MemTotal_bytes", "incus_memory_MemFree_bytes",
			"incus_memory_Inactive_file_bytes", "incus_memory_Cached_bytes", "incus_memory_Shmem_bytes",
			"incus_network_receive_bytes_total", "incus_network_transmit_bytes_total":
		default:
			continue
		}
		if labels["name"] == "" {
			continue
		}
		key := incusKey(labels["project"], labels["name"])
		m := result[key]
		if m == nil {
			m = &incusInstanceMetrics{}
			result[key] = m
		}
		if labels["type"] == "virtual-machine" {
			m.isVM = true
		}
		switch name {
		case "incus_boot_time_seconds":
			m.bootTime = value
		case "incus_cpu_seconds_total":
			if !incusIdleCPUModes[labels["mode"]] {
				m.cpuSeconds += value
			}
		case "incus_memory_MemTotal_bytes":
			m.memTotal = uint64(value)
		case "incus_memory_MemFree_bytes":
			m.memFree = uint64(value)
		case "incus_memory_Inactive_file_bytes":
			m.inactiveFile = uint64(value)
		case "incus_memory_Cached_bytes":
			m.cached = uint64(value)
		case "incus_memory_Shmem_bytes":
			m.shmem = uint64(value)
		case "incus_network_receive_bytes_total":
			if labels["device"] != "lo" {
				m.recv += uint64(value)
			}
		case "incus_network_transmit_bytes_total":
			if labels["device"] != "lo" {
				m.sent += uint64(value)
			}
		}
	}
	return result, scanner.Err()
}

// parsePrometheusLine parses one sample line: name{label="value",...} value [timestamp].
func parsePrometheusLine(line string) (name string, labels map[string]string, value float64, ok bool) {
	i := strings.IndexAny(line, "{ ")
	if i < 0 {
		return "", nil, 0, false
	}
	name, rest := line[:i], line[i:]

	if rest[0] == '{' {
		labels = make(map[string]string, 4)
		rest = rest[1:]
		for {
			rest = strings.TrimLeft(rest, ", ")
			if rest == "" {
				return "", nil, 0, false
			}
			if rest[0] == '}' {
				rest = rest[1:]
				break
			}
			eq := strings.Index(rest, `="`)
			if eq < 0 {
				return "", nil, 0, false
			}
			key := rest[:eq]
			rest = rest[eq+2:]
			var sb strings.Builder
			closed := false
			for i := 0; i < len(rest); i++ {
				c := rest[i]
				if c == '\\' && i+1 < len(rest) {
					i++
					switch rest[i] {
					case 'n':
						sb.WriteByte('\n')
					default:
						sb.WriteByte(rest[i])
					}
					continue
				}
				if c == '"' {
					rest = rest[i+1:]
					closed = true
					break
				}
				sb.WriteByte(c)
			}
			if !closed {
				return "", nil, 0, false
			}
			labels[key] = sb.String()
		}
	}

	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", nil, 0, false
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, false
	}
	return name, labels, value, true
}

// incusImageLabel builds a human-readable image string from instance config.
func incusImageLabel(config map[string]string) string {
	if desc := config["image.description"]; desc != "" {
		return desc
	}
	return strings.TrimSpace(config["image.os"] + " " + config["image.release"])
}

// getIncusSocketPath returns the first existing Incus Unix socket path.
func getIncusSocketPath() string {
	candidates := []string{
		"/var/lib/incus/unix.socket",
		"/run/incus/unix.socket",
	}
	for _, s := range candidates {
		if _, err := os.Stat(s); err == nil {
			return s
		}
	}
	return candidates[0]
}

// newIncusManager creates an incusManager connected to the Incus Unix socket.
// Returns nil if Incus is not available or explicitly disabled via INCUS_HOST="".
func newIncusManager() *incusManager {
	var socketPath string

	incusHost, exists := utils.GetEnv("INCUS_HOST")
	if exists {
		if incusHost == "" {
			return nil
		}
		parsedURL, err := url.Parse(incusHost)
		if err != nil || parsedURL.Scheme != "unix" {
			slog.Error("INCUS_HOST must be a unix:// URL", "value", incusHost)
			return nil
		}
		socketPath = parsedURL.Path
	} else {
		socketPath = getIncusSocketPath()
		if _, err := os.Stat(socketPath); err != nil {
			slog.Debug("Incus socket not found", "path", socketPath)
			return nil
		}
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}

	timeout := time.Millisecond * time.Duration(incusTimeoutMs)
	if t, set := utils.GetEnv("INCUS_TIMEOUT"); set {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
			slog.Info("INCUS_TIMEOUT", "timeout", timeout)
		} else {
			slog.Error("Invalid INCUS_TIMEOUT", "err", err)
			return nil
		}
	}

	slog.Info("Incus", "socket", socketPath)

	return &incusManager{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		excludeContainers: getExcludeContainers(),
		numCPU:            runtime.NumCPU(),
		prevSamples:       make(map[uint16]map[string]incusSample),
	}
}

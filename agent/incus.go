package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/henrygd/beszel/agent/deltatracker"
	"github.com/henrygd/beszel/agent/utils"
	"github.com/henrygd/beszel/internal/entities/container"
)

const incusTimeoutMs = 2100

// incusManager handles Incus instance stats collection via the Incus REST API.
type incusManager struct {
	client              *http.Client
	wg                  sync.WaitGroup
	sem                 chan struct{}
	containerStatsMutex sync.Mutex
	containerStatsMap   map[string]*container.Stats
	validNames          map[string]struct{}
	excludeContainers   []string

	// Reusable buffer and decoder (not thread-safe; protected by containerStatsMutex)
	buf     *bytes.Buffer
	decoder *json.Decoder

	// Per-cache-time CPU tracking (cumulative nanoseconds from Incus state API)
	lastCpuUsage    map[uint16]map[string]uint64
	lastCpuReadTime map[uint16]map[string]time.Time

	// Per-cache-time network delta trackers
	networkSentTrackers map[uint16]*deltatracker.DeltaTracker[string, uint64]
	networkRecvTrackers map[uint16]*deltatracker.DeltaTracker[string, uint64]
	lastNetworkReadTime map[uint16]map[string]time.Time
}

// incusResponse is the Incus REST API response envelope.
type incusResponse[T any] struct {
	StatusCode int `json:"status_code"`
	Metadata   T   `json:"metadata"`
}

// incusInstance represents an entry from GET /1.0/instances?recursion=1.
type incusInstance struct {
	Name   string            `json:"name"`
	Status string            `json:"status"`
	Type   string            `json:"type"`
	Config map[string]string `json:"config"`
}

// incusState is the response body from GET /1.0/instances/{name}/state.
type incusState struct {
	CPU struct {
		Usage uint64 `json:"usage"` // cumulative CPU nanoseconds
	} `json:"cpu"`
	Memory struct {
		Usage uint64 `json:"usage"` // bytes
	} `json:"memory"`
	Network map[string]incusNetworkState `json:"network"`
	Status  string                       `json:"status"`
}

type incusNetworkState struct {
	Counters struct {
		BytesReceived uint64 `json:"bytes_received"`
		BytesSent     uint64 `json:"bytes_sent"`
	} `json:"counters"`
	Type string `json:"type"` // "loopback", "broadcast", etc.
}

func (im *incusManager) shouldExclude(name string) bool {
	for _, pattern := range im.excludeContainers {
		if match, _ := path.Match(pattern, name); match {
			return true
		}
	}
	return false
}

// getIncusStats returns stats for all running Incus instances with cache-time-aware delta tracking.
func (im *incusManager) getIncusStats(cacheTimeMs uint16) ([]*container.Stats, error) {
	resp, err := im.client.Get("http://localhost/1.0/instances?recursion=1")
	if err != nil {
		return nil, err
	}

	im.containerStatsMutex.Lock()
	var listResp incusResponse[[]incusInstance]
	err = im.decode(resp, &listResp)
	im.containerStatsMutex.Unlock()
	if err != nil {
		return nil, err
	}

	instances := listResp.Metadata

	if im.validNames == nil {
		im.validNames = make(map[string]struct{}, len(instances))
	} else {
		clear(im.validNames)
	}

	for i := range instances {
		inst := instances[i]
		if inst.Status != "Running" {
			continue
		}
		if im.shouldExclude(inst.Name) {
			slog.Debug("Excluding Incus instance", "name", inst.Name)
			continue
		}
		im.validNames[inst.Name] = struct{}{}
		im.wg.Add(1)
		im.sem <- struct{}{}
		go func(inst incusInstance) {
			defer func() {
				<-im.sem
				im.wg.Done()
			}()
			if err := im.updateInstanceStats(inst, cacheTimeMs); err != nil {
				slog.Debug("Incus instance stats error", "name", inst.Name, "err", err)
			}
		}(inst)
	}

	im.wg.Wait()

	// Collect results and prune stale entries.
	stats := make([]*container.Stats, 0, len(im.validNames))
	for name, v := range im.containerStatsMap {
		if _, ok := im.validNames[name]; !ok {
			delete(im.containerStatsMap, name)
			im.deleteCpuTracking(name)
			im.deleteNetworkTracking(name)
		} else {
			stats = append(stats, v)
		}
	}

	im.cycleNetworkTrackers(cacheTimeMs)
	return stats, nil
}

// updateInstanceStats fetches and updates stats for a single Incus instance.
// The HTTP request runs outside the lock; decode and map updates run inside.
func (im *incusManager) updateInstanceStats(inst incusInstance, cacheTimeMs uint16) error {
	resp, err := im.client.Get(fmt.Sprintf("http://localhost/1.0/instances/%s/state", url.PathEscape(inst.Name)))
	if err != nil {
		return err
	}

	readTime := time.Now()

	im.containerStatsMutex.Lock()
	defer im.containerStatsMutex.Unlock()

	var stateResp incusResponse[incusState]
	if err := im.decode(resp, &stateResp); err != nil {
		return err
	}
	state := stateResp.Metadata

	if state.Status != "Running" {
		return nil
	}

	// CPU: Incus exposes cumulative nanoseconds; derive % from time-weighted delta.
	im.initCpuTracking(cacheTimeMs)
	prevCpuNs := im.lastCpuUsage[cacheTimeMs][inst.Name]
	prevReadTime := im.lastCpuReadTime[cacheTimeMs][inst.Name]
	currentCpuNs := state.CPU.Usage

	var cpuPct float64
	if prevCpuNs > 0 && !prevReadTime.IsZero() && currentCpuNs >= prevCpuNs {
		cpuDelta := currentCpuNs - prevCpuNs
		elapsedNs := uint64(readTime.Sub(prevReadTime).Nanoseconds())
		if elapsedNs > 0 {
			cpuPct = float64(cpuDelta) / float64(elapsedNs) * 100.0
			// Sanity cap: more than 10000% (100 CPUs fully used) indicates a bad delta.
			if cpuPct > 10000 {
				slog.Warn("Bad CPU delta", "instance", inst.Name)
				cpuPct = 0
			}
		}
	}
	im.lastCpuUsage[cacheTimeMs][inst.Name] = currentCpuNs
	im.lastCpuReadTime[cacheTimeMs][inst.Name] = readTime

	// Memory.
	usedMemory := state.Memory.Usage
	if usedMemory > maxMemoryUsage {
		return fmt.Errorf("bad memory stats for %s", inst.Name)
	}

	// Network: sum all non-loopback interfaces.
	var totalSent, totalRecv uint64
	for _, iface := range state.Network {
		if iface.Type == "loopback" {
			continue
		}
		totalSent += iface.Counters.BytesSent
		totalRecv += iface.Counters.BytesReceived
	}

	sentTracker := im.getNetworkTracker(cacheTimeMs, true)
	recvTracker := im.getNetworkTracker(cacheTimeMs, false)
	sentTracker.Set(inst.Name, totalSent)
	recvTracker.Set(inst.Name, totalRecv)

	var sentDelta, recvDelta uint64
	if prevNetTime, ok := im.lastNetworkReadTime[cacheTimeMs][inst.Name]; ok {
		ms := uint64(readTime.Sub(prevNetTime).Milliseconds())
		if ms > 0 {
			if raw := sentTracker.Delta(inst.Name); raw > 0 {
				sentDelta = raw * 1000 / ms
				if sentDelta > maxNetworkSpeedBps {
					slog.Warn("Bad network sent delta", "instance", inst.Name)
					sentDelta = 0
				}
			}
			if raw := recvTracker.Delta(inst.Name); raw > 0 {
				recvDelta = raw * 1000 / ms
				if recvDelta > maxNetworkSpeedBps {
					slog.Warn("Bad network recv delta", "instance", inst.Name)
					recvDelta = 0
				}
			}
		}
	}
	if im.lastNetworkReadTime[cacheTimeMs] == nil {
		im.lastNetworkReadTime[cacheTimeMs] = make(map[string]time.Time)
	}
	im.lastNetworkReadTime[cacheTimeMs][inst.Name] = readTime

	// Update or create container stats entry.
	stats, ok := im.containerStatsMap[inst.Name]
	if !ok {
		stats = &container.Stats{
			Name:  inst.Name,
			Id:    inst.Name,
			Image: incusImageLabel(inst.Config),
		}
		im.containerStatsMap[inst.Name] = stats
	}

	stats.Status = state.Status
	stats.Health = container.DockerHealthNone
	stats.Cpu = utils.TwoDecimals(cpuPct)
	stats.Mem = utils.BytesToMegabytes(float64(usedMemory))
	stats.Bandwidth = [2]uint64{sentDelta, recvDelta}
	// TODO(0.19+): stop populating NetworkSent/NetworkRecv (deprecated in 0.18.3)
	stats.NetworkSent = utils.BytesToMegabytes(float64(sentDelta))
	stats.NetworkRecv = utils.BytesToMegabytes(float64(recvDelta))

	return nil
}

// incusImageLabel builds a human-readable image string from instance config.
func incusImageLabel(config map[string]string) string {
	if desc := config["image.description"]; desc != "" {
		return desc
	}
	parts := strings.TrimSpace(config["image.os"] + " " + config["image.release"])
	return parts
}

// initCpuTracking ensures per-cache-time CPU tracking maps exist.
func (im *incusManager) initCpuTracking(cacheTimeMs uint16) {
	if im.lastCpuUsage[cacheTimeMs] == nil {
		im.lastCpuUsage[cacheTimeMs] = make(map[string]uint64)
	}
	if im.lastCpuReadTime[cacheTimeMs] == nil {
		im.lastCpuReadTime[cacheTimeMs] = make(map[string]time.Time)
	}
}

// deleteCpuTracking removes CPU tracking data for an instance across all cache times.
func (im *incusManager) deleteCpuTracking(name string) {
	for ct := range im.lastCpuUsage {
		delete(im.lastCpuUsage[ct], name)
	}
	for ct := range im.lastCpuReadTime {
		delete(im.lastCpuReadTime[ct], name)
	}
}

// deleteNetworkTracking removes network tracking data for an instance across all cache times.
func (im *incusManager) deleteNetworkTracking(name string) {
	for ct := range im.lastNetworkReadTime {
		delete(im.lastNetworkReadTime[ct], name)
	}
}

// getNetworkTracker returns the DeltaTracker for a cache time, creating it if needed.
func (im *incusManager) getNetworkTracker(cacheTimeMs uint16, isSent bool) *deltatracker.DeltaTracker[string, uint64] {
	trackers := im.networkRecvTrackers
	if isSent {
		trackers = im.networkSentTrackers
	}
	if trackers[cacheTimeMs] == nil {
		trackers[cacheTimeMs] = deltatracker.NewDeltaTracker[string, uint64]()
	}
	return trackers[cacheTimeMs]
}

// cycleNetworkTrackers advances network delta trackers for the given cache time.
func (im *incusManager) cycleNetworkTrackers(cacheTimeMs uint16) {
	if im.networkSentTrackers[cacheTimeMs] != nil {
		im.networkSentTrackers[cacheTimeMs].Cycle()
	}
	if im.networkRecvTrackers[cacheTimeMs] != nil {
		im.networkRecvTrackers[cacheTimeMs].Cycle()
	}
}

// decode reads and decodes an Incus API JSON response using a reusable buffer. Not thread-safe.
func (im *incusManager) decode(resp *http.Response, d any) error {
	if im.buf == nil {
		im.buf = bytes.NewBuffer(make([]byte, 0, 1024*64))
		im.decoder = json.NewDecoder(im.buf)
	}
	defer resp.Body.Close()
	defer im.buf.Reset()
	if _, err := im.buf.ReadFrom(resp.Body); err != nil {
		return err
	}
	return im.decoder.Decode(d)
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
		DisableCompression: true,
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

	var excludeContainers []string
	if excludeStr, set := utils.GetEnv("INCUS_EXCLUDE_CONTAINERS"); set && excludeStr != "" {
		for part := range strings.SplitSeq(excludeStr, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				excludeContainers = append(excludeContainers, trimmed)
			}
		}
		slog.Info("INCUS_EXCLUDE_CONTAINERS", "patterns", excludeContainers)
	}

	slog.Info("Incus", "socket", socketPath)

	return &incusManager{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		containerStatsMap:   make(map[string]*container.Stats),
		sem:                 make(chan struct{}, 5),
		excludeContainers:   excludeContainers,
		lastCpuUsage:        make(map[uint16]map[string]uint64),
		lastCpuReadTime:     make(map[uint16]map[string]time.Time),
		networkSentTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		networkRecvTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		lastNetworkReadTime: make(map[uint16]map[string]time.Time),
	}
}

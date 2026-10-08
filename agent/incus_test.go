//go:build testing

package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henrygd/beszel/internal/entities/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIncusManagerForTest creates an incusManager wired to a test HTTP server.
func newIncusManagerForTest(server *httptest.Server, numCPU int) *incusManager {
	return &incusManager{
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, network, _ string) (net.Conn, error) {
					return net.Dial(network, server.Listener.Addr().String())
				},
			},
		},
		numCPU:      numCPU,
		prevSamples: make(map[uint16]map[string]incusSample),
	}
}

// Incus API response fixtures used across multiple tests.
const (
	incusListFixture = `{"status_code":200,"metadata":[
		{"name":"web","project":"default","status":"Running","type":"container","config":{"image.description":"Alpine 3.22","volatile.uuid":"4a2b18b4-f856-4b47-92c9-f7aa7aaba1c7"}},
		{"name":"web","project":"shop","status":"Running","type":"container","config":{"image.os":"debian","image.release":"12","volatile.uuid":"9f0c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f"}},
		{"name":"paused","project":"default","status":"Frozen","type":"container","config":{"volatile.uuid":"e248b79d-eb96-4bf0-ad7e-02b1128d4ae5"}},
		{"name":"off","project":"default","status":"Stopped","type":"virtual-machine","config":{"volatile.uuid":"adb0dc8f-3c6e-4e77-ad61-b575d7d4ca19"}}
	]}`

	// Trimmed from a real Incus 7.4 /1.0/metrics response.
	incusMetricsFixture = `# HELP incus_cpu_seconds_total The total number of CPU time used in seconds.
# TYPE incus_cpu_seconds_total counter
incus_cpu_seconds_total{cpu="0",mode="system",name="web",project="default",type="container"} 2
incus_cpu_seconds_total{cpu="0",mode="user",name="web",project="default",type="container"} 8
incus_cpu_seconds_total{cpu="0",mode="user",name="web",project="shop",type="container"} 1
incus_cpu_seconds_total{cpu="0",mode="user",name="paused",project="default",type="container"} 0.5
# HELP incus_memory_Inactive_file_bytes The amount of inactive file-backed memory.
# TYPE incus_memory_Inactive_file_bytes gauge
incus_memory_Inactive_file_bytes{name="web",project="default",type="container"} 4.19459072e+08
incus_memory_Inactive_file_bytes{name="web",project="shop",type="container"} 0
incus_memory_MemFree_bytes{name="web",project="default",type="container"} 3.571157408e+09
incus_memory_MemFree_bytes{name="web",project="shop",type="container"} 3.9e+09
incus_memory_MemTotal_bytes{name="web",project="default",type="container"} 4.004932e+09
incus_memory_MemTotal_bytes{name="web",project="shop",type="container"} 4.004932e+09
incus_network_receive_bytes_total{device="lo",name="web",project="default",type="container"} 999
incus_network_receive_bytes_total{device="eth0",name="web",project="default",type="container"} 7229
incus_network_transmit_bytes_total{device="lo",name="web",project="default",type="container"} 999
incus_network_transmit_bytes_total{device="eth0",name="web",project="default",type="container"} 4380
incus_storage_pool_size_bytes{name="default",type="dir"} 6.2e+10
incus_boot_time_seconds{name="web",project="default",type="container"} 1.7e+09
incus_boot_time_seconds{name="paused",project="default",type="container"} 1.7e+09
incus_uptime_seconds 2448.880084765
`
)

// makeIncusServer serves the instance list and a metrics body that can be swapped between polls.
func makeIncusServer(t *testing.T, listJSON string, metrics *atomic.Value) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1.0/instances":
			assert.Equal(t, "true", r.URL.Query().Get("all-projects"))
			fmt.Fprint(w, listJSON)
		case "/1.0/metrics":
			fmt.Fprint(w, metrics.Load().(string))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func statsByName(stats []*container.Stats) map[string]*container.Stats {
	m := make(map[string]*container.Stats, len(stats))
	for _, s := range stats {
		m[s.Name] = s
	}
	return m
}

func TestIncusImageLabel(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]string
		expected string
	}{
		{"description takes priority over os/release", map[string]string{"image.description": "Ubuntu 22.04 LTS", "image.os": "ubuntu", "image.release": "22.04"}, "Ubuntu 22.04 LTS"},
		{"falls back to os + release when no description", map[string]string{"image.os": "debian", "image.release": "12"}, "debian 12"},
		{"os only", map[string]string{"image.os": "alpine"}, "alpine"},
		{"empty config returns empty string", map[string]string{}, ""},
		{"nil config returns empty string", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, incusImageLabel(tt.config))
		})
	}
}

func TestIncusShouldExclude(t *testing.T) {
	tests := []struct {
		name     string
		inst     incusInstance
		patterns []string
		expected bool
	}{
		{"no patterns excludes nothing", incusInstance{Name: "any", Project: "default"}, nil, false},
		{"exact match", incusInstance{Name: "test-web", Project: "default"}, []string{"test-web"}, true},
		{"exact match not hit", incusInstance{Name: "prod-web", Project: "default"}, []string{"test-web"}, false},
		{"wildcard prefix match", incusInstance{Name: "test-web", Project: "default"}, []string{"test-*"}, true},
		{"wildcard suffix match", incusInstance{Name: "web-staging", Project: "default"}, []string{"*-staging"}, true},
		{"multi-pattern no match", incusInstance{Name: "prod-web", Project: "default"}, []string{"test-*", "*-staging"}, false},
		{"name matches in another project", incusInstance{Name: "web", Project: "shop"}, []string{"web"}, true},
		{"project/name match", incusInstance{Name: "web", Project: "shop"}, []string{"shop/*"}, true},
		{"project pattern doesn't match other project", incusInstance{Name: "web", Project: "blog"}, []string{"shop/*"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &incusManager{excludeContainers: tt.patterns}
			assert.Equal(t, tt.expected, im.shouldExclude(&tt.inst))
		})
	}
}

func TestIncusDisplayName(t *testing.T) {
	assert.Equal(t, "web", (&incusInstance{Name: "web", Project: "default"}).displayName())
	assert.Equal(t, "web", (&incusInstance{Name: "web"}).displayName())
	assert.Equal(t, "shop/web", (&incusInstance{Name: "web", Project: "shop"}).displayName())
}

func TestIncusContainerId(t *testing.T) {
	inst := incusInstance{Name: "web", Project: "default", Config: map[string]string{"volatile.uuid": "4a2b18b4-f856-4b47-92c9-f7aa7aaba1c7"}}
	assert.Equal(t, "incus_4a2b18b4f856", inst.containerId())

	// The same name on another host has a different UUID, so a different ID.
	other := incusInstance{Name: "web", Project: "default", Config: map[string]string{"volatile.uuid": "9f0c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f"}}
	assert.NotEqual(t, inst.containerId(), other.containerId())

	// Without a UUID the ID is a stable hash of project and name.
	noUUID := incusInstance{Name: "web", Project: "default"}
	id := noUUID.containerId()
	assert.Regexp(t, `^incus_[a-f0-9]{12}$`, id)
	assert.Equal(t, id, noUUID.containerId())
	assert.NotEqual(t, id, (&incusInstance{Name: "web", Project: "shop"}).containerId())
}

func TestParsePrometheusLine(t *testing.T) {
	name, labels, value, ok := parsePrometheusLine(`incus_cpu_seconds_total{cpu="0",mode="user",name="web",project="default"} 83.984759`)
	require.True(t, ok)
	assert.Equal(t, "incus_cpu_seconds_total", name)
	assert.Equal(t, map[string]string{"cpu": "0", "mode": "user", "name": "web", "project": "default"}, labels)
	assert.Equal(t, 83.984759, value)

	name, labels, value, ok = parsePrometheusLine(`incus_uptime_seconds 2448.88`)
	require.True(t, ok)
	assert.Equal(t, "incus_uptime_seconds", name)
	assert.Nil(t, labels)
	assert.Equal(t, 2448.88, value)

	_, labels, value, ok = parsePrometheusLine(`m{name="a \"b\" \\c",x=""} 4.19459072e+08 1700000000000`)
	require.True(t, ok)
	assert.Equal(t, `a "b" \c`, labels["name"])
	assert.Equal(t, "", labels["x"])
	assert.Equal(t, 4.19459072e+08, value)

	for _, bad := range []string{`m`, `m{name="a"`, `m{name="a} 1`, `m{name} 1`, `m{name="a"}`, `m{name="a"} x`} {
		_, _, _, ok := parsePrometheusLine(bad)
		assert.False(t, ok, bad)
	}
}

func TestParseIncusMetrics(t *testing.T) {
	metrics, err := parseIncusMetrics(strings.NewReader(incusMetricsFixture))
	require.NoError(t, err)
	require.Len(t, metrics, 3)

	web := metrics["default/web"]
	require.NotNil(t, web)
	assert.Equal(t, 10.0, web.cpuSeconds, "user + system")
	assert.Equal(t, uint64(4004932000), web.memTotal)
	assert.Equal(t, uint64(3571157408), web.memFree)
	assert.Equal(t, uint64(419459072), web.inactiveFile)
	assert.Equal(t, uint64(7229), web.recv, "loopback excluded")
	assert.Equal(t, uint64(4380), web.sent, "loopback excluded")

	shopWeb := metrics["shop/web"]
	require.NotNil(t, shopWeb, "same name in another project is kept apart")
	assert.Equal(t, 1.0, shopWeb.cpuSeconds)
}

func TestParseIncusMetricsVMCPUModes(t *testing.T) {
	// VMs report every CPU mode through incus-agent; idle time isn't usage.
	body := `incus_cpu_seconds_total{cpu="0",mode="idle",name="vm",project="default",type="virtual-machine"} 1000
incus_cpu_seconds_total{cpu="0",mode="iowait",name="vm",project="default",type="virtual-machine"} 50
incus_cpu_seconds_total{cpu="0",mode="steal",name="vm",project="default",type="virtual-machine"} 5
incus_cpu_seconds_total{cpu="0",mode="user",name="vm",project="default",type="virtual-machine"} 3
incus_cpu_seconds_total{cpu="1",mode="system",name="vm",project="default",type="virtual-machine"} 2
incus_cpu_seconds_total{cpu="1",mode="softirq",name="vm",project="default",type="virtual-machine"} 1
`
	metrics, err := parseIncusMetrics(strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, 6.0, metrics["default/vm"].cpuSeconds)
}

func TestIncusUsedMemory(t *testing.T) {
	// Real numbers from a container that wrote a 400 MB file: memory.current is
	// 433 MB, almost all of it inactive page cache.
	m := incusInstanceMetrics{memTotal: 4004932000, memFree: 3571157408, inactiveFile: 419459072}
	assert.Equal(t, uint64(14315520), m.usedMemory())

	assert.Equal(t, uint64(0), incusInstanceMetrics{}.usedMemory(), "no metrics")
	assert.Equal(t, uint64(0), incusInstanceMetrics{memTotal: 100, memFree: 200}.usedMemory(), "free above total")
	assert.Equal(t, uint64(0), incusInstanceMetrics{memTotal: 100, memFree: 50, inactiveFile: 80}.usedMemory(), "cache above usage")
}

func TestIncusUsedMemoryVM(t *testing.T) {
	// Real numbers from a 2 GiB VM after an 800 MiB file was read three times and
	// 200 MiB written to tmpfs. The guest's own "used" plus shmem was 315 MiB; most
	// of the cache is active, so the containers' formula would give 1186 MiB.
	m := incusInstanceMetrics{
		isVM:         true,
		memTotal:     1921777664,
		memFree:      659173376,
		inactiveFile: 19349504,
		cached:       1123069952,
		shmem:        234795008,
	}
	assert.Equal(t, uint64(374329344), m.usedMemory()) // 357 MiB

	// Without incus-agent there are no cache figures: everything the VM touched counts.
	noAgent := incusInstanceMetrics{isVM: true, memTotal: 2147483648, memFree: 57143648}
	assert.Equal(t, uint64(2090340000), noAgent.usedMemory())

	assert.Equal(t, uint64(0), incusInstanceMetrics{isVM: true, memTotal: 100, memFree: 50, cached: 80}.usedMemory(), "cache above usage")
	assert.Equal(t, uint64(50), incusInstanceMetrics{isVM: true, memTotal: 100, memFree: 50, cached: 10, shmem: 20}.usedMemory(), "shmem above cache")
}

func TestParseIncusMetricsVMMemory(t *testing.T) {
	body := `incus_memory_MemTotal_bytes{name="vm",project="default",type="virtual-machine"} 1000
incus_memory_Cached_bytes{name="vm",project="default",type="virtual-machine"} 300
incus_memory_Shmem_bytes{name="vm",project="default",type="virtual-machine"} 40
incus_memory_MemTotal_bytes{name="ct",project="default",type="container"} 1000
`
	metrics, err := parseIncusMetrics(strings.NewReader(body))
	require.NoError(t, err)
	vm := metrics["default/vm"]
	assert.True(t, vm.isVM)
	assert.Equal(t, uint64(300), vm.cached)
	assert.Equal(t, uint64(40), vm.shmem)
	assert.False(t, metrics["default/ct"].isVM)
}

func TestIncusCalculateRates(t *testing.T) {
	im := &incusManager{numCPU: 4}
	t0 := time.Unix(1000, 0)
	prev := incusSample{cpuSeconds: 10, sent: 1000, recv: 2000, readTime: t0}

	// One core busy for 10s on a 4-CPU host is 25% of the host.
	cpu, sent, recv := im.calculateRates("web", prev, incusSample{cpuSeconds: 20, sent: 11000, recv: 7000, readTime: t0.Add(10 * time.Second)})
	assert.InDelta(t, 25.0, cpu, 0.001)
	assert.Equal(t, uint64(1000), sent)
	assert.Equal(t, uint64(500), recv)

	// Capped at 100.
	cpu, _, _ = im.calculateRates("web", prev, incusSample{cpuSeconds: 100, readTime: t0.Add(10 * time.Second)})
	assert.Equal(t, 100.0, cpu)

	// First sample.
	cpu, sent, recv = im.calculateRates("web", incusSample{}, incusSample{cpuSeconds: 20, sent: 5, recv: 5, readTime: t0})
	assert.Zero(t, cpu)
	assert.Zero(t, sent)
	assert.Zero(t, recv)

	// Counters reset (instance restarted).
	cpu, sent, recv = im.calculateRates("web", prev, incusSample{cpuSeconds: 1, sent: 1, recv: 1, readTime: t0.Add(10 * time.Second)})
	assert.Zero(t, cpu)
	assert.Zero(t, sent)
	assert.Zero(t, recv)

	// Impossible network rate.
	_, sent, _ = im.calculateRates("web", prev, incusSample{sent: 1000 + maxNetworkSpeedBps*100, readTime: t0.Add(10 * time.Second)})
	assert.Zero(t, sent)
}

func TestGetIncusStats(t *testing.T) {
	var metrics atomic.Value
	metrics.Store(incusMetricsFixture)
	server := makeIncusServer(t, incusListFixture, &metrics)
	im := newIncusManagerForTest(server, 2)

	stats, err := im.getIncusStats(60000)
	require.NoError(t, err)
	byName := statsByName(stats)
	require.Len(t, byName, 3, "running and frozen in all projects; stopped skipped")

	web := byName["web"]
	require.NotNil(t, web)
	assert.Equal(t, "incus_4a2b18b4f856", web.Id)
	assert.Equal(t, "incus", web.Type)
	assert.Regexp(t, `^Up \d+ (years|months)$`, web.Status, "Docker-style uptime")
	assert.Equal(t, "Alpine 3.22", web.Image)
	assert.Equal(t, container.DockerHealthNone, web.Health)
	assert.InDelta(t, 13.65, web.Mem, 0.01, "page cache excluded")
	assert.Zero(t, web.Cpu, "first call has no CPU delta")

	shopWeb := byName["shop/web"]
	require.NotNil(t, shopWeb)
	assert.Equal(t, "incus_9f0c1d2e3b4a", shopWeb.Id)
	assert.Equal(t, "debian 12", shopWeb.Image)

	assert.Regexp(t, `^Up .+ \(Paused\)$`, byName["paused"].Status)
	assert.Equal(t, "Running", shopWeb.Status, "no boot time falls back to the Incus status")
	assert.Nil(t, byName["off"])
}

func TestGetIncusStatsSecondCall(t *testing.T) {
	var metrics atomic.Value
	metrics.Store(incusMetricsFixture)
	server := makeIncusServer(t, incusListFixture, &metrics)
	im := newIncusManagerForTest(server, 2)

	_, err := im.getIncusStats(60000)
	require.NoError(t, err)

	// Move the stored sample back 10s, then add 5 CPU seconds and 10 KB sent.
	prev := im.prevSamples[60000]["default/web"]
	prev.readTime = prev.readTime.Add(-10 * time.Second)
	im.prevSamples[60000]["default/web"] = prev
	metrics.Store(strings.NewReplacer(
		`mode="user",name="web",project="default",type="container"} 8`, `mode="user",name="web",project="default",type="container"} 13`,
		`device="eth0",name="web",project="default",type="container"} 4380`, `device="eth0",name="web",project="default",type="container"} 14380`,
	).Replace(incusMetricsFixture))

	stats, err := im.getIncusStats(60000)
	require.NoError(t, err)
	web := statsByName(stats)["web"]
	require.NotNil(t, web)
	// 5 CPU seconds over ~10s on 2 CPUs is ~25% of the host.
	assert.InDelta(t, 25.0, web.Cpu, 1.0)
	assert.InDelta(t, 1000, web.Bandwidth[0], 50)
	assert.Zero(t, web.Bandwidth[1])
}

func TestGetIncusStatsCacheTimeIsolation(t *testing.T) {
	var metrics atomic.Value
	metrics.Store(incusMetricsFixture)
	server := makeIncusServer(t, incusListFixture, &metrics)
	im := newIncusManagerForTest(server, 2)

	_, err := im.getIncusStats(60000)
	require.NoError(t, err)
	_, err = im.getIncusStats(1000)
	require.NoError(t, err)
	assert.Contains(t, im.prevSamples[60000], "default/web")
	assert.Contains(t, im.prevSamples[1000], "default/web")
}

func TestGetIncusStatsPrunesGoneInstances(t *testing.T) {
	var metrics atomic.Value
	metrics.Store(incusMetricsFixture)
	var list atomic.Value
	list.Store(incusListFixture)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/1.0/instances" {
			fmt.Fprint(w, list.Load().(string))
			return
		}
		fmt.Fprint(w, metrics.Load().(string))
	}))
	defer server.Close()
	im := newIncusManagerForTest(server, 2)

	_, err := im.getIncusStats(60000)
	require.NoError(t, err)
	_, err = im.getIncusStats(1000)
	require.NoError(t, err)
	require.Contains(t, im.prevSamples[1000], "shop/web")

	list.Store(`{"status_code":200,"metadata":[
		{"name":"web","project":"default","status":"Running","config":{"volatile.uuid":"4a2b18b4-f856-4b47-92c9-f7aa7aaba1c7"}}
	]}`)
	stats, err := im.getIncusStats(60000)
	require.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.NotContains(t, im.prevSamples[60000], "shop/web")
	assert.NotContains(t, im.prevSamples[1000], "shop/web", "pruned in every cache time")
}

func TestGetIncusStatsExcludesInstances(t *testing.T) {
	var metrics atomic.Value
	metrics.Store(incusMetricsFixture)
	server := makeIncusServer(t, incusListFixture, &metrics)
	im := newIncusManagerForTest(server, 2)
	im.excludeContainers = []string{"shop/*", "paused"}

	stats, err := im.getIncusStats(60000)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, "web", stats[0].Name)
}

func TestGetIncusStatsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/1.0/metrics" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		fmt.Fprint(w, incusListFixture)
	}))
	defer server.Close()
	im := newIncusManagerForTest(server, 2)

	_, err := im.getIncusStats(60000)
	assert.ErrorContains(t, err, "403")
}

// ——— newIncusManager construction tests ———

func TestNewIncusManagerDisabledByEnvVar(t *testing.T) {
	t.Setenv("INCUS_HOST", "")
	im := newIncusManager()
	assert.Nil(t, im, "empty INCUS_HOST must disable Incus")
}

func TestNewIncusManagerInvalidScheme(t *testing.T) {
	// Only unix:// is supported; an http:// host must return nil.
	t.Setenv("INCUS_HOST", "http://localhost:8443")
	im := newIncusManager()
	assert.Nil(t, im, "non-unix INCUS_HOST must return nil")
}

func TestNewIncusManagerUnixSocket(t *testing.T) {
	t.Setenv("INCUS_HOST", "unix:///tmp/does-not-matter.socket")
	im := newIncusManager()
	require.NotNil(t, im)
	assert.Positive(t, im.numCPU)
}

func TestNewIncusManagerUsesExcludeContainers(t *testing.T) {
	t.Setenv("INCUS_HOST", "unix:///tmp/does-not-matter.socket")
	t.Setenv("EXCLUDE_CONTAINERS", "test-*, shop/* ,")
	im := newIncusManager()
	require.NotNil(t, im)
	assert.Equal(t, []string{"test-*", "shop/*"}, im.excludeContainers)
}

func TestIncusHumanDuration(t *testing.T) {
	// Matches HumanDuration in github.com/docker/go-units, used for Docker's "Up ..." status.
	tests := []struct {
		d        time.Duration
		expected string
	}{
		{500 * time.Millisecond, "Less than a second"},
		{time.Second, "1 second"},
		{45 * time.Second, "45 seconds"},
		{90 * time.Second, "About a minute"},
		{5 * time.Minute, "5 minutes"},
		{50 * time.Minute, "50 minutes"},
		{70 * time.Minute, "About an hour"},
		{3 * time.Hour, "3 hours"},
		{47 * time.Hour, "47 hours"},
		{3 * 24 * time.Hour, "3 days"},
		{20 * 24 * time.Hour, "2 weeks"},
		{90 * 24 * time.Hour, "3 months"},
		{800 * 24 * time.Hour, "2 years"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, humanDuration(tt.d), tt.d.String())
	}
}

func TestIncusStatus(t *testing.T) {
	now := time.Unix(1700000300, 0)
	m := &incusInstanceMetrics{bootTime: 1700000000}
	assert.Equal(t, "Up 5 minutes", m.status("Running", now))
	assert.Equal(t, "Up 5 minutes (Paused)", m.status("Frozen", now))
	assert.Equal(t, "Running", (&incusInstanceMetrics{}).status("Running", now))
	assert.Equal(t, "Frozen", (*incusInstanceMetrics)(nil).status("Frozen", now))
}

func TestGetIncusStatsInstanceMissingFromMetrics(t *testing.T) {
	// An instance that started between the two requests has no metrics yet.
	var metrics atomic.Value
	metrics.Store("")
	server := makeIncusServer(t, incusListFixture, &metrics)
	im := newIncusManagerForTest(server, 2)

	stats, err := im.getIncusStats(60000)
	require.NoError(t, err)
	require.Len(t, stats, 3)
	web := statsByName(stats)["web"]
	assert.Zero(t, web.Mem)
	assert.Equal(t, "Running", web.Status)
}

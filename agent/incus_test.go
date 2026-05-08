//go:build testing

package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/henrygd/beszel/agent/deltatracker"
	"github.com/henrygd/beszel/agent/utils"
	"github.com/henrygd/beszel/internal/entities/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newIncusManagerForTest creates an incusManager wired to a test HTTP server.
func newIncusManagerForTest(server *httptest.Server) *incusManager {
	return &incusManager{
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, network, _ string) (net.Conn, error) {
					return net.Dial(network, server.Listener.Addr().String())
				},
			},
		},
		containerStatsMap:   make(map[string]*container.Stats),
		sem:                 make(chan struct{}, 5),
		lastCpuUsage:        make(map[uint16]map[string]uint64),
		lastCpuReadTime:     make(map[uint16]map[string]time.Time),
		networkSentTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		networkRecvTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		lastNetworkReadTime: make(map[uint16]map[string]time.Time),
	}
}

// Incus API response fixtures used across multiple tests.
const (
	incusListTwoRunning = `{"status_code":200,"metadata":[
		{"name":"web","status":"Running","type":"container","config":{"image.description":"Ubuntu 22.04 LTS"}},
		{"name":"db","status":"Running","type":"container","config":{"image.os":"debian","image.release":"12"}},
		{"name":"stopped-vm","status":"Stopped","type":"virtual-machine","config":{}}
	]}`

	incusWebState = `{"status_code":200,"metadata":{
		"cpu":{"usage":1000000000},
		"memory":{"usage":268435456},
		"network":{
			"eth0":{"counters":{"bytes_sent":1000000,"bytes_received":500000},"type":"broadcast"},
			"lo":{"counters":{"bytes_sent":100,"bytes_received":100},"type":"loopback"}
		},
		"status":"Running"
	}}`

	incusDbState = `{"status_code":200,"metadata":{
		"cpu":{"usage":500000000},
		"memory":{"usage":134217728},
		"network":{
			"eth0":{"counters":{"bytes_sent":200000,"bytes_received":100000},"type":"broadcast"}
		},
		"status":"Running"
	}}`
)

// makeIncusServer builds a test HTTP server that serves fixed Incus API responses.
// The stateMap maps instance names to their state JSON.
func makeIncusServer(t *testing.T, listJSON string, stateMap map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/1.0/instances":
			fmt.Fprint(w, listJSON)
		default:
			// /1.0/instances/{name}/state
			trimmed := strings.TrimPrefix(r.URL.Path, "/1.0/instances/")
			name := strings.TrimSuffix(trimmed, "/state")
			if body, ok := stateMap[name]; ok {
				fmt.Fprint(w, body)
			} else {
				http.NotFound(w, r)
			}
		}
	}))
}

// ——— Pure-function tests ———

func TestIncusImageLabel(t *testing.T) {
	tests := []struct {
		name     string
		config   map[string]string
		expected string
	}{
		{
			name:     "description takes priority over os/release",
			config:   map[string]string{"image.description": "Ubuntu 22.04 LTS", "image.os": "ubuntu", "image.release": "22.04"},
			expected: "Ubuntu 22.04 LTS",
		},
		{
			name:     "falls back to os + release when no description",
			config:   map[string]string{"image.os": "debian", "image.release": "12"},
			expected: "debian 12",
		},
		{
			name:     "os only",
			config:   map[string]string{"image.os": "alpine"},
			expected: "alpine",
		},
		{
			name:     "empty config returns empty string",
			config:   map[string]string{},
			expected: "",
		},
		{
			name:     "nil config returns empty string",
			config:   nil,
			expected: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, incusImageLabel(tt.config))
		})
	}
}

func TestIncusShouldExclude(t *testing.T) {
	tests := []struct {
		name         string
		instanceName string
		patterns     []string
		expected     bool
	}{
		{"no patterns excludes nothing", "any-container", nil, false},
		{"empty patterns excludes nothing", "any-container", []string{}, false},
		{"exact match", "test-web", []string{"test-web"}, true},
		{"exact match not hit", "prod-web", []string{"test-web"}, false},
		{"wildcard prefix match", "test-web", []string{"test-*"}, true},
		{"wildcard prefix no match", "prod-web", []string{"test-*"}, false},
		{"wildcard suffix match", "web-staging", []string{"*-staging"}, true},
		{"multi-pattern first match", "test-web", []string{"test-*", "*-staging"}, true},
		{"multi-pattern second match", "web-staging", []string{"test-*", "*-staging"}, true},
		{"multi-pattern no match", "prod-web", []string{"test-*", "*-staging"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			im := &incusManager{excludeContainers: tt.patterns}
			assert.Equal(t, tt.expected, im.shouldExclude(tt.instanceName))
		})
	}
}

// ——— CPU tracking tests ———

func TestIncusInitCpuTracking(t *testing.T) {
	im := &incusManager{
		lastCpuUsage:    make(map[uint16]map[string]uint64),
		lastCpuReadTime: make(map[uint16]map[string]time.Time),
	}

	ct := uint16(30000)
	im.initCpuTracking(ct)

	assert.NotNil(t, im.lastCpuUsage[ct])
	assert.NotNil(t, im.lastCpuReadTime[ct])
	assert.Empty(t, im.lastCpuUsage[ct])

	// Second call must not overwrite existing entries.
	im.lastCpuUsage[ct]["instance-a"] = 99
	im.initCpuTracking(ct)
	assert.Equal(t, uint64(99), im.lastCpuUsage[ct]["instance-a"])
}

func TestIncusDeleteCpuTracking(t *testing.T) {
	now := time.Now()
	im := &incusManager{
		lastCpuUsage: map[uint16]map[string]uint64{
			1000:  {"a": 10, "b": 20},
			60000: {"a": 30},
		},
		lastCpuReadTime: map[uint16]map[string]time.Time{
			1000:  {"a": now, "b": now},
			60000: {"a": now},
		},
	}

	im.deleteCpuTracking("a")

	assert.NotContains(t, im.lastCpuUsage[1000], "a")
	assert.NotContains(t, im.lastCpuUsage[60000], "a")
	assert.Contains(t, im.lastCpuUsage[1000], "b", "unrelated entry must survive")
	assert.NotContains(t, im.lastCpuReadTime[1000], "a")
	assert.NotContains(t, im.lastCpuReadTime[60000], "a")
	assert.Contains(t, im.lastCpuReadTime[1000], "b")
}

// ——— Network tracking tests ———

func TestIncusDeleteNetworkTracking(t *testing.T) {
	now := time.Now()
	im := &incusManager{
		lastNetworkReadTime: map[uint16]map[string]time.Time{
			1000:  {"a": now, "b": now},
			60000: {"a": now},
		},
	}

	im.deleteNetworkTracking("a")

	assert.NotContains(t, im.lastNetworkReadTime[1000], "a")
	assert.NotContains(t, im.lastNetworkReadTime[60000], "a")
	assert.Contains(t, im.lastNetworkReadTime[1000], "b", "unrelated entry must survive")
}

func TestIncusGetNetworkTracker(t *testing.T) {
	im := &incusManager{
		networkSentTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		networkRecvTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
	}

	ct := uint16(30000)
	sent := im.getNetworkTracker(ct, true)
	recv := im.getNetworkTracker(ct, false)

	assert.NotNil(t, sent)
	assert.NotNil(t, recv)
	assert.NotSame(t, sent, recv)

	// Repeated calls return the same tracker instance.
	assert.Same(t, sent, im.getNetworkTracker(ct, true))
	assert.Same(t, recv, im.getNetworkTracker(ct, false))

	// Different cache times produce separate tracker instances.
	sent2 := im.getNetworkTracker(uint16(60000), true)
	assert.NotSame(t, sent, sent2)
}

func TestIncusCycleNetworkTrackers(t *testing.T) {
	im := &incusManager{
		networkSentTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
		networkRecvTrackers: make(map[uint16]*deltatracker.DeltaTracker[string, uint64]),
	}
	ct := uint16(60000)
	sent := im.getNetworkTracker(ct, true)
	recv := im.getNetworkTracker(ct, false)

	sent.Set("inst", 100)
	recv.Set("inst", 200)
	// No previous → delta is 0.
	assert.Equal(t, uint64(0), sent.Delta("inst"))

	im.cycleNetworkTrackers(ct)
	sent.Set("inst", 300)
	recv.Set("inst", 500)

	assert.Equal(t, uint64(200), sent.Delta("inst"))
	assert.Equal(t, uint64(300), recv.Delta("inst"))

	// Cycling a cache time with no trackers must not panic.
	assert.NotPanics(t, func() { im.cycleNetworkTrackers(uint16(1)) })
}

// ——— End-to-end stats collection tests ———

func TestGetIncusStatsFirstCall(t *testing.T) {
	server := makeIncusServer(t, incusListTwoRunning, map[string]string{
		"web": incusWebState,
		"db":  incusDbState,
	})
	defer server.Close()

	im := newIncusManagerForTest(server)
	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)

	assert.Len(t, stats, 2, "stopped-vm must not appear in results")

	byName := make(map[string]*container.Stats, len(stats))
	for _, s := range stats {
		byName[s.Name] = s
	}

	web, ok := byName["web"]
	require.True(t, ok, "web must be present")
	assert.Equal(t, "incus_web", web.Id)
	assert.Equal(t, "Ubuntu 22.04 LTS", web.Image)
	assert.Equal(t, "Running", web.Status)
	assert.Equal(t, container.DockerHealthNone, web.Health)
	// Memory: 268435456 bytes → MB
	assert.Equal(t, utils.BytesToMegabytes(268435456), web.Mem)
	// First call — no previous data — CPU and bandwidth must be zero.
	assert.Equal(t, 0.0, web.Cpu)
	assert.Equal(t, [2]uint64{0, 0}, web.Bandwidth)

	db, ok := byName["db"]
	require.True(t, ok, "db must be present")
	assert.Equal(t, "debian 12", db.Image)
	assert.Equal(t, utils.BytesToMegabytes(134217728), db.Mem)
	assert.Equal(t, 0.0, db.Cpu)

	assert.NotContains(t, byName, "stopped-vm")
}

func TestGetIncusStatsExcludesInstances(t *testing.T) {
	server := makeIncusServer(t, incusListTwoRunning, map[string]string{
		"web": incusWebState,
		"db":  incusDbState,
	})
	defer server.Close()

	im := newIncusManagerForTest(server)
	im.excludeContainers = []string{"web", "stopped-*"}

	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)

	assert.Len(t, stats, 1)
	assert.Equal(t, "db", stats[0].Name)
}

func TestGetIncusStatsNetworkRate(t *testing.T) {
	// Use a server that returns higher byte counts on the second call.
	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1.0/instances":
			fmt.Fprint(w, `{"status_code":200,"metadata":[
				{"name":"web","status":"Running","type":"container","config":{}}
			]}`)
		case "/1.0/instances/web/state":
			call++
			if call == 1 {
				fmt.Fprint(w, `{"status_code":200,"metadata":{
					"cpu":{"usage":1000000000},
					"memory":{"usage":104857600},
					"network":{"eth0":{"counters":{"bytes_sent":1000000,"bytes_received":500000},"type":"broadcast"}},
					"status":"Running"
				}}`)
			} else {
				// 2 MB more sent, 1 MB more received.
				fmt.Fprint(w, `{"status_code":200,"metadata":{
					"cpu":{"usage":2000000000},
					"memory":{"usage":104857600},
					"network":{"eth0":{"counters":{"bytes_sent":3000000,"bytes_received":1500000},"type":"broadcast"}},
					"status":"Running"
				}}`)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	im := newIncusManagerForTest(server)

	// First call establishes baseline.
	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.Equal(t, [2]uint64{0, 0}, stats[0].Bandwidth)

	// Rewind the network read time so the second call sees ~1 second elapsed.
	im.lastNetworkReadTime[defaultCacheTimeMs]["web"] = time.Now().Add(-time.Second)
	// Also seed a non-zero previous CPU so the second call computes a rate.
	im.lastCpuUsage[defaultCacheTimeMs]["web"] = 1000000000
	im.lastCpuReadTime[defaultCacheTimeMs]["web"] = time.Now().Add(-time.Second)

	stats, err = im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	require.Len(t, stats, 1)

	web := stats[0]
	// 2 MB delta over ~1 second → bytes/s should be close to 2 000 000.
	assert.Greater(t, web.Bandwidth[0], uint64(0), "sent rate must be positive")
	assert.LessOrEqual(t, web.Bandwidth[0], uint64(2_000_000), "sent rate must not exceed delta")
	assert.Greater(t, web.Bandwidth[1], uint64(0), "recv rate must be positive")

	// CPU: 1e9 ns delta over ~1 second ≈ 100 %.
	assert.Greater(t, web.Cpu, 0.0)
	assert.LessOrEqual(t, web.Cpu, 100.0)

	// Deprecated MB fields must mirror bandwidth bytes.
	assert.Equal(t, utils.BytesToMegabytes(float64(web.Bandwidth[0])), web.NetworkSent)
	assert.Equal(t, utils.BytesToMegabytes(float64(web.Bandwidth[1])), web.NetworkRecv)
}

func TestGetIncusStatsLoopbackExcluded(t *testing.T) {
	// lo bytes must not contribute to network totals.
	// eth0 grows by 1 000 bytes between calls; lo grows by 1 billion bytes.
	// If loopback leaked, the rate would be in the billions and exceed maxNetworkSpeedBps.
	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1.0/instances":
			fmt.Fprint(w, `{"status_code":200,"metadata":[
				{"name":"web","status":"Running","type":"container","config":{}}
			]}`)
		case "/1.0/instances/web/state":
			if call == 0 {
				fmt.Fprint(w, `{"status_code":200,"metadata":{
					"cpu":{"usage":0},"memory":{"usage":1048576},
					"network":{
						"eth0":{"counters":{"bytes_sent":1000,"bytes_received":500},"type":"broadcast"},
						"lo":{"counters":{"bytes_sent":1000000000,"bytes_received":1000000000},"type":"loopback"}
					},"status":"Running"}}`)
			} else {
				// eth0 +1000, lo +1 billion — loopback must be ignored.
				fmt.Fprint(w, `{"status_code":200,"metadata":{
					"cpu":{"usage":0},"memory":{"usage":1048576},
					"network":{
						"eth0":{"counters":{"bytes_sent":2000,"bytes_received":1500},"type":"broadcast"},
						"lo":{"counters":{"bytes_sent":2000000000,"bytes_received":2000000000},"type":"loopback"}
					},"status":"Running"}}`)
			}
			call++
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	im := newIncusManagerForTest(server)

	// First call establishes baseline; getIncusStats cycles trackers at the end.
	_, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)

	// Rewind read time so the second call sees ~1 second elapsed.
	im.lastNetworkReadTime[defaultCacheTimeMs]["web"] = time.Now().Add(-time.Second)

	// Second call: eth0 delta=1000, lo delta=1 billion (excluded).
	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	require.Len(t, stats, 1)

	// Rate must reflect eth0 only (~1000 B/s), not the loopback billions.
	assert.LessOrEqual(t, stats[0].Bandwidth[0], uint64(1000), "sent rate must reflect eth0 only, not loopback")
	assert.Greater(t, stats[0].Bandwidth[0], uint64(0))
}

func TestGetIncusStatsStaleInstancesPruned(t *testing.T) {
	listWith2 := `{"status_code":200,"metadata":[
		{"name":"web","status":"Running","type":"container","config":{}},
		{"name":"db","status":"Running","type":"container","config":{}}
	]}`
	listWith1 := `{"status_code":200,"metadata":[
		{"name":"web","status":"Running","type":"container","config":{}}
	]}`

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1.0/instances":
			if callCount == 0 {
				fmt.Fprint(w, listWith2)
			} else {
				fmt.Fprint(w, listWith1)
			}
		case "/1.0/instances/web/state":
			fmt.Fprint(w, incusWebState)
		case "/1.0/instances/db/state":
			fmt.Fprint(w, incusDbState)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	im := newIncusManagerForTest(server)

	// First call: both instances present.
	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	assert.Len(t, stats, 2)
	assert.Contains(t, im.containerStatsMap, "db")

	callCount++

	// Second call: db disappears — must be pruned from the map.
	stats, err = im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	assert.Len(t, stats, 1)
	assert.Equal(t, "web", stats[0].Name)
	assert.NotContains(t, im.containerStatsMap, "db", "stale entry must be removed")
}

// TestIncusNetworkCacheTimeIsolation verifies that rapid collections at one cache interval
// do not inflate rates at a different cache interval, mirroring the same test for Docker.
func TestIncusNetworkCacheTimeIsolation(t *testing.T) {
	// Single running instance with incrementing byte counters.
	bytesSent := uint64(1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/1.0/instances":
			fmt.Fprint(w, `{"status_code":200,"metadata":[
				{"name":"app","status":"Running","type":"container","config":{}}
			]}`)
		case "/1.0/instances/app/state":
			fmt.Fprintf(w, `{"status_code":200,"metadata":{
				"cpu":{"usage":0},
				"memory":{"usage":1048576},
				"network":{"eth0":{"counters":{"bytes_sent":%d,"bytes_received":%d},"type":"broadcast"}},
				"status":"Running"
			}}`, bytesSent, bytesSent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	im := newIncusManagerForTest(server)
	fastCache := uint16(1000)
	slowCache := uint16(60000)

	// Baseline: one call per cache time so both trackers have a "previous" value.
	// getIncusStats cycles trackers at the end, so after each call:
	//   tracker.previous = {app: bytesSent}, tracker.current = {}
	_, err := im.getIncusStats(fastCache)
	require.NoError(t, err)
	_, err = im.getIncusStats(slowCache)
	require.NoError(t, err)

	// Simulate several fast (1 s) collections, each adding 10 bytes.
	// The slow tracker's previous stays at the baseline value throughout.
	for i := 0; i < 5; i++ {
		bytesSent += 10
		im.lastNetworkReadTime[fastCache]["app"] = time.Now().Add(-time.Second)
		stats, err := im.getIncusStats(fastCache)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		assert.LessOrEqual(t, stats[0].Bandwidth[0], uint64(100), "fast-cache rate should be small")
	}
	// After the loop: bytesSent = 1050
	// fast.previous = {app: 1050} (updated each fast cycle)
	// slow.previous = {app: 1000} (untouched by fast cycles)

	// Slow collection: 50-byte total delta over ~5 seconds → ~10 B/s.
	im.lastNetworkReadTime[slowCache]["app"] = time.Now().Add(-5 * time.Second)
	stats, err := im.getIncusStats(slowCache)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.LessOrEqual(t, stats[0].Bandwidth[0], uint64(100), "slow-cache rate must not be inflated by fast-cache activity")
	assert.Greater(t, stats[0].Bandwidth[0], uint64(0), "slow-cache must still report traffic")
}

func TestGetIncusStatsBadMemoryRejected(t *testing.T) {
	// memory.usage exceeds maxMemoryUsage (100 TB) — the instance must be skipped gracefully.
	server := makeIncusServer(t, `{"status_code":200,"metadata":[
		{"name":"web","status":"Running","type":"container","config":{}}
	]}`, map[string]string{
		"web": `{"status_code":200,"metadata":{
			"cpu":{"usage":0},
			"memory":{"usage":999999999999999},
			"network":{},
			"status":"Running"
		}}`,
	})
	defer server.Close()

	im := newIncusManagerForTest(server)
	stats, err := im.getIncusStats(defaultCacheTimeMs)
	require.NoError(t, err)
	// The bad instance is skipped; no stats entry should exist.
	assert.Empty(t, stats)
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

func TestNewIncusManagerSocketNotFound(t *testing.T) {
	// Clear any explicit host override so auto-detection runs.
	t.Setenv("BESZEL_AGENT_INCUS_HOST", "")
	t.Setenv("INCUS_HOST", "")
	// The env var is set to "" which disables Incus explicitly — nil expected.
	im := newIncusManager()
	assert.Nil(t, im)
}

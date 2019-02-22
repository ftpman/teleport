/*
Copyright 2019 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backend

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gravitational/teleport/lib/defaults"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// ReporterConfig configures reporter wrapper
type ReporterConfig struct {
	// Backend is a backend to wrap
	Backend Backend
	// TrackTopRequests turns on tracking of top
	// requests on
	TrackTopRequests bool
	// TopRequestsCount sets up count of top requests to track
	TopRequestsCount int
}

// CheckAndSetDefaults checks and sets
func (r *ReporterConfig) CheckAndSetDefaults() error {
	if r.Backend == nil {
		return trace.BadParameter("missing parameter Backend")
	}
	if r.TopRequestsCount == 0 {
		r.TopRequestsCount = defaults.TopRequestsCapacity
	}
	return nil
}

// Reporter wraps a Backend implementation and reports
// statistics about the backend operations
type Reporter struct {
	// ReporterConfig contains reporter wrapper configuration
	ReporterConfig

	// topRequests is LRU cache with requests
	topRequests *simplelru.LRU

	// mutex protects topRequests cache
	mutex *sync.RWMutex
}

// NewReporter returns a new Reporter.
func NewReporter(cfg ReporterConfig) (*Reporter, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}
	r := &Reporter{
		ReporterConfig: cfg,
		mutex:          &sync.RWMutex{},
	}
	topRequests, err := simplelru.NewLRU(cfg.TopRequestsCount, nil)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	r.topRequests = topRequests
	return r, nil
}

// GetRange returns query range
func (s *Reporter) GetRange(ctx context.Context, startKey []byte, endKey []byte, limit int) (*GetResult, error) {
	start := s.Clock().Now()
	res, err := s.Backend.GetRange(ctx, startKey, endKey, limit)
	batchReadLatencies.Observe(time.Since(start).Seconds())
	batchReadRequests.Inc()
	if err != nil {
		batchReadRequestsFailed.Inc()
	}
	s.trackRequest(OpGet, startKey, endKey)
	return res, err
}

// Create creates item if it does not exist
func (s *Reporter) Create(ctx context.Context, i Item) (*Lease, error) {
	start := s.Clock().Now()
	lease, err := s.Backend.Create(ctx, i)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpPut, i.Key, nil)
	return lease, err
}

// Put puts value into backend (creates if it does not
// exists, updates it otherwise)
func (s *Reporter) Put(ctx context.Context, i Item) (*Lease, error) {
	start := s.Clock().Now()
	lease, err := s.Backend.Put(ctx, i)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpPut, i.Key, nil)
	return lease, err
}

// Update updates value in the backend
func (s *Reporter) Update(ctx context.Context, i Item) (*Lease, error) {
	start := s.Clock().Now()
	lease, err := s.Backend.Update(ctx, i)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpPut, i.Key, nil)
	return lease, err
}

// Get returns a single item or not found error
func (s *Reporter) Get(ctx context.Context, key []byte) (*Item, error) {
	start := s.Clock().Now()
	readLatencies.Observe(time.Since(start).Seconds())
	readRequests.Inc()
	item, err := s.Backend.Get(ctx, key)
	if err != nil && !trace.IsNotFound(err) {
		readRequestsFailed.Inc()
	}
	s.trackRequest(OpGet, key, nil)
	return item, err
}

// CompareAndSwap compares item with existing item
// and replaces is with replaceWith item
func (s *Reporter) CompareAndSwap(ctx context.Context, expected Item, replaceWith Item) (*Lease, error) {
	start := s.Clock().Now()
	lease, err := s.Backend.CompareAndSwap(ctx, expected, replaceWith)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil && !trace.IsNotFound(err) && !trace.IsCompareFailed(err) {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpPut, expected.Key, nil)
	return lease, err
}

// Delete deletes item by key
func (s *Reporter) Delete(ctx context.Context, key []byte) error {
	start := s.Clock().Now()
	err := s.Backend.Delete(ctx, key)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil && !trace.IsNotFound(err) {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpDelete, key, nil)
	return err
}

// DeleteRange deletes range of items
func (s *Reporter) DeleteRange(ctx context.Context, startKey []byte, endKey []byte) error {
	start := s.Clock().Now()
	err := s.Backend.DeleteRange(ctx, startKey, endKey)
	batchWriteLatencies.Observe(time.Since(start).Seconds())
	batchWriteRequests.Inc()
	if err != nil && !trace.IsNotFound(err) {
		batchWriteRequestsFailed.Inc()
	}
	s.trackRequest(OpDelete, startKey, endKey)
	return err
}

// KeepAlive keeps object from expiring, updates lease on the existing object,
// expires contains the new expiry to set on the lease,
// some backends may ignore expires based on the implementation
// in case if the lease managed server side
func (s *Reporter) KeepAlive(ctx context.Context, lease Lease, expires time.Time) error {
	start := s.Clock().Now()
	err := s.Backend.KeepAlive(ctx, lease, expires)
	writeLatencies.Observe(time.Since(start).Seconds())
	writeRequests.Inc()
	if err != nil && !trace.IsNotFound(err) {
		writeRequestsFailed.Inc()
	}
	s.trackRequest(OpPut, lease.Key, nil)
	return err
}

// NewWatcher returns a new event watcher
func (s *Reporter) NewWatcher(ctx context.Context, watch Watch) (Watcher, error) {
	w, err := s.Backend.NewWatcher(ctx, watch)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return NewReporterWatcher(ctx, w), nil
}

// Close releases the resources taken up by this backend
func (s *Reporter) Close() error {
	return s.Backend.Close()
}

// Clock returns clock used by this backend
func (s *Reporter) Clock() clockwork.Clock {
	return s.Backend.Clock()
}

// RequestStat is a statistic about request
type RequestStat struct {
	// Key is a tracking request key
	Key string `json:"key"`
	// Count is requests count
	Count int64 `json:"count"`
}

// TopRequests returns top requests statistics
func (s *Reporter) TopRequests() []RequestStat {
	requests := s.getTopRequests()
	sort.Slice(requests, func(i, j int) bool { return requests[i].Count > requests[j].Count })
	return requests
}

func (s *Reporter) getTopRequests() []RequestStat {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	keys := s.topRequests.Keys()
	requests := make([]RequestStat, 0, len(keys))
	for _, key := range keys {
		keyS, ok := key.(string)
		if !ok {
			continue
		}
		val, ok := s.topRequests.Get(keys)
		if !ok {
			continue
		}
		counterD, ok := val.(int64)
		if !ok {
			continue
		}
		requests = append(requests, RequestStat{Key: keyS, Count: counterD})
	}
	return requests
}

// trackRequests tracks top requests, endKey is supplied for ranges
func (s *Reporter) trackRequest(opType OpType, key []byte, endKey []byte) {
	if !s.TrackTopRequests {
		return
	}
	if len(key) == 0 {
		return
	}
	// take just the first two parts, otherwise too many distinct requests
	// can end up in the map
	parts := bytes.SplitN(key, []byte{Separator}, 3)
	rangeSuffix := ""
	if len(endKey) != 0 {
		// R denotes range queries in stat entry
		rangeSuffix = "R"
	}
	trackingKey := fmt.Sprintf("%v %v %v", opType.String(), rangeSuffix, string(bytes.Join(parts, []byte{Separator})))

	s.mutex.Lock()
	defer s.mutex.Unlock()

	counter := int64(1)
	val, ok := s.topRequests.Get(trackingKey)
	if ok {
		counterD, ok := val.(int64)
		if !ok {
			log.Warningf("Requests counter type is not int64: %v.", val)
		} else {
			counter = counterD + 1
		}
	}
	s.topRequests.Add(trackingKey, counter)
}

// ReporterWatcher is a wrapper around backend
// watcher that reports events
type ReporterWatcher struct {
	Watcher
}

// NewReporterWatcher creates new reporter watcher instance
func NewReporterWatcher(ctx context.Context, w Watcher) *ReporterWatcher {
	rw := &ReporterWatcher{
		Watcher: w,
	}
	go rw.watch(ctx)
	return rw
}

func (r *ReporterWatcher) watch(ctx context.Context) {
	watchers.Inc()
	defer watchers.Dec()
	select {
	case <-r.Done():
		return
	case <-ctx.Done():
		return
	}
}

var (
	watchers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "backend_watchers",
			Help: "Number of active backend watchers",
		},
	)
	writeRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_write_requests",
			Help: "Number of write requests to the backend",
		},
	)
	writeRequestsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_write_requests_failed",
			Help: "Number of failed write requests to the backend",
		},
	)
	batchWriteRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_batch_write_requests",
			Help: "Number of batch write requests to the backend",
		},
	)
	batchWriteRequestsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_batch_write_requests_failed",
			Help: "Number of failed write requests to the backend",
		},
	)
	readRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_read_requests",
			Help: "Number of read requests to the backend",
		},
	)
	readRequestsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_read_requests_failed",
			Help: "Number of failed read requests to the backend",
		},
	)
	batchReadRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_batch_read_requests",
			Help: "Number of read requests to the backend",
		},
	)
	batchReadRequestsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "backend_batch_read_requests_failed",
			Help: "Number of failed read requests to the backend",
		},
	)
	writeLatencies = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "backend_write_seconds",
			Help: "Latency for backend write operations",
			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^15 == 32.768 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		},
	)
	batchWriteLatencies = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "backend_batch_write_seconds",
			Help: "Latency for backend batch write operations",
			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^15 == 32.768 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		},
	)
	batchReadLatencies = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "backend_batch_read_seconds",
			Help: "Latency for batch read operations",
			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^15 == 32.768 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		},
	)
	readLatencies = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "backend_read_seconds",
			Help: "Latency for read operations",
			// lowest bucket start of upper bound 0.001 sec (1 ms) with factor 2
			// highest bucket start of 0.001 sec * 2^15 == 32.768 sec
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(watchers)
	prometheus.MustRegister(writeRequests)
	prometheus.MustRegister(writeRequestsFailed)
	prometheus.MustRegister(batchWriteRequests)
	prometheus.MustRegister(batchWriteRequestsFailed)
	prometheus.MustRegister(readRequests)
	prometheus.MustRegister(readRequestsFailed)
	prometheus.MustRegister(batchReadRequests)
	prometheus.MustRegister(batchReadRequestsFailed)
	prometheus.MustRegister(writeLatencies)
	prometheus.MustRegister(batchWriteLatencies)
	prometheus.MustRegister(batchReadLatencies)
	prometheus.MustRegister(readLatencies)
}

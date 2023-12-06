package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/prometheus/model/labels"
	flag "github.com/spf13/pflag"

	"prom-remote-write-shard/pkg"
	"prom-remote-write-shard/pkg/bloomfilter"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/resurgence72/persistentqueue"
)

const (
	version = "v0.1.0"

	metricShardKey = "metric"
	seriesShardKey = "series"

	namespace              = "prws"
	persistentQueueDirname = "persistent-queue"
)

type remote struct {
	addr       string
	api        *http.Client
	seriesChs  []chan *prompb.TimeSeries
	containers [][]*prompb.TimeSeries

	queue *persistentqueue.FastQueue

	shard int
}

var (
	ch *pkg.ConsistentHash

	// flag section
	promes     string
	shardKey   string
	addr       string
	remotePath string

	externalLabelsStr string
	externalLabels    map[string]string

	batch int
	shard int

	remoteWriteRetryTimes int
	remoteWriteTimeout    int // s
	remoteWriteMinWait    int // ms

	maxHourlySeries int
	maxDailySeries  int

	replicaFactor int

	h              bool
	v              bool
	wd             bool
	forceUseSelfTS bool

	persistentQueue        bool
	persistentQueuePath    string
	persistentQueueMaxSize int

	// pool section
	bufPool = &pkg.ByteBufferPool{}
	tsPool  = sync.Pool{New: func() any {
		return make([]prompb.TimeSeries, 0)
	}}

	// bloomfilter
	hourlySeriesLimiter *bloomfilter.Limiter
	dailySeriesLimiter  *bloomfilter.Limiter

	// prometheus section
	seriesKeepCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "series_keep_total",
		Help:      "series keep total",
	})
	seriesDropCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "series_drop_total",
		Help:      "series drop total",
	})

	hourlySeriesMaxLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "hourly_series_max_limit",
		Help:      "hourly series max limit count",
	})
	dailySeriesMaxLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "daily_series_max_limit",
		Help:      "daily series max limit count",
	})

	hourlySeriesLimitCurrentSeries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "hourly_series_limit_current_series",
		Help:      "hourly series limit current series",
	})
	dailySeriesLimitCurrentSeries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "daily_series_limit_current_series",
		Help:      "daily series limit current series",
	})

	hourlySeriesLimitRowsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "hourly_series_limit_rows_drop_total",
		Help:      "hourly series limit rows drop total",
	})
	dailySeriesLimitRowsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "daily_series_limit_rows_drop_total",
		Help:      "daily series limit rows drop total",
	})

	remoteWriteSendDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "remote_write_send_duration_seconds",
		Help:      "remote write send duration seconds",
		Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 19),
	})

	alive, lose map[string]struct{}
	ready       = make(chan struct{})

	m sync.Mutex
)

func initLog() {
	handler := slog.NewTextHandler(os.Stdout, nil)
	l := slog.New(handler)
	slog.SetDefault(l)
}

func initFlag() {
	flag.StringVar(&promes, "promes", "", "prometheus地址，多台使用 , 分割")
	flag.StringVar(&shardKey, "shard_key", "series", "根据什么来分片,metric/series")
	flag.StringVar(&addr, "listen", "0.0.0.0:9999", "http监听地址")
	flag.StringVar(&remotePath, "remote_path", "/api/v1/receive", "组件接收remote write的 http path")
	flag.StringVar(&externalLabelsStr, "external_labels", "", "添加 external_labels; env=prod,app=prom-remote-write-shard")

	flag.IntVar(&batch, "batch", 5000, "批量发送大小")
	flag.IntVar(&shard, "shard", 2, "每个remote write的分片数,必须为2的n次方")

	flag.IntVar(&remoteWriteRetryTimes, "remote_write_retry_times", 3, "remote write 重试次数")
	flag.IntVar(&remoteWriteTimeout, "remote_write_timeout", 5, "remote write 超时时间 (s)")
	flag.IntVar(&remoteWriteMinWait, "remote_write_min_wait", 200, "remote write 首次重试间隔 (ms)")

	flag.BoolVar(&wd, "watchdog", false, "是否开启 watchDog; 开启后会自动检测后端 promes 并根据健康状态自动加入/摘除 prome 节点")
	flag.BoolVar(&forceUseSelfTS, "force_use_self_ts", false, "是否将 series 强制设置为自身时间戳")

	flag.IntVar(&maxHourlySeries, "max_hourly_series", 0, "每小时最多发送的 series 数量")
	flag.IntVar(&maxDailySeries, "max_daily_series", 0, "每天最多发送的 series 数量")
	flag.IntVar(&replicaFactor, "replica_factor", 1, "数据转发的副本数, 通常小于等于后端节点数")

	flag.BoolVar(&persistentQueue, "persistent_queue", false, "是否开启 persistentqueue 功能")
	flag.StringVar(&persistentQueuePath, "persistent_queue_path", "./prws_queue", "persistentqueue 目录")
	flag.IntVar(&persistentQueueMaxSize, "persistent_queue_max_size", 5*1024*1024*1024, "每个 persistentqueue 的最大容量")

	flag.BoolVar(&v, "v", false, "版本信息")

	flag.Parse()

	if h {
		flag.Usage()
		os.Exit(0)
	}

	if v {
		slog.Warn("prom-remote-write-shard version", slog.String("version", version))
		os.Exit(0)
	}
}

func initPrometheusMetric() {
	// register prom metrics
	prometheus.Register(seriesKeepCounter)
	prometheus.Register(seriesDropCounter)
	prometheus.Register(hourlySeriesLimitRowsDropped)
	prometheus.Register(hourlySeriesMaxLimit)
	prometheus.Register(hourlySeriesLimitCurrentSeries)
	prometheus.Register(dailySeriesLimitRowsDropped)
	prometheus.Register(dailySeriesMaxLimit)
	prometheus.Register(dailySeriesLimitCurrentSeries)
	prometheus.Register(remoteWriteSendDurationSeconds)

	go func() {
		for range time.After(time.Hour) {
			hourlySeriesLimitCurrentSeries.Set(0)
		}
	}()

	go func() {
		for range time.After(24 * time.Hour) {
			dailySeriesLimitCurrentSeries.Set(0)
		}
	}()
}

func main() {
	initLog()
	initFlag()
	initPrometheusMetric()

	if len(promes) == 0 || len(strings.Split(promes, ",")) == 0 {
		slog.Error("--promes can not be empty")
		return
	}

	// 保证shard为2的n次方
	if !(shard > 0 && (shard&(shard-1)) == 0) {
		slog.Error("--shard has to be 2 to the n power")
		return
	}

	externalLabels = make(map[string]string)
	for _, labelkv := range strings.Split(externalLabelsStr, ",") {
		if kv := strings.Split(labelkv, "="); len(kv) == 2 &&
			len(strings.TrimSpace(kv[0])) > 0 &&
			len(strings.TrimSpace(kv[1])) > 0 {
			externalLabels[kv[0]] = kv[1]
		}
	}

	//if persistentQueue {
	//	if err := os.RemoveAll(persistentQueuePath); err != nil {
	//		slog.Error("remove persistentQueuePath", slog.String("persistentQueuePath", persistentQueuePath), slog.String("error", err.Error()))
	//		return
	//	}
	//}

	if maxHourlySeries > 0 {
		hourlySeriesLimiter = bloomfilter.NewLimiter(maxHourlySeries, time.Hour)
	}
	if maxDailySeries > 0 {
		dailySeriesLimiter = bloomfilter.NewLimiter(maxDailySeries, 24*time.Hour)
	}
	hourlySeriesMaxLimit.Set(float64(maxHourlySeries))
	dailySeriesMaxLimit.Set(float64(maxDailySeries))

	switch shardKey {
	case metricShardKey:
	case seriesShardKey:
	default:
		slog.Error("--shard_key not support, only support metric or series", slog.String("shard_key", shardKey))
		return
	}
	slog.Warn("shard key", slog.String("shard_key", shardKey))

	ps := strings.Split(promes, ",")
	alive = make(map[string]struct{}, len(ps))
	lose = make(map[string]struct{}, len(ps))

	var nodes []string
	for _, p := range ps {
		_, err := url.ParseRequestURI(p)
		if err != nil {
			slog.Error("remote write url parse failed", slog.String("url", p), slog.String("error", err.Error()))
		}

		nodes = append(nodes, p)
		alive[p] = struct{}{}
	}

	sort.Strings(nodes)
	ch = pkg.NewConsistentHash(nodes, replicaFactor, 0)
	ctx, cancel := context.WithCancel(context.Background())

	remoteSet := make(map[string]*remote, len(ps))
	existingQueues := make(map[string]struct{}, len(ps))
	for i, prom := range ps {
		client := retryWithBackOff(
			remoteWriteRetryTimes,
			time.Duration(remoteWriteMinWait)*time.Millisecond,
			time.Duration(remoteWriteTimeout)*time.Second,
		)

		// shard
		seriesChs := make([]chan *prompb.TimeSeries, 0, shard)
		containers := make([][]*prompb.TimeSeries, 0, shard)

		for i := 0; i < shard; i++ {
			seriesChs = append(seriesChs, make(chan *prompb.TimeSeries, batch))
			containers = append(containers, make([]*prompb.TimeSeries, 0, batch))
		}

		r := &remote{
			addr:       prom,
			api:        client,
			seriesChs:  seriesChs,
			containers: containers,
			shard:      shard,
		}

		if persistentQueue {
			queuePath := filepath.Join(persistentQueuePath, persistentQueueDirname, fmt.Sprintf("%d_%016X", i+1, xxhash.Sum64([]byte(prom))))
			sanitizedURL := fmt.Sprintf("%d:%s", i+1, prom)
			maxInmemoryBlocks := 100 * runtime.GOMAXPROCS(-1) * 2
			maxPendingBytes := persistentQueueMaxSize
			fq := persistentqueue.MustOpenFastQueue(
				queuePath,
				sanitizedURL,
				maxInmemoryBlocks,
				int64(maxPendingBytes),
			)
			existingQueues[fq.Dirname()] = struct{}{}

			r.queue = fq
		}
		remoteSet[prom] = r
	}

	if persistentQueue {
		queuesDir := filepath.Join(persistentQueuePath, persistentQueueDirname)
		files, err := os.ReadDir(queuesDir)
		if err != nil {
			slog.Error("read queues dir failed", slog.String("dir", queuesDir), slog.String("error", err.Error()))
			return
		}
		for _, f := range files {
			dirname := f.Name()
			if _, ok := existingQueues[dirname]; ok {
				continue
			}

			fullPath := filepath.Join(queuesDir, dirname)
			if err = os.RemoveAll(fullPath); err != nil {
				slog.Error("remove queues dir failed", slog.String("dir", fullPath), slog.String("error", err.Error()))
				return
			}
		}
	}

	var (
		i  int
		wg sync.WaitGroup
	)

	for prom := range remoteSet {
		r := remoteSet[prom]
		go consumer(ctx, &wg, i, r)
		i++

		if persistentQueue {
			go consumerWithQueue(ctx, &wg, r)
		}
	}

	read := func(r *http.Request, bb *pkg.ByteBuffer) error {
		if _, err := bb.ReadFrom(r.Body); err != nil {
			return err
		} else {
			return nil
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc(remotePath, func(w http.ResponseWriter, r *http.Request) {
		readBuf := bufPool.Get()
		defer bufPool.Put(readBuf)

		if err := read(r, readBuf); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		bb := bufPool.Get()
		defer bufPool.Put(bb)

		var err error
		bb.B, err = snappy.Decode(bb.B[:cap(bb.B)], readBuf.B)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err = proto.Unmarshal(bb.B, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var metricName string
		getLabelsHash := func(lbs []prompb.Label) uint64 {
			bb := bufPool.Get()
			b := bb.B[:0]
			for _, label := range lbs {
				name, value := label.GetName(), label.GetValue()
				if name == labels.MetricName {
					metricName = value
				}

				b = append(b, label.Name...)
				b = append(b, label.Value...)
			}
			h := xxhash.Sum64(b)
			bb.B = b
			bufPool.Put(bb)
			return h
		}

		for i := 0; i < len(req.Timeseries); i++ {
			ts := req.Timeseries[i]
			lbs := ts.GetLabels()

			var hash uint64
			switch shardKey {
			case seriesShardKey:
				hash = getLabelsHash(lbs)
			case metricShardKey:
				for _, label := range lbs {
					name, value := label.GetName(), label.GetValue()
					if name == labels.MetricName {
						metricName = value
						hash = xxhash.Sum64String(value)
						break
					}
				}
			}

			if !limitSeriesCardinality(metricName, hash) {
				continue
			}

			for _, idx := range ch.GetReplicaNodeIdx(hash, nil) {
				if rt, ok := remoteSet[nodes[idx]]; ok {
					select {
					case rt.seriesChs[hash&uint64(shard-1)] <- &ts:
						seriesKeepCounter.Inc()
					default:
						seriesDropCounter.Inc()
					}
				}
			}
		}
	})

	timeout := 30 * time.Second
	serve := http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       timeout,
		ReadHeaderTimeout: timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       timeout,
	}

	if wd {
		go watchDog(ctx)
		<-ready
	}

	slog.Warn("prom-remote-write-shard is ready to receive traffic")
	go serve.ListenAndServe()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	cancel()
	serve.Shutdown(ctx)
	wg.Wait()

	slog.Warn("prom-remote-write-shard exit")
}

func consumerWithQueue(ctx context.Context, wg *sync.WaitGroup, r *remote) {
	var (
		block []byte
		ok    bool
	)
	wg.Add(1)

	quitCh := make(chan struct{})
	defer func() {
		r.queue.MustClose()
		wg.Done()
		<-quitCh
	}()

	readCh := make(chan []byte)
	go func() {
		defer func() {
			close(readCh)
			close(quitCh)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			block, ok = r.queue.MustReadBlock(block[:0])
			if !ok {
				return
			}
			readCh <- block
		}
	}()

	errCh := make(chan error, 1)
	for {
		select {
		case block = <-readCh:
			go func() {
				startTime := time.Now()
				errCh <- send(r, block)
				remoteWriteSendDurationSeconds.Observe(time.Since(startTime).Seconds())
			}()

			select {
			case err := <-errCh:
				if err != nil && errors.Is(err, netWorkConnectErr) {
					r.queue.MustWriteBlock(block)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func limitSeriesCardinality(metric string, h uint64) bool {
	// 自身打点指标不做限制
	if len(metric) > len(namespace) && metric[:len(namespace)] == namespace {
		return true
	}

	hourlySeriesLimitCurrentSeries.Inc()
	dailySeriesLimitCurrentSeries.Inc()

	if hourlySeriesLimiter != nil && !hourlySeriesLimiter.Add(h) {
		hourlySeriesLimitRowsDropped.Inc()
		return false
	}
	if dailySeriesLimiter != nil && !dailySeriesLimiter.Add(h) {
		dailySeriesLimitRowsDropped.Inc()
		return false
	}
	return true
}

func consumer(ctx context.Context, wg *sync.WaitGroup, i int, r *remote) {
	slog.Warn("consumer start", slog.Int("consumer", i), slog.String("addr", r.addr), slog.Int("shard", r.shard))

	report := func(shard int) {
		if len(r.containers[shard]) == 0 {
			return
		}
		//slog.Warn("consumer shard report series",
		//	slog.Int("consumer", i),
		//	slog.Int("shard", shard),
		//	slog.Int("seriesLens", len(r.containers[shard])),
		//)
		defer func() {
			// re-slice
			r.containers[shard] = r.containers[shard][:0]
		}()

		c := tsPool.Get().([]prompb.TimeSeries)
		defer func() {
			c = c[:0]
			tsPool.Put(c)
		}()

		// copy
		selfTS := time.Now().UnixMilli()
		for idx := range r.containers[shard] {
			if forceUseSelfTS {
				r.containers[shard][idx].Samples[0].Timestamp = selfTS
			}

			// add external labels
			if len(externalLabels) > 0 {
				for ek, ev := range externalLabels {
					r.containers[shard][idx].Labels = append(r.containers[shard][idx].Labels, prompb.Label{
						Name:  ek,
						Value: ev,
					})
				}
			}
			c = append(c, *r.containers[shard][idx])
		}

		bb := bufPool.Get()
		defer bufPool.Put(bb)

		marshal, err := proto.Marshal(&prompb.WriteRequest{Timeseries: c})
		if err != nil {
			slog.Error("send series proto marshal failed", slog.String("error", err.Error()))
			return
		}

		// persistent local
		if persistentQueue {
			r.queue.MustWriteBlock(snappy.Encode(bb.B[:cap(bb.B)], marshal))
			return
		}

		// remote send
		go func() {
			startTime := time.Now()
			if err = send(r, snappy.Encode(bb.B[:cap(bb.B)], marshal)); err != nil {
				slog.Error("remote send series failed", slog.String("error", err.Error()))
			}
			remoteWriteSendDurationSeconds.Observe(time.Since(startTime).Seconds())
		}()

	}

	wg.Add(r.shard)
	for shard := 0; shard < r.shard; shard++ {
		go func(shard int) {
			ticker := time.NewTicker(5 * time.Second)
			defer func() {
				wg.Done()
				ticker.Stop()
				close(r.seriesChs[shard])
			}()

			for {
				select {
				case <-ctx.Done():
					report(shard)
					return
				case <-ticker.C:
					report(shard)
				case series := <-r.seriesChs[shard]:
					r.containers[shard] = append(r.containers[shard], series)
					if len(r.containers[shard]) == batch {
						report(shard)
					}
				}
			}
		}(shard)
	}
}

func retryWithBackOff(retry int, minWait, timeout time.Duration) *http.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = retry
	retryClient.RetryWaitMin = minWait
	retryClient.RetryWaitMax = timeout
	retryClient.HTTPClient.Timeout = timeout
	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		if err != nil {
			return false, err
		}

		// 如果响应码为 2xx 则不重试
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return false, nil
		}

		// 如果响应码 >= 500 或为 400 则不重试
		if resp.StatusCode >= 500 || resp.StatusCode == 400 {
			return false, nil
		}
		return true, nil
	}
	retryClient.Logger = slog.Default()
	return retryClient.StandardClient()
}

var (
	netWorkConnectErr = errors.New("network connect error")
	otherErr          = errors.New("other error")
)

func send(r *remote, req []byte) error {
	httpReq, err := http.NewRequest("POST", r.addr, bytes.NewReader(req))
	if err != nil {
		return otherErr
	}

	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "prom-remote-write-shard")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := r.api.Do(httpReq)
	if err != nil {
		slog.Error("http do failed", slog.String("error", err.Error()))
		return netWorkConnectErr
	}
	defer clean(resp)

	if resp.StatusCode >= 400 {
		all, _ := io.ReadAll(resp.Body)
		slog.Error("api do status code >= 400", slog.Int("code", resp.StatusCode), slog.Int("req-size", len(req)), slog.String("resp", string(all)))
		return otherErr
	}

	slog.Warn("api do success", slog.Int("code", resp.StatusCode), slog.Int("req-size", len(req)))
	return nil
}

func clean(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func watchDog(ctx context.Context) {
	loop := 3 * time.Second

	var once sync.Once
	isLose := func() {
		for {
			for addr := range alive {
				if !isHealthy(addr) {
					offline(addr)
					reHash()
				}
			}

			once.Do(func() {
				close(ready)
			})
			select {
			case <-time.After(loop):
			case <-ctx.Done():
				return
			}
		}
	}
	isAlive := func() {
		for {
			for addr := range lose {
				if isHealthy(addr) {
					online(addr)
					reHash()
				}
			}
			select {
			case <-time.After(loop):
			case <-ctx.Done():
				return
			}
		}
	}

	go isLose()
	go isAlive()
}

func reHash() {
	m.Lock()
	defer m.Unlock()

	var nodes []string
	for a := range alive {
		nodes = append(nodes, a)
	}

	sort.Strings(nodes)
	ch = pkg.NewConsistentHash(nodes, replicaFactor, 0)
	slog.Warn("hash ring has changed", slog.Int("alives", len(alive)), slog.String("nodes", strings.Join(nodes, ",")))
}

func online(addr string) {
	m.Lock()
	defer m.Unlock()
	slog.Warn("remote write endpoint online", slog.String("addr", addr))

	alive[addr] = struct{}{}
	delete(lose, addr)
}

func offline(addr string) {
	m.Lock()
	defer m.Unlock()
	slog.Warn("remote write endpoint offline:", slog.String("addr", addr))

	lose[addr] = struct{}{}
	delete(alive, addr)
}

func isHealthy(addr string) bool {
	parse, _ := url.Parse(addr)
	req, err := http.NewRequest("GET", fmt.Sprintf("%s://%s/-/healthy", parse.Scheme, parse.Host), nil)
	if err != nil {
		return false
	}

	resp, err := retryWithBackOff(
		3,
		100*time.Millisecond,
		time.Second,
	).Do(req)
	if err != nil {
		return false
	}

	defer clean(resp)
	return resp.StatusCode == 200
}

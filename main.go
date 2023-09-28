package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/prometheus/prometheus/model/labels"

	"prom-remote-write-shard/pkg"
	"prom-remote-write-shard/pkg/bloomfilter"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

const (
	version = "v0.0.7"

	metricShardKey = "metric"
	seriesShardKey = "series"

	namespace = "prws"
)

type remote struct {
	addr       string
	api        *http.Client
	seriesChs  []chan *prompb.TimeSeries
	containers [][]*prompb.TimeSeries

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

	h              bool
	v              bool
	wd             bool
	forceUseSelfTS bool

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

	namespaceRegexp = regexp.MustCompile("^" + namespace)

	alive, lose map[string]struct{}
	ready       = make(chan struct{})

	m sync.Mutex
)

func initFlag() {
	flag.StringVar(&promes, "promes", "http://localhost:9090/api/v1/write", "prometheus地址，多台使用 `,` 逗号分割")
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

	flag.BoolVar(&v, "v", false, "版本信息")

	flag.Parse()

	if h {
		flag.Usage()
		os.Exit(0)
	}

	if v {
		logrus.Warnln("version", version)
		os.Exit(0)
	}
}

func main() {
	initFlag()

	if len(promes) == 0 || len(strings.Split(promes, ",")) == 0 {
		logrus.Fatalln("promes can not be empty")
	}

	// 保证shard为2的n次方
	if !(shard > 0 && (shard&(shard-1)) == 0) {
		logrus.Fatalln("shard has to be 2 to the n power")
	}

	externalLabels = make(map[string]string)
	for _, labelkv := range strings.Split(externalLabelsStr, ",") {
		if kv := strings.Split(labelkv, "="); len(kv) == 2 {
			externalLabels[kv[0]] = kv[1]
		}
	}

	if maxHourlySeries > 0 {
		hourlySeriesLimiter = bloomfilter.NewLimiter(maxHourlySeries, time.Hour)
	}
	if maxDailySeries > 0 {
		dailySeriesLimiter = bloomfilter.NewLimiter(maxDailySeries, 24*time.Hour)
	}

	switch shardKey {
	case metricShardKey:
	case seriesShardKey:
	default:
		logrus.Fatalln("shardKey not support, only support metric or series")
	}
	logrus.Warnf("prom-remote-write-shard used [%s] shard key", shardKey)

	// register prom metrics
	prometheus.Register(seriesKeepCounter)
	prometheus.Register(seriesDropCounter)
	prometheus.Register(hourlySeriesLimitRowsDropped)
	prometheus.Register(dailySeriesLimitRowsDropped)

	ps := strings.Split(promes, ",")
	alive = make(map[string]struct{}, len(ps))
	lose = make(map[string]struct{}, len(ps))

	var nodes []string
	for _, p := range ps {
		_, err := url.ParseRequestURI(p)
		if err != nil {
			logrus.Fatalf("remote write url [%s] parse failed: [%s]", p, err)
		}

		nodes = append(nodes, p)
		alive[p] = struct{}{}
	}

	sort.Strings(nodes)
	ch = pkg.NewConsistentHash(nodes, 0)
	ctx, cancel := context.WithCancel(context.TODO())

	var wg sync.WaitGroup
	remoteSet := make(map[string]*remote, len(ps))
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

		remoteSet[prom] = r
		go consumer(ctx, &wg, i, r)
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
		if err := proto.Unmarshal(bb.B, &req); err != nil {
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

			if rt, ok := remoteSet[nodes[ch.GetNodeIdx(hash, nil)]]; ok && limitSeriesCardinality(metricName, hash) {
				select {
				case rt.seriesChs[hash&uint64(shard-1)] <- &ts:
					seriesKeepCounter.Inc()
				default:
					seriesDropCounter.Inc()
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

	logrus.Warnln("prom-remote-write-shard is ready to receive traffic")
	go serve.ListenAndServe()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	serve.Shutdown(ctx)
	cancel()
	wg.Wait()
}

func limitSeriesCardinality(metric string, h uint64) bool {
	if namespaceRegexp.MatchString(metric) {
		// 自身打点指标不做限制
		return true
	}

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
	logrus.Warnf("consumer [%d] start [%s], shards [%d] for pre consumer", i, r.addr, r.shard)

	report := func(shard int) {
		if len(r.containers[shard]) == 0 {
			return
		}
		logrus.Warnf("consumer [%d] - shard [%d] start report series, len [%d]", i, shard, len(r.containers[shard]))
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
			logrus.Errorln("send series proto marshal failed", err)
			return
		}

		// remote send
		go send(r, snappy.Encode(bb.B[:cap(bb.B)], marshal))
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
	retryClient.HTTPClient.Timeout = timeout
	retryClient.Logger = nil
	return retryClient.StandardClient()
}

func send(r *remote, req []byte) {
	httpReq, err := http.NewRequest("POST", r.addr, bytes.NewReader(req))
	if err != nil {
		return
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "prom-remote-write-shard")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := r.api.Do(httpReq)
	if err != nil {
		logrus.Errorln("api do failed", err)
		return
	}
	defer clean(resp)

	if resp.StatusCode >= 400 {
		all, _ := io.ReadAll(resp.Body)
		logrus.Errorln("api do status code >= 400", resp.StatusCode, string(all))
		return
	}
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
	ch = pkg.NewConsistentHash(nodes, 0)
	logrus.Warnf("now hash ring has [%d] nodes: %s", len(alive), strings.Join(nodes, ","))
}

func online(addr string) {
	m.Lock()
	defer m.Unlock()
	logrus.Warnln("remote write online:", addr)

	alive[addr] = struct{}{}
	delete(lose, addr)
}

func offline(addr string) {
	m.Lock()
	defer m.Unlock()
	logrus.Warnln("remote write offline:", addr)

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
	if err == nil && resp.StatusCode == 200 {
		defer clean(resp)
		return true
	}
	return false
}

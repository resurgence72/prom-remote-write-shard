package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"prom-remote-write-shard/pkg"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

const (
	version = "v0.0.1"

	MetricShardKey = "metric"
	SeriesShardKey = "series"

	MURMUR3Hash = "murmur3"
	CRC323Hash  = "crc32"
)

type remote struct {
	addr       string
	api        *http.Client
	seriesChs  []chan *prompb.TimeSeries
	containers [][]*prompb.TimeSeries

	shard int
}

var (
	// flag section
	promes        string
	shardKey      string
	addr          string
	remotePath    string
	hashAlgorithm string

	batch int
	shard int

	remoteWriteRetryTimes int
	remoteWriteTimeout    int // s
	remoteWriteMinWait    int // ms

	h bool
	v bool

	// pool section
	bufPool = &pkg.ByteBufferPool{}
	tsPool  = sync.Pool{New: func() any {
		return make([]prompb.TimeSeries, 0)
	}}
	builderPool = sync.Pool{New: func() any {
		return &bytes.Buffer{}
	}}

	// prometheus section
	promRWShardSeriesDropCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prom_rw_shard_series_drop_counter",
		Help: "prom rw shard series drop counter",
	})

	alive, lose map[string]struct{}
)

func initFlag() {
	flag.StringVar(&promes, "promes", "http://localhost:9090/api/v1/write", "prometheus地址，多台使用 `,` 逗号分割")
	flag.StringVar(&shardKey, "shard_key", "series", "根据什么来分片,metric/series")
	flag.StringVar(&addr, "listen", "0.0.0.0:9999", "http监听地址")
	flag.StringVar(&remotePath, "remote_path", "/api/v1/receive", "http remote路径")
	flag.StringVar(&hashAlgorithm, "hash_algorithm", "murmur3", "一致性哈希算法")

	flag.IntVar(&batch, "batch", 5000, "批量发送大小")
	flag.IntVar(&shard, "shard", 2, "每个remote write的分片数")

	flag.IntVar(&remoteWriteRetryTimes, "remote_write_retry_times", 3, "remote write 重试次数")
	flag.IntVar(&remoteWriteTimeout, "remote_write_timeout", 1, "remote write 超时时间 (s)")
	flag.IntVar(&remoteWriteMinWait, "remote_write_min_wait", 200, "remote write 首次重试间隔 (ms)")

	flag.BoolVar(&h, "h", false, "帮助信息")
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

	// register prom metrics
	prometheus.Register(promRWShardSeriesDropCounter)

	var ch *pkg.Map
	switch hashAlgorithm {
	case MURMUR3Hash:
		ch = pkg.New()
	case CRC323Hash:
		ch = pkg.New(pkg.WithCRC32Hash(crc32.ChecksumIEEE))
	default:
		logrus.Fatalln("hash algorithm not support, only support murmur3 or crc32 algorithm")
	}
	logrus.Warnf("prom-remote-write-shard used [%s] hash algorithm", hashAlgorithm)

	switch shardKey {
	case MetricShardKey:
	case SeriesShardKey:
	default:
		logrus.Fatalln("shardKey not support, only support metric or series")
	}
	logrus.Warnf("prom-remote-write-shard used [%s] shard key", shardKey)

	ps := strings.Split(promes, ",")
	alive = make(map[string]struct{}, len(ps))
	lose = make(map[string]struct{}, len(ps))

	for _, p := range ps {
		_, err := url.ParseRequestURI(p)
		if err != nil {
			logrus.Fatalf("remote write url [%s] parse failed: [%s]", p, err)
		}

		ch.Add(p)
		alive[p] = struct{}{}
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	var wg sync.WaitGroup
	remoteSet := make(map[string]*remote, len(ps))
	for i, prom := range ps {
		//client, _ := api.NewClient(api.Config{
		//	Address: prom,
		//	RoundTripper: &http.Transport{
		//		DialContext: (&net.Dialer{
		//			Timeout:   15 * time.Second,
		//			KeepAlive: 15 * time.Second,
		//		}).DialContext,
		//	},
		//})
		client := retryWithBackOff(
			remoteWriteRetryTimes,
			time.Duration(remoteWriteMinWait)*time.Millisecond,
			time.Duration(remoteWriteTimeout)*time.Second,
		)

		// shard
		var (
			seriesChs  []chan *prompb.TimeSeries
			containers [][]*prompb.TimeSeries
		)
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

		buf := builderPool.Get().(*bytes.Buffer)
		defer func() {
			buf.Reset()
			builderPool.Put(buf)
		}()
		for _, ts := range req.Timeseries {
			ts := ts

			lbs := ts.Labels
			switch shardKey {
			case SeriesShardKey:
				for _, label := range lbs {
					buf.WriteString(label.GetValue())
					buf.WriteByte('_')
				}
			case MetricShardKey:
				for _, label := range lbs {
					if label.GetName() == "__name__" {
						buf.WriteString(label.GetValue())
						break
					}
				}
			}

			select {
			case remoteSet[ch.Get(buf.Bytes())].seriesChs[murmur3.Sum32([]byte(ts.String()))%uint32(shard)] <- &ts:
			default:
				promRWShardSeriesDropCounter.Inc()
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

	go serve.ListenAndServe()
	go watchDog(ctx, ch)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	serve.Shutdown(ctx)
	cancel()
	wg.Wait()
}

func consumer(ctx context.Context, wg *sync.WaitGroup, i int, r *remote) {
	logrus.Warnf("consumer [%d] start [%s], shards [%d] for pre consumer", i, r.addr, r.shard)

	report := func(container []*prompb.TimeSeries) {
		if len(container) == 0 {
			return
		}

		defer func() {
			// re-slice
			container = container[:0]
		}()

		c := tsPool.Get().([]prompb.TimeSeries)
		defer func() {
			c = c[:0]
			tsPool.Put(c)
		}()

		// copy
		for _, series := range container {
			series := series
			c = append(c, *series)
		}

		// send series
		req := &prompb.WriteRequest{
			Timeseries: c,
		}

		bb := bufPool.Get()
		defer bufPool.Put(bb)

		marshal, err := proto.Marshal(req)
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
			defer wg.Done()

			container := r.containers[shard]
			for {
				select {
				case <-ctx.Done():
					report(container)
					close(r.seriesChs[shard])
					return
				case <-time.After(5 * time.Second):
					report(container)
				case series := <-r.seriesChs[shard]:
					container = append(container, series)

					if len(container) == cap(container) {
						report(container)
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

func watchDog(ctx context.Context, ch *pkg.Map) {
	loop := 3 * time.Second

	isLose := func() {
		for {
			select {
			case <-time.After(loop):
				for addr := range alive {
					if !isHealthy(addr) {
						offline(addr)
						reHash(ch)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}
	isAlive := func() {
		for {
			select {
			case <-time.After(loop):
				for addr := range lose {
					if isHealthy(addr) {
						online(addr)
						reHash(ch)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}

	go isLose()
	go isAlive()
}

func reHash(ch *pkg.Map) {
	var s []string
	for a := range alive {
		s = append(s, a)
	}
	ch.ReHash(s...)

	logrus.Warnf("now hash ring has [%d] nodes", len(alive))
}

func online(addr string) {
	logrus.Warnln("remote write online:", addr)

	alive[addr] = struct{}{}
	delete(lose, addr)
}

func offline(addr string) {
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

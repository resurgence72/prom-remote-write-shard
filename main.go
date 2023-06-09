package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/hashicorp/go-retryablehttp"

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
	version = "v0.0.3"

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
	ring *pkg.Map

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

	h              bool
	v              bool
	wd             bool
	forceUseSelfTS bool

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
	ready       = make(chan struct{})

	m sync.Mutex
)

func initFlag() {
	flag.StringVar(&promes, "promes", "http://localhost:9090/api/v1/write", "prometheus地址，多台使用 `,` 逗号分割")
	flag.StringVar(&shardKey, "shard_key", "series", "根据什么来分片,metric/series")
	flag.StringVar(&addr, "listen", "0.0.0.0:9999", "http监听地址")
	flag.StringVar(&remotePath, "remote_path", "/api/v1/receive", "组件接收remote write的 http path")
	flag.StringVar(&hashAlgorithm, "hash_algorithm", "murmur3", "一致性哈希算法")

	flag.IntVar(&batch, "batch", 5000, "批量发送大小")
	flag.IntVar(&shard, "shard", 2, "每个remote write的分片数,必须为2的n次方")

	flag.IntVar(&remoteWriteRetryTimes, "remote_write_retry_times", 3, "remote write 重试次数")
	flag.IntVar(&remoteWriteTimeout, "remote_write_timeout", 5, "remote write 超时时间 (s)")
	flag.IntVar(&remoteWriteMinWait, "remote_write_min_wait", 200, "remote write 首次重试间隔 (ms)")

	flag.BoolVar(&wd, "watchdog", false, "是否开启 watchDog; 开启后会自动检测后端 promes 并根据健康状态自动加入/摘除 prome 节点")
	flag.BoolVar(&forceUseSelfTS, "force_use_self_ts", false, "是否将 series 强制设置为自身时间戳")

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
	if !func() bool {
		return shard > 0 && (shard&(shard-1)) == 0
	}() {
		logrus.Fatalln("shard has to be 2 to the n power")
	}

	switch hashAlgorithm {
	case MURMUR3Hash:
		ring = pkg.New()
	case CRC323Hash:
		ring = pkg.New(pkg.WithCRC32Hash(crc32.ChecksumIEEE))
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

	// register prom metrics
	prometheus.Register(promRWShardSeriesDropCounter)

	ps := strings.Split(promes, ",")
	alive = make(map[string]struct{}, len(ps))
	lose = make(map[string]struct{}, len(ps))

	for _, p := range ps {
		_, err := url.ParseRequestURI(p)
		if err != nil {
			logrus.Fatalf("remote write url [%s] parse failed: [%s]", p, err)
		}

		ring.Add(p)
		alive[p] = struct{}{}
	}

	ctx, cancel := context.WithCancel(context.TODO())

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

		buf := builderPool.Get().(*bytes.Buffer)
		toByte := builderPool.Get().(*bytes.Buffer)
		defer func() {
			buf.Reset()
			toByte.Reset()
			builderPool.Put(buf)
			builderPool.Put(toByte)
		}()

		bufWrite := func(buf *bytes.Buffer, lbs []prompb.Label) {
			for _, label := range lbs {
				buf.WriteString(label.GetName())
				buf.WriteByte('_')
				buf.WriteString(label.GetValue())
			}
		}

		for i := range req.Timeseries {
			ts := req.Timeseries[i]
			reuse := false
			lbs := ts.GetLabels()

			switch shardKey {
			case SeriesShardKey:
				bufWrite(buf, lbs)
				reuse = true
			case MetricShardKey:
				for _, label := range lbs {
					if label.GetName() == "__name__" {
						buf.WriteString(label.GetValue())
						break
					}
				}
			}

			var hash uint32
			node, nh := ring.Get(buf.Bytes())
			if rt, ok := remoteSet[node]; ok {
				if reuse {
					hash = nh
				} else {
					toByte.Reset()
					bufWrite(toByte, lbs)
					hash = murmur3.Sum32(toByte.Bytes())
				}

				select {
				case rt.seriesChs[hash&uint32(shard-1)] <- &ts:
				default:
					promRWShardSeriesDropCounter.Inc()
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

func string2byte(s string) (b []byte) {
	/* #nosec G103 */
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	/* #nosec G103 */
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
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
	var s []string
	for a := range alive {
		s = append(s, a)
	}
	m.Unlock()

	ring.ReHash(s...)
	logrus.Warnf("now hash ring has [%d] nodes", len(alive))
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

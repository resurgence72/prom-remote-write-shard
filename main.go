package main

import (
	"bytes"
	"context"
	"flag"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"prom-remote-write-shard/pkg"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
)

const (
	version = "v0.0.1"

	MetricShardKey = "metric"
	SeriesShardKey = "series"

	MURMUR3Hash = "murmur3"
	CRC323Hash  = "crc32"
)

type remote struct {
	addr     string
	api      api.Client
	seriesCh chan *prompb.TimeSeries
}

var (
	promes        string
	shardKey      string
	addr          string
	remotePath    string
	hashAlgorithm string
	batch         int

	h bool
	v bool

	bufPool = &pkg.ByteBufferPool{}
	tsPool  = sync.Pool{New: func() any {
		return make([]prompb.TimeSeries, 0)
	}}
)

func initFlag() {
	flag.StringVar(&promes, "promes", "http://localhost:9090/api/v1/write", "prometheus地址，多台使用 `,` 逗号分割")
	flag.StringVar(&shardKey, "shard_key", "series", "根据什么来分片,metric/series")
	flag.StringVar(&addr, "listen", "0.0.0.0:9999", "http监听地址")
	flag.StringVar(&remotePath, "remote_path", "/api/v1/write", "http remote路径")
	flag.StringVar(&hashAlgorithm, "hash_algorithm", "murmur3", "一致性哈希算法")
	flag.IntVar(&batch, "batch", 5000, "批量发送大小")
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

	ps := strings.Split(promes, ",")

	var ch *pkg.Map
	switch hashAlgorithm {
	case MURMUR3Hash:
		ch = pkg.New()
	case CRC323Hash:
		ch = pkg.New(pkg.WithCRC32Hash(crc32.ChecksumIEEE))
	default:
		logrus.Fatalln("hash algorithm not support, only support murmur3 and crc32 algorithm")
	}

	ch.Adds(ps...)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	remoteSet := make(map[string]*remote, len(ps))

	for i, prom := range ps {
		client, _ := api.NewClient(api.Config{
			Address: prom,
			RoundTripper: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 5 * time.Second,
				}).DialContext,
			},
		})
		r := &remote{
			addr:     prom,
			api:      client,
			seriesCh: make(chan *prompb.TimeSeries, batch),
		}

		remoteSet[prom] = r
		go consumer(ctx, i, r)
	}

	read := func(r *http.Request, bb *pkg.ByteBuffer) error {
		if _, err := bb.ReadFrom(r.Body); err != nil {
			return err
		} else {
			return nil
		}
	}

	http.HandleFunc(remotePath, func(w http.ResponseWriter, r *http.Request) {
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

		for _, ts := range req.Timeseries {
			ts := ts
			var kv []string

			lbs := ts.Labels
			switch shardKey {
			case SeriesShardKey:
				for _, label := range lbs {
					kv = append(kv, label.Value)
				}
			case MetricShardKey:
				for _, label := range lbs {
					if label.Name == "__name__" {
						kv = append(kv, label.Value)
						break
					}
				}
			default:
				continue
			}
			sort.Strings(kv)
			remoteSet[ch.Get(strings.Join(kv, "_"))].seriesCh <- &ts
		}
	})

	http.ListenAndServe(addr, nil)
}

func consumer(ctx context.Context, i int, r *remote) {
	logrus.Warnln("consumer start", i, r.addr)

	container := make([]*prompb.TimeSeries, 0, batch)
	report := func() {
		if len(container) == 0 {
			return
		}

		c := tsPool.Get().([]prompb.TimeSeries)
		defer tsPool.Put(c)

		// copy
		for _, series := range container {
			series := series
			c = append(c, *series)
		}

		// re-slice
		container = container[:0]

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

	for {
		select {
		case <-ctx.Done():
			report()
			close(r.seriesCh)
			return
		case <-time.After(10 * time.Second):
			report()
		case series := <-r.seriesCh:
			container = append(container, series)

			if len(container) == cap(container) {
				report()
			}
		}
	}
}

func send(r *remote, req []byte) {
	logrus.Warnln("send batch", batch)
	httpReq, err := http.NewRequest("POST", r.addr, bytes.NewReader(req))
	if err != nil {
		return
	}

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "prom-remote-write-shard")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, _, err := r.api.Do(context.Background(), httpReq)
	if err != nil {
		logrus.Errorln("api do failed", err)
		return
	}

	if resp.StatusCode >= 400 {
		all, _ := io.ReadAll(resp.Body)
		logrus.Errorln("api do status code >= 400", resp.StatusCode, string(all))
		return
	}
}

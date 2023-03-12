package main

import (
	"bytes"
	"context"
	"flag"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"prom-remote-write-shard/pkg"
	"sort"
	"strings"
	"time"
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
		ch = pkg.New(500, nil)
	case CRC323Hash:
		ch = pkg.New(500, crc32.ChecksumIEEE)
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

	http.HandleFunc(remotePath, func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
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

		// copy
		c := make([]prompb.TimeSeries, 0, len(container))
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
		marshal, err := proto.Marshal(req)
		if err != nil {
			logrus.Errorln("send series proto marshal failed", err)
			return
		}

		// remote send
		go send(r, snappy.Encode(nil, marshal))
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

			if len(container) == batch {
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

### prom-remote-write-shard
> 用来接收 remote_write 数据，分片存储至后端 tsdb; 减轻单台 prometheus 机器的压力，查询测需要做 merge;
>
> 支持两种分片策略，基于 metric name 的，和基于 series 的；
>
> 一致性哈希分片，支持两种哈希算法选择，murmur3 和 crc32;




#### 参数文档
> ```shell
> go run main.go -h
> 
> Usage of C:\Users\y\AppData\Local\Temp\go-build876467282\b001\exe\main.exe:
> -batch int
> 		批量发送大小 (default 5000)
> -h    帮助信息
> -listen string
> 		http监听地址 (default "0.0.0.0:9999")
> -promes string
> 		prometheus地址，多台使用 `,` 逗号分割 (default "http://localhost:9090/api/v1/write")
> -hash_algorithm string                     
>         一致性哈希算法 (default "murmur3")
> -remote_path string
> 		组件接收remote write的 http path (default "/api/v1/receive")
> -shard_key string
> 		根据什么来分片,metric/series (default "series")
> -shard int
>        每个remote write的分片数 (default 2)
>   -remote_write_min_wait int
>        remote write 首次重试间隔 (ms) (default 200)
> -remote_write_retry_times int
>        remote write 重试次数 (default 3)
> -remote_write_timeout int
>        remote write 超时时间 (s) (default 1)
> -v    版本信息
> ```



#### 使用说明

> ```shell
> go build -o prom-remote-wirte-shard main.go
> 
> 
> ./prom-remote-wirte-shard \
> -listen 0.0.0.0:9999 \
> -promes http://localhost:9090/api/v1/write,http://localhost:9091/api/v1/write \
> -shard_key series
> 
> 
> # 运行后修改监控系统的remote write 地址即可实现分片效果；
> # 例如修改 categraf,prometheus,夜莺等监控系统的 remote_write 配置为  http://0.0.0.0:9999/api/v1/write  即 http://${listen}/${remote_path}
> ```


#### 自监控
> 项目提供 /metrics 接口供 prometheus 抓取；暴漏 prom_rw_shard_series_drop_counter 指标，标识当前 chan 写满之后 drop 的 series 数量，衡量压力
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
> 		prometheus地址，使用,分割 (default "http://localhost:9090/api/v1/write")
> -hash_algorithm string                     
>         一致性哈希算法 (default "murmur3")
> -remote_path string
> 		http remote路径 (default "/api/v1/write")
> -shard_key string
> 		根据什么来分片,metric/series (default "series")
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
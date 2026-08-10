## Usage
```bash
./webdav_bench.py curl 192.168.95.1 --size 200 --repeats 3          # through proxy, curl
./webdav_bench.py rclone webdav_proxy --size 200 --repeats 3        # through proxy, rclone
./webdav_bench.py rclone webdav_direct --size 200 --repeats 3       # bypass proxy
```
基于 epoll + SO_REUSEPORT 的高并发 HTTP 服务器，单机 QPS 23万+。

## 特性

- **多线程 Reactor**：每个线程独立 epoll，无锁竞争
- **SO_REUSEPORT**：内核按四元组哈希分发连接
- **边缘触发 ET**：循环读写直到 EAGAIN
- **HTTP/1.1 keep-alive**：长连接复用
- **RESTful API**：支持 GET/POST/PUT/DELETE
- **JSON 处理**：nlohmann/json 序列化
- **用户系统**：注册/登录/Token 鉴权

## 编译运行

```bash
g++ -std=c++17 -O3 -pthread -o server server.cpp
./server


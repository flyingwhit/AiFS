# AIFS Phase 1 — 控制面与分布式底座对接：核心函数速查

> 说明：类型 `*kvclient.Client` 即控制面访问 KV 的 HTTP 客户端（非 `KVClient` 命名）。

---

## 一、运行环境初始化与进程守护

### `NewCluster(n int, httpAddrs []string, maxRaftState int) (*Cluster, error)`  
`package kvraft/runtime` · `cluster.go`

1. **函数签名**：`func NewCluster(n int, httpAddrs []string, maxRaftState int) (*Cluster, error)`
2. **核心职责（一句话大白话）**：在内存里拉起一整组 Raft KV 节点，每个节点挂上 HTTP，并把节点之间的 **labrpc 模拟网络** 建好。
3. **主要物理流程**：
   1. 校验 `len(httpAddrs) == n`，否则直接返回错误。
   2. `labrpc.MakeNetwork()` 创建进程内网络；为每对 `(i,j)` 生成 `raft-i-to-j` 端点名。
   3. 对每个 `i` 调用内部 `startServer`：`MakeEnd` + `Connect` 到对端、**labrpc** `AddServer` 注册 KV/Raft 两套 RPC 服务、`kvraft.StartKVServer`、`kvraft.StartHTTPServer`。
   4. `connectAll()` 打开所有方向的 **labrpc** 链路。
   5. 任一步失败则 `Shutdown()` 后返回错误。
4. **并发与阻塞特性**：同步；`startServer` 内 `StartHTTPServer` 会 **go func** 起 HTTP `ListenAndServe`（不阻塞 `NewCluster` 返回）。

---

### `(c *Cluster) connectAll()`  
`package kvraft/runtime` · `cluster.go`

1. **函数签名**：`func (c *Cluster) connectAll()`
2. **核心职责（一句话大白话）**：把集群里所有 Raft 节点之间的 **labrpc** 连接全部“通电”，让 AppendEntries / RequestVote 等能互通。
3. **主要物理流程**：
   1. 双重循环 `i,j`，对 `c.names[i][j]` 与 `c.names[j][i]` 调用 `c.net.Enable(..., true)`。
   2. 不发起 HTTP；仅影响 **labrpc** 层是否投递 RPC。
4. **并发与阻塞特性**：同步、无阻塞等待。

---

### `StartHTTPServer(kv *kvraft.KVServer, addr string) *http.Server`  
`package kvraft` · `http.go`

1. **函数签名**：`func StartHTTPServer(kv *kvraft.KVServer, addr string) *http.Server`
2. **核心职责（一句话大白话）**：给单个 KV 节点开一个 **HTTP** 入口（`/kv`），把控制面的 JSON 请求转进 `KVServer`。
3. **主要物理流程**：
   1. `ServeMux` 注册 `POST /kv` → `kv`（`KVServer` 实现 `http.Handler`）。
   2. 组装 `http.Server{Addr, Handler}`。
   3. **go func** 里 `ListenAndServe` 常驻监听。
   4. 立即把 `*http.Server` 返回给调用方。
4. **并发与阻塞特性**：调用方同步返回；真正监听在 **goroutine** 中异步跑；`ListenAndServe` 在子 goroutine 内阻塞直到 `Close`/错误。

---

### 信号守护与优雅退出（`cmd/kvcluster/main.go` · `cmd/controller/main.go`）

#### `kvcluster`：`main()` 尾部

1. **函数签名**：`func main()`（内含 `signal.Notify`、`<-sig`、`cluster.Shutdown()`）
2. **核心职责（一句话大白话）**：KV 集群进程跑起来后，**阻塞等 SIGINT/SIGTERM**，收到后关掉 HTTP 与 Raft。
3. **主要物理流程**：
   1. `sig := make(chan os.Signal, 1)`，`signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)`。
   2. `<-sig` **阻塞主 goroutine** 直到信号到达。
   3. `cluster.Shutdown()`：关闭各节点 **HTTP.Server**、`KVServer.Kill()`、**labrpc** `Network.Cleanup()`。
4. **并发与阻塞特性**：主线程在 `<-sig` 上 **同步阻塞**；不是内核死锁，是正常信号等待。

#### `controller`：`main()` 尾部

1. **函数签名**：`func main()`（内含 `signal.Notify`、`<-sig`、`srv.Shutdown(ctx)`）
2. **核心职责（一句话大白话）**：控制面 HTTP 跑起来后等退出信号，再 **限时** 关掉 Controller 的 `http.Server`。
3. **主要物理流程**：
   1. 同上用 `signal.Notify` + `<-sig` 阻塞等待。
   2. `context.WithTimeout(5s)` 后 `srv.Shutdown(ctx)`：停止接受新 **HTTP** 连接并等已有请求收尾（超时则强制推进）。
4. **并发与阻塞特性**：`<-sig` 同步阻塞；`Shutdown` 同步调用，内部可能短暂阻塞于连接关闭。

---

### `(c *Cluster) Shutdown()`  
`package kvraft/runtime` · `cluster.go`

1. **函数签名**：`func (c *Cluster) Shutdown()`
2. **核心职责（一句话大白话）**：把测试/进程里起的 KV 集群一次性关掉，避免 goroutine 与端口泄漏。
3. **主要物理流程**：
   1. 对每个节点：`http.Server.Close()`（停止 **HTTP**）。
   2. `KVServer.Kill()` → 内部 `raft.Kill()`，让 Raft 长循环退出。
   3. `labrpc.Network.Cleanup()` 清理模拟网络。
4. **并发与阻塞特性**：同步；`Close`/`Kill` 通常较快返回，不设计为无限阻塞。

---

## 二、Worker 进程生命周期（`cmd/worker/main.go` · `models/worker.go`）

### 命令行：`flag.String` / `flag.Int` / `flag.Duration` + `flag.Parse()`

1. **函数签名**：`func main()` 内：`flag.String(...)`、`flag.Int(...)`、`flag.Duration(...)`、`flag.Parse()`
2. **核心职责（一句话大白话）**：从命令行读 Controller 地址、本机宣告 IP/端口、状态、可选心跳周期；**保底**用默认值（如 controller `http://127.0.0.1:9000`）。
3. **主要物理流程**：
   1. 注册各 flag 指针与默认值、说明字符串。
   2. `flag.Parse()` 解析 `argv`，覆盖默认值。
   3. 后续逻辑用解引用后的 `*controllerURL`、`*interval` 等。
4. **并发与阻塞特性**：同步；无网络。

---

### `(w *WorkerInfo) EnsureID()`  
`package models` · `worker.go`

1. **函数签名**：`func (w *WorkerInfo) EnsureID()`
2. **核心职责（一句话大白话）**：若 Worker 没带 `ID`，就用 **`IP:Port`** 拼一个稳定身份，后面 KV key 和索引都靠它。
3. **主要物理流程**：
   1. 若 `w.ID == ""`，则 `w.ID = fmt.Sprintf("%s:%d", w.IP, w.Port)`。
   2. 否则不改。
4. **并发与阻塞特性**：纯内存、同步、无 IO。

---

### `register(base string, info models.WorkerInfo) error` · 定时心跳：`main()` 内 `time.NewTicker` + `heartbeat`

1. **函数签名**：`func register(base string, info models.WorkerInfo) error`；心跳：`func heartbeat(base string, info models.WorkerInfo) error`；循环在 `main()`：`for range ticker.C { ... }`
2. **核心职责（一句话大白话）**：**register** 做一次注册；若 `interval > 0`，则按周期 **heartbeat** 刷状态到控制面。
3. **主要物理流程**：
   1. `register`：`postJSON(base+"/workers/register", info)` → **HTTP POST** 到 Controller。
   2. 若 `*interval <= 0`，`main` 直接 `return`。
   3. 否则 `time.NewTicker(*interval)`，`for range ticker.C` 每次调 `heartbeat` → `postJSON(.../workers/heartbeat, ...)` → **HTTP POST**。
   4. 心跳失败只打日志 `continue`，不退出进程。
4. **并发与阻塞特性**：`register`/`heartbeat` 内 **HTTP** 同步阻塞；`for range ticker.C` 在 **主 goroutine** 上按间隔阻塞等待 tick；无额外线程（除 `http` 库内部）。

---

### `postJSON(url string, body interface{}) error`  
`package main` · `cmd/worker/main.go`

1. **函数签名**：`func postJSON(url string, body interface{}) error`
2. **核心职责（一句话大白话）**：把任意可 JSON 序列化的结构体 **POST** 到给定 URL，并检查 HTTP 状态码。
3. **主要物理流程**：
   1. `json.Marshal(body)` 得到 body 字节。
   2. `http.Post(url, "application/json", bytes.NewReader(data))` 发起 **HTTP**。
   3. 读状态码：非 2xx 则返回 `fmt.Errorf("http %s", status)`。
4. **并发与阻塞特性**：同步阻塞在 **HTTP 往返**；超时行为取决于默认 `http.Client`（未显式设置 Timeout，可能较长）。

---

## 三、控制面大脑（`controlplane/controller/controller.go`）

### `(c *Controller) handleRegister(w http.ResponseWriter, r *http.Request)`

1. **函数签名**：`func (c *Controller) handleRegister(w http.ResponseWriter, r *http.Request)`
2. **核心职责（一句话大白话）**：接住 Worker 发来的 **HTTP 注册**，校验 JSON，再交给业务层落 KV，最后写回 JSON 响应。
3. **主要物理流程**：
   1. `decodeWorker(r)`：`io.ReadAll` + `json.Unmarshal` → `models.WorkerInfo`；校验 `IP`/`Port`；空 `Status` 默认 `idle`；`EnsureID()`。
   2. `c.RegisterWorker(info)`：触发下游 **kvclient → KV HTTP → Raft**（见下一节）。
   3. 失败：`http.Error` 400/502；成功：`writeJSON` 200 + `{"status":"registered","id":...}`。
4. **并发与阻塞特性**：在 **Controller 的 HTTP handler goroutine** 中同步执行；阻塞点主要在 `RegisterWorker` 内的 **HTTP 客户端** 调用链。

---

### `(c *Controller) RegisterWorker(info models.WorkerInfo) error`

1. **函数签名**：`func (c *Controller) RegisterWorker(info models.WorkerInfo) error`
2. **核心职责（一句话大白话）**：把 Worker 打成 **KV 里的一条记录**（key + JSON value），并更新内存里的 **workers 索引**，再写回 `workers/index` 这条 KV。
3. **主要物理流程**：
   1. `info.EnsureID()`；`json.Marshal(info)` → `data`。
   2. `key := models.WorkerKey(info)`（形如 `worker/{id}`）。
   3. `c.kv.Put(key, string(data))`：**HTTP POST** 到某 KV 节点 `/kv`（内部寻主重试）。
   4. `addToIndex(info.ID)` → `saveIndexLocked()` → 再次 `c.kv.Put(workersIndexKey, json(ids))`：**第二次 HTTP** 写索引。
4. **并发与阻塞特性**：同步；两次 `Put` 均可能阻塞在 **HTTP + KVServer 内 Raft 等待提交**。

---

## 四、客户端软路由与分布式寻主（`controlplane/kvclient/client.go`）

> 结构体名：`Client`（`*kvclient.Client`）。`Put` 内部通过 `execute` 调用 `nextAddr` / `doOnce`。

### `(c *Client) nextAddr() string`

1. **函数签名**：`func (c *Client) nextAddr() string`
2. **核心职责（一句话大白话）**：**轮询**下一个 KV 节点的 base URL，用来在“不是 Leader / 超时”时换一台试。
3. **主要物理流程**：
   1. 加锁读 `c.addrs[c.curr]`，然后 `c.curr = (c.curr+1) % len`。
   2. 无网络调用。
4. **并发与阻塞特性**：同步、极短；`sync.Mutex` 保护轮询游标。

---

### `(c *Client) doOnce(addr string, req kvraft.HTTPRequest) (kvraft.HTTPResponse, error)`

1. **函数签名**：`func (c *Client) doOnce(addr string, req kvraft.HTTPRequest) (kvraft.HTTPResponse, error)`
2. **核心职责（一句话大白话）**：对**某一个** KV 地址发一次 **`POST {addr}/kv`**，把 JSON 请求体与 JSON 响应体编解码好。
3. **主要物理流程**：
   1. `json.Marshal(req)` → `HTTPRequest{op,key,value}`。
   2. `http.NewRequest(POST, addr+"/kv", body)`，`Content-Type: application/json`。
   3. `c.http.Do`：**HTTP 客户端** 同步请求（`Client` 自带 **2s Timeout**）。
   4. `ReadAll` + `json.Unmarshal` → `kvraft.HTTPResponse`（含 `Err` 字符串）。
4. **并发与阻塞特性**：同步阻塞在单次 **HTTP**；超时会返回 `error`，由上层 `execute` 触发 `continue` 换节点。

---

### `(c *Client) execute(req kvraft.HTTPRequest) (kvraft.HTTPResponse, error)`

1. **函数签名**：`func (c *Client) execute(req kvraft.HTTPRequest) (kvraft.HTTPResponse, error)`
2. **核心职责（一句话大白话）**：最多试 **len(addrs)** 次：每次换一个地址发 `doOnce`，直到拿到“可接受”的 `HTTPResponse`，否则报没有可用 Leader。
3. **主要物理流程**：
   1. 若 `addrs` 为空，直接错误返回。
   2. `for i := 0; i < len(addrs); i++`：`addr := nextAddr()` → `doOnce(addr, req)`。
   3. `doOnce` 返回 **error**（网络/超时/解码）：`continue` 下一轮。
   4. 若 `kvResp.Err == ErrWrongLeader` 或 `ErrTimeOut`：**continue**（换下一台 KV，期望轮到 Leader 或 Leader 已稳定）。
   5. 否则 `return kvResp, nil`。
   6. 全部未成功 → `error: no available kv leader`。
4. **并发与阻塞特性**：同步；最坏 **O(n)** 次 **HTTP** 往返，每次最多阻塞约 **2s**（`http.Client.Timeout`）。

---

### `(c *Client) Put(key, value string) error`

1. **函数签名**：`func (c *Client) Put(key, value string) error`
2. **核心职责（一句话大白话）**：以 **`op: put`** 把一对 KV 写进 Raft 集群；自动吃 **WrongLeader / TimeOut** 的软重试，只对最终业务错误返回 `fmt.Errorf`。
3. **主要物理流程**：
   1. `execute(kvraft.HTTPRequest{Op: "put", Key: key, Value: value})`。
   2. 内部：`nextAddr` 轮询 + `doOnce` **HTTP POST** `/kv`；遇 `ErrWrongLeader` / `ErrTimeOut` 或传输错误 → **continue** 下一轮，不向上抛（直到 `execute` 成功或耗尽次数）。
   3. `execute` 返回的 `resp.Err != "OK"` 时，`Put` 包装为 `kv put: ...` 错误（例如 `ErrNoKey` 不应出现在 put 路径，但会被如实返回）。
4. **并发与阻塞特性**：**同步阻塞**；可能连续多次 **HTTP**；不是 `go func` 异步 fire-and-forget；亦非内核死锁，是带超时与重试上限的用户态等待。

---

## 附：与 Phase 1 强相关但未单独成节的内部函数（便于对照）

| 位置 | 符号 | 作用摘要 |
|------|------|----------|
| `runtime/cluster.go` | `(c *Cluster) startServer(i, maxRaftState)` | 单节点：labrpc 建链、`StartKVServer`、`StartHTTPServer` |
| `controller/controller.go` | `decodeWorker(r)` | 读 **HTTP** body → `WorkerInfo` |
| `kvraft/http.go` | `(kv *KVServer) ServeHTTP` | **HTTP POST /kv** 入口 → `executeHTTP` → `PutAppend`/`Get`/`Delete` |
| `kvraft/server.go` | `(kv *KVServer) waitApplied(...)` | **select** 阻塞等 `notifyChans[index]`，与 `applier` 闭环 |

# Phase 1 控制面 — Worker 注册（RegisterWorker）调用与数据流

## 部署拓扑（cluster.go 启动态）

```mermaid
graph TD
    subgraph RuntimeCluster["kvraft/runtime/cluster.go"]
        NC["NewCluster(n, httpAddrs, maxRaftState)"]
        SS["startServer(i)"]
        SKV["kvraft.StartKVServer(ends, i, persister, maxRaftState)"]
        RM["raft.Make(..., applyCh)"]
        SH["kvraft.StartHTTPServer(kv[i], httpAddrs[i])"]
        CA["connectAll() — labrpc 全网互通"]
        NC --> SS
        SS --> SKV
        SKV --> RM
        SKV --> GO_AP["go kv.applier()"]
        SS --> SH
        NC --> CA
    end

    subgraph Nodes["每节点进程内"]
        HTTP_KV[":8001 / :8002 / :8003<br/>POST /kv"]
        RAFT["Raft 共识层<br/>labrpc AppendEntries"]
        KV["KVServer"]
        HTTP_KV --> KV
        KV --> RAFT
        RAFT -->|ApplyMsg| AP["applier goroutine"]
        AP --> KV
    end

    SH --> HTTP_KV
```

## RegisterWorker 完整链路

```mermaid
graph TD
    %% ── Worker 侧 ──
    subgraph Worker["cmd/worker — Worker 进程"]
        W1["构造 models.WorkerInfo<br/>{ip, port, gpus, status}"]
        W2["info.EnsureID()"]
        W3["json.Marshal → HTTP Body"]
        W4["http.Post<br/>POST /workers/register"]
        W1 --> W2 --> W3 --> W4
    end

    %% ── Controller HTTP 路由 ──
    subgraph ControllerHTTP["controlplane/controller — HTTP :9000"]
        C_MUX["StartHTTPServer mux<br/>/workers/register → Controller"]
        C_SH["Controller.ServeHTTP"]
        C_HR["handleRegister"]
        C_DW["decodeWorker(r)<br/>io.ReadAll + json.Unmarshal → WorkerInfo"]
        C_RW["RegisterWorker(info)"]
        C_MUX --> C_SH --> C_HR --> C_DW --> C_RW
    end

  W4 -->|application/json<br/>WorkerInfo| C_MUX

    %% ── Controller 业务：JSON 打包为 KV ──
    subgraph ControllerLogic["RegisterWorker 逻辑"]
        R1["info.EnsureID()"]
        R2["json.Marshal(info) → data"]
        R3["key = models.WorkerKey(info)<br/>worker/{id}"]
        R4["kvclient.Client.Put(key, string(data))"]
        R5["addToIndex(id)"]
        R6["saveIndexLocked()"]
        R7["kvclient.Put('workers/index', json(ids))"]
        C_RW --> R1 --> R2 --> R3 --> R4
        R4 -->|成功| R5 --> R6 --> R7
    end

    %% ── kvclient Leader 轮询 ──
    subgraph KVClient["controlplane/kvclient/client.go"]
        P1["Client.Put(key, value)"]
        EX["execute(HTTPRequest{op:'put', key, value})"]
        NA["nextAddr() — 轮询 curr++ % len(addrs)"]
        DO["doOnce(addr, req)"]
        MAR["json.Marshal → POST {addr}/kv"]
        HDO["http.Client.Do"]
        PAR["json.Unmarshal → HTTPResponse"]
        RETRY{"Err == ErrWrongLeader<br/>或 ErrTimeOut ?"}
        OKC{"Err == OK ?"}
        FAIL["返回 error<br/>no available kv leader"]
        P1 --> EX
        EX --> NA --> DO
        DO --> MAR --> HDO --> PAR --> RETRY
        RETRY -->|是, i < tries| NA
        RETRY -->|否, 成功| OKC
        RETRY -->|全部失败| FAIL
    end

    R4 --> P1
    R7 --> P1

    %% ── KV HTTP 入口 ──
    subgraph KVHTTP["kvraft/http.go — KVServer HTTP"]
        K_MUX["StartHTTPServer mux /kv"]
        K_SH["KVServer.ServeHTTP"]
        K_RD["io.ReadAll + json.Unmarshal<br/>→ HTTPRequest{op,key,value}"]
        K_EX["executeHTTP('put', key, value)"]
        K_SEQ["nextHTTPSeq()"]
        K_PA["PutAppend(PutAppendArgs{Op:'Put', ...})"]
        K_MUX --> K_SH --> K_RD --> K_EX --> K_SEQ --> K_PA
    end

    HDO -->|POST /kv JSON| K_MUX
    OKC -->|HTTP 200 err:OK| C_HR
    FAIL -->|502 Bad Gateway| C_HR

    %% ── PutAppend + Raft Start + waitApplied 阻塞闭环 ──
    subgraph KVPut["kvraft/server.go — PutAppend"]
        PA1["构造 Op{Type:'Put', Key, Value, SeqId, ClientId}"]
        PA2{"recordMap 去重?<br/>SeqId <= LastSeqId"}
        PA3["rf.Start(command)<br/>→ index, startTerm, isLeader"]
        PA4{"isLeader?"}
        PA_WL["reply.Err = ErrWrongLeader"]
        PA5["waitApplied(index, startTerm)"]
        PA6["delete(notifyChans, index)"]
        PA7["填充 reply.Err"]
        K_PA --> PA1 --> PA2
        PA2 -->|命中缓存| PA7
        PA2 -->|新请求| PA3 --> PA4
        PA4 -->|否| PA_WL
        PA4 -->|是| PA5 --> PA6 --> PA7
    end

    PA_WL -->|HTTPResponse err| PAR

    subgraph WaitApplied["waitApplied — 阻塞等待 notifyChans"]
        WA1["ch := make(chan OpReply, 1)"]
        WA2["notifyChans[index] = ch"]
        WA3["select"]
        WA4["<-ch 收到 OpReply"]
        WA5["rf.GetState() 校验 term/leader"]
        WA6["time.After 500ms → ErrTimeOut"]
        WA1 --> WA2 --> WA3
        WA3 --> WA4 --> WA5
        WA3 --> WA6
    end

    PA5 --> WA1

    %% ── Raft 提交与应用 ──
    subgraph RaftPath["Raft 共识 → 状态机应用"]
        RS["rf.Start 追加日志条目"]
        RC["Raft 复制 + 多数派 commit"]
        SA["raft.sendApplyMsg<br/>ApplyMsg → applyCh"]
        AP["kv.applier() 循环读 applyCh"]
        AP1["cmd := msg.Command.(Op)"]
        AP2["applyToStateMachine(cmd)"]
        AP3["putHandler → kvStore[key]=value"]
        AP4["notifyChans[idx] <- reply"]
        RS --> RC --> SA --> AP --> AP1 --> AP2 --> AP3 --> AP4
    end

    PA3 --> RS
    AP4 -->|唤醒| WA4

    %% ── HTTP 响应回传 ──
    subgraph ResponseBack["响应回传路径"]
        HR["httpResponseFromErr → HTTPResponse"]
        ENC["json.Encode → HTTP 200/503"]
        HR2["Controller 收到 Put 成功"]
        WJ["writeJSON {status:'registered', id}"]
        PA7 --> HR --> ENC
        ENC -->|沿调用栈返回| PAR
        R7 --> HR2 --> WJ
    end

    W4 -.->|最终| WJ

    %% ── 数据标注 ──
    classDef data fill:#e8f4fc,stroke:#4a90d9,color:#111
    classDef block fill:#fff3e0,stroke:#f5a623,color:#111

    D1["数据: WorkerInfo JSON"]:::data
    D2["数据: key=worker/127.0.0.1:9100<br/>value=WorkerInfo JSON"]:::data
    D3["数据: HTTPRequest<br/>{op:'put', key, value}"]:::data
    D4["数据: Op 日志命令"]:::data
    D5["数据: kvStore 持久化"]:::data

    W3 -.-> D1
    R3 -.-> D2
    MAR -.-> D3
    PA1 -.-> D4
    AP3 -.-> D5
```

## 第二次写入：workers/index（同链路缩写）

```mermaid
graph LR
    A["addToIndex(id)"] --> B["seen[id]=struct{}{}"]
    B --> C["json.Marshal(ids)"]
    C --> D["kvclient.Put('workers/index', ...)"]
    D --> E["execute → doOnce → POST /kv"]
    E --> F["PutAppend → rf.Start → waitApplied"]
    F --> G["applier → kvStore['workers/index']"]
```

## Leader 轮询重试（kvclient.execute）状态机

```mermaid
graph TD
    START(["execute(req)"]) --> LOOP{"i < len(addrs)?"}
    LOOP -->|是| NEXT["addr = nextAddr()"]
    NEXT --> ONCE["doOnce(addr, req)"]
    ONCE --> NETERR{"网络/解码错误?"}
    NETERR -->|是| INC["i++"] --> LOOP
    NETERR -->|否| CHECK{"resp.Err"}
    CHECK -->|ErrWrongLeader| INC
    CHECK -->|ErrTimeOut| INC
    CHECK -->|OK / 其他可接受| SUCCESS(["return kvResp"])
    LOOP -->|否| FAIL(["error: no available kv leader"])
```

## waitApplied 与 applier 同步闭环

```mermaid
sequenceDiagram
    participant RPC as PutAppend/HTTP goroutine
    participant KV as KVServer
    participant RF as raft.Raft
    participant AP as applier goroutine
    participant CH as notifyChans[index]

    RPC->>KV: rf.Start(Op) → index
    RPC->>KV: waitApplied(index, term)
    KV->>CH: notifyChans[index] = ch (buffer=1)
    Note over RPC: select 阻塞在 <-ch

    RF->>RF: 日志 commit
    RF->>AP: ApplyMsg{CommandValid, CommandIndex=index}
    AP->>KV: putHandler → kvStore[key]=value
    AP->>CH: notifyChan <- OpReply{OK}
    CH-->>RPC: opreply 送达
    RPC->>KV: delete(notifyChans, index)
    RPC-->>RPC: 返回 HTTPResponse / reply
```

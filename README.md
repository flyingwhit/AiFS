# 基于raft实现的分布式ai推理控制面

## 项目介绍
AiFS是以raft协议为基础的分布式大模型(LLM)推理集群控制系统。
整个系统核心采用"数据流和控制流分离"的架构设计

**数据面**：基于 `epoll` 事件驱动的异步网关，负责并发请求的接收、本地路由缓存（Meta Cache）的高效决策，以及向异构（硬件设备，算力大小完全不同的） Worker 推理池进行负载均衡分发。

**控制面（强一致容错）**：由控制器（Controller）通过心跳机制实时监控 Worker 状态，并利用基于 Raft 共识算法的强一致性 KV 存储记录记录集群状态，实现元数据的安全持久化与控制面的高可用容错。


![项目架构图](./images/model.png)

## 项目架构

**Raft KV**
**Gateway**
**Mock Worker**


## 项目流程
用户请求   
  |        
  v     
Gateway 网关：决定请求发给哪个模型服务   
  |    
  v   
Worker：真正处理请求   
  |     
  v     
Raft KV：保存系统状态



## 技术特性

**强一致性存储**
**故障容错**
**负载均衡**


## 项目流程

**phase1**:
实现功能：kvserver提供基本API：Put，Get，Delete。以供controller调用来注册和修改workers状态
~~~go
Put(key, value)
Get(key)
Delete(key)
~~~

逻辑链路：controller注册 -> kvserver记录

具备快照(snapshot)功能

实现细节：   
重定义JSON格式    
将labrpc通信转化为http通信

**phase2**：
逻辑链路 ：员工注册 --> controller记录 --> 存入kvserver   


实现功能：
controller提供提供记录接口

~~~http
POST /workers/register
POST /workers/heartbeat
GET  /workers
~~~

员工定期发送心跳(heartbeat)，如果controller一定时间未收到，则将员工标记为删除状态

**phase3**:

实现功能：worker提供接口，模仿推理

~~~http
POST /v1/chat/completions
~~~

**phase4**

实现功能：gateway提供访问API，用户可以访问gateway -> gateway选择worker -> 调用worker API -> gateway转发结果   


~~~http
POST /v1/chat/completions
~~~

**phase5**:

实现功能：限流和熔断，防止worker被打爆   

一个worker最多同时处理16个请求   
一个worker如果连续失败五次，Gateway暂时先不发送请求给他

**phase6**:

实现功能：加premethus字段， 使状态可观测 

**phase7**：

故障演示





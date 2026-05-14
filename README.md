# 基于raft实现的分布式ai推理控制面



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








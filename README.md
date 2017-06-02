# 

## 版本
v1.0 - 无

## 功能
- 从udp-1接收消息包，并将消息包通过udp-2转发出去。若udp-2故障，则将消息包生产到nsq。
- 优先考虑将消息包通过udp发送出去。

### 编译
go build logserver.go

### 运行
./logserver -config test.cfg
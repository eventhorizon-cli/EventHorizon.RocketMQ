# RocketMQ 本地测试环境

[所有测试环境](../README.zh-CN.md) | [English](README.md)

此目录提供一套用于手动测试客户端的本地 Apache RocketMQ 5.5.0 环境，包含：

- 一个 NameServer，地址为 `localhost:9876`；
- 一个 Broker，地址为 `localhost:10911`；
- 一个 cluster-mode Proxy，提供 `localhost:8080`（Remoting）和 `localhost:8081`（gRPC）。
- 一个 RocketMQ Dashboard，访问地址为 `http://localhost:8082`。

额外的单次运行服务 `volume-init` 会把持久化日志和存储卷的所有权交给镜像中的非 root `rocketmq` 用户。
该服务完成后即退出，NameServer 会在它成功完成后启动。

Broker 与 Proxy 在同一个容器中运行，但它们是两个独立进程。Broker 会先启动并注册到 NameServer。

启动脚本确认注册成功后，才会执行 `mqproxy -pm cluster`。这样既能保留 NameServer 向宿主机客户端公布的
可达 Broker 地址，又能提供 LitePush 所需的 cluster-mode Proxy。

这不同于通过 `mqbroker --enable-proxy` 启动 Broker-integrated Proxy 的方式。

Dashboard 是独立容器，但与 Broker 共用网络命名空间，因此能够使用 NameServer 返回的
`127.0.0.1:10911` Broker 路由。它在容器内监听 `8082`，不会与 Proxy 的 Remoting 端口冲突。

组件职责、两条客户端通信路径和完整拓扑请参阅
[RocketMQ 架构说明](../../docs/zh-CN/architecture/rocketmq-architecture.md)。

运行仓库提供的 LitePush 示例时，优先使用会自动初始化资源的
[`rocketmq-litepush`](../rocketmq-litepush) 环境。它的 Compose 启动过程会自动创建示例所需的 LITE parent Topic 和
Consumer Group。

本环境还包含供普通示例使用的一次性 `resource-init` 服务。Broker 与 Proxy 健康检查通过后，它会以幂等方式创建共享的普通
Topic `eventhorizon-test-topic`。

## 启动与检查

在本目录执行以下命令；若从仓库根目录执行，请额外传入
`-f test-environments/rocketmq/compose.yaml`。

如果本机尚未缓存 RocketMQ Dashboard 镜像，首次运行会额外拉取 `apacherocketmq/rocketmq-dashboard:2.1.0`。

```shell
docker compose config --quiet
docker compose up -d --wait
docker compose ps --all
```

`resource-init` 应显示为 `Exited (0)`。这是一次性初始化完成后的正常状态；`docker compose up -d --wait` 会在
共享的普通示例 Topic 创建成功后才返回。

确认 Broker 已注册到 NameServer：

```shell
docker compose exec broker sh mqadmin clusterList -n nameserver:9876
```

访问 [RocketMQ Dashboard](http://localhost:8082) 可以查看集群、Topic、Consumer Group 和消息。Dashboard 也能
修改 RocketMQ 资源，因此只应将它用作本地开发工具。

## 可选：持久化到宿主机目录

默认情况下，日志和 Broker 数据保存在 Docker 命名卷中。若需要直接检查或备份本目录下的文件，可在启动时叠加
宿主机卷覆盖配置：

```shell
docker compose -f compose.yaml -f compose.host-volumes.yaml up -d --wait
```

后续所有 Compose 命令也必须带上这两个 `-f` 参数。覆盖配置会替换相同容器路径上的默认命名卷挂载，并将文件保存到：

- `./data/nameserver-logs`：NameServer 文件日志；
- `./data/broker-logs`：Broker 与 Proxy 文件日志；
- `./data/broker-store`：Broker 消息数据，包括 CommitLog 和消费队列。

`volume-init` 会创建这些目录，并将其所有权设为镜像中的非 root RocketMQ 用户。`data/` 已被 Git 忽略。
与 Docker 命名卷不同，`docker compose down -v` 不会删除宿主机目录中的数据；如需彻底重置，请显式删除 `./data`。

## 准备手动测试资源

所有普通示例都可以直接使用 `eventhorizon-test-topic`。测试独立场景时，再创建独立的普通 topic 和 consumer group：

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t eventhorizon-test-topic

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-pull

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-push

docker compose exec broker sh mqadmin topicRoute \
  -n nameserver:9876 -t eventhorizon-test-topic
```

如果要在这个通用环境中手动验证 gRPC LitePush，需要创建专用 LITE parent Topic，并为 Consumer Group 配置
bind topic。仓库提供的 LitePush 示例使用 [`rocketmq-litepush`](../rocketmq-litepush) 时不需要这些命令，因为专用环境
会自动创建默认资源：

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t eventhorizon-test-lite-parent-topic -a +message.type=LITE

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g eventhorizon-test-lite-push-consumer \
  --attributes +lite.bind.topic=eventhorizon-test-lite-parent-topic
```

此环境的 Broker 配置已启用 `enableLmq=true` 和 `enableMultiDispatch=true`，Lite 消息必须依赖这两个开关来创建
并写入 LMQ 队列。LitePush 还需要本环境使用的 cluster-mode Proxy；RocketMQ 5.5.0 的 Broker-integrated Proxy
不支持 `SyncLiteSubscription` RPC。

客户端应使用以下端点：

| 客户端路径 | 连接地址 |
| --- | --- |
| classic Remoting Producer/Consumer | NameServer：`localhost:9876` |
| RocketMQ 5 gRPC Producer/Consumer | Proxy：`localhost:8081` |
| RocketMQ Dashboard | 浏览器：`http://localhost:8082` |

当 LITE parent Topic 和 Consumer Group 按上述方式准备后，RocketMQ 5.5.0 Proxy 支持标准 gRPC 发送、SimpleConsumer、PushConsumer
和 LitePush 流程。

## 停止与重置

停止服务但保留 Broker 数据：

```shell
docker compose down
```

删除服务及所有本地 RocketMQ 数据：

```shell
docker compose down -v --remove-orphans
```

公开端口仅绑定在 `127.0.0.1`，只适用于本地开发。Broker 端口必须固定，因为 NameServer 会向宿主机客户端
公布 `127.0.0.1:10911`。

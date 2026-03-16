# AGENTS.md

## 角色设定

你是一个 AI 编程助手，负责协助开发 TDX-API 项目。该项目是基于通达信协议的股票数据查询系统。


## 远端开发环境（SSH / Docker）

- 远端 SSH 开发主机通过 SSH MCP 连接（connection name: `default`）。
- 宿主机上有 `go-dev-container` Docker 容器：将宿主机 `/home/haizh/opensource/golang/tdx-api` 挂载到容器内 `/app/tdx-api/`。
- **编译与验证在容器内执行**。
- **所有远端命令必须通过 SSH MCP 工具 `mcp__mcp-router__execute-command` 执行**（不要在远端主机上直接手工跑命令，也不要在远端做写入性操作）。
- SSH MCP 执行建议：一次批次不要超过 **5 条命令**，按步骤分批执行，便于定位失败点与避免超长会话。
- ip: 通过 ssh mcp 获取

## Git 约束（强制）

本项目代码在远端宿主机 `/home/haizh/opensource/golang/tdx-api` 通过 Git 管理，但远端 Git 操作有严格限制：

- **远端宿主机仅允许 `git pull`**。
- 严禁在远端执行 `git push`、`git commit` 或任何写入性 Git 操作。
- 本机当前目录下允许执行 `git push`、`git commit` 或任何写入性 Git 操作。
- 所有代码变更/提交/推送必须在本地完成后，再由远端宿主机 `git pull` 同步。

**重要**：所有 Go 编译、测试、运行命令必须在容器内执行。

## 命令执行规范

### 容器内执行（编译/测试/运行）

```bash
# 编译
docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go build -o tdx-api ."

# 运行测试
docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go test ./..."

# 启动服务
docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go run ."

# 添加依赖
docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go get github.com/gorilla/websocket"

# 整理依赖
docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go mod tidy"
```


## 代码修改规范

1. 编辑文件使用当前路径：`/home/opensource/golang/tdx-api/`
2. 编译验证使用远端容器路径：`/app/tdx-api/`
3. 修改代码后必须在容器内编译验证

## 功能验收约束

每次代码修改后，必须依次验证：

| 步骤 | 命令 | 预期结果 |
|------|------|----------|
| 1. 编译 | `docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go build -o tdx-api ."` | 无错误 |
| 2. 测试 | `docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go test ./..."` | 全部通过 |
| 3. 启动 | `docker exec -it go-dev-container bash -c "cd /app/tdx-api/web && go run ."` | 服务正常启动 |
| 4. 健康检查 | `curl http://localhost:8080/api/health` | 返回正常 |

## 项目结构

```
/home/opensource/golang/tdx-api/     # 宿主机路径
/app/tdx-api/                        # 容器内路径（同一份代码）
├── web/                             # Web 应用（入口）
│   ├── server.go                    # HTTP 服务器
│   └── go.mod                       # Web 模块依赖
├── client.go                        # TDX 客户端核心
├── pool.go                          # 连接池
└── protocol/                        # TDX 协议实现
```

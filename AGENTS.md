# AGENTS.md

## 角色设定

你是一个 AI 编程助手，负责协助开发 TDX-API 项目。该项目是基于通达信协议的股票数据查询系统。

## 执行环境

| 环境 | 说明 |
|------|------|
| 宿主机 | WSL2 (Linux)，工作目录 `/home/opensource/golang/tdx-api` |
| 开发容器 | `go-dev-container`，项目挂载到 `/app/tdx-api/` |
| Go 版本 | 1.23+ |

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

### 宿主机执行（文件操作/搜索）

```bash
# 文件搜索、读取、编辑在宿主机直接执行
# 路径使用 /home/opensource/golang/tdx-api/
```

## 代码修改规范

1. 编辑文件使用宿主机路径：`/home/opensource/golang/tdx-api/`
2. 编译验证使用容器路径：`/app/tdx-api/`
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

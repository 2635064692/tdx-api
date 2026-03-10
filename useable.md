
# 启动配置
```
export $(cat .env | grep -v '^#' | xargs)

cd web && go run .
```
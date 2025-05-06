# 使用官方的基础镜像
FROM ghcr.io/astral-sh/uv:python3.12-alpine

# 更新 apk 软件包索引并安装必要的依赖
RUN apk update && apk add --no-cache \
    build-base \
    mysql-client \
    mysql-dev \
    mariadb-dev \
    postgresql-dev \
    postgresql-client \
    unixodbc-dev \
    pkgconfig \
    openssl-dev

# 添加项目代码
ADD . /LiteETL
WORKDIR /LiteETL

# 安装依赖
RUN uv sync --locked

# 运行应用
CMD ["uv", "run", "main.py"]
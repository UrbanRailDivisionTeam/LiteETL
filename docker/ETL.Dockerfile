FROM ghcr.io/astral-sh/uv:python3.12-alpine

ADD ../ /LiteETL
WORKDIR /LiteETL
RUN uv sync --locked
CMD ["uv", "run", "main.py"]
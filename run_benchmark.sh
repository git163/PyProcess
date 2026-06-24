#!/usr/bin/env bash
# 运行进程池性能基准测试。

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="${PROJECT_ROOT}/.venv/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "未找到虚拟环境解释器: ${VENV_PYTHON}" >&2
    echo "请先执行: python3 -m venv .venv && pip install -e \".[dev]\"" >&2
    exit 1
fi

RUN_BENCHMARK=1 exec "${VENV_PYTHON}" -m pytest tests/pool/test_pool_benchmark.py -v -s "$@"

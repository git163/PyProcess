# pyprocess

A Python project starter.

## Requirements

- Python ≥ 3.9
- `venv`（Python 3.3+ 内置）
- 推荐：VSCode + 项目推荐扩展，可获得最佳开发体验。

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"
```

这会以可编辑模式安装包，并同时安装开发依赖（pytest、pytest-cov、ruff）。

## Run

```bash
python -m pyprocess
# 或
python -c "import pyprocess; print(pyprocess.__version__)"
```

## Test

```bash
pytest
```

测试文件放在 `tests/<模块名>/test_<...>.py` 下 —— pytest 会通过 `pyproject.toml` 中的 `testpaths` 自动发现。

## Lint / format

```bash
ruff check .        # 静态检查
ruff format .       # 代码格式化
```

## Debug

用 VSCode 打开项目（`code .`）。项目已预置启动配置（`.vscode/launch.json`）：

- **Python: Current File** — 调试当前打开的文件。
- **Python: Module pyprocess** — 以模块方式调试包入口。
- **Python: pytest** — 调试全部测试。
- **Python: pytest current file** — 调试当前打开的测试文件。

`settings.json` 将 VSCode 的 Python 解释器指向 `.venv/bin/python`，因此 IntelliSense、断点和测试资源管理器都会使用正确的环境。默认格式化工具为 Ruff，并开启保存时自动格式化。

如果不使用 VSCode，也可以在命令行附加调试器：

```bash
python -m pdb -m pyprocess
# 或
pytest --pdb                  # 首次失败时进入 pdb
```

## Project layout

- `docs/` — 设计文档和计划（使用 `docs/plan-template.md`）
- `src/pyprocess/` — 包源码
- `tests/` — pytest 测试，按模块组织

## Conventions

详见项目根目录下的 `CLAUDE.md`。

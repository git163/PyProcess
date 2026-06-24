# pyprocess

一个轻量级的 Python 异步进程池库，支持任务提交、结果等待、优雅/强制关闭以及信号触发自动清理子进程。

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

### `pip install -e ".[dev]"` 是什么意思？

这条命令做了两件事，是本地 Python 开发的标准做法。

#### 1. `-e` / `--editable`：可编辑模式安装

普通安装 `pip install .` 会把包复制到虚拟环境的 `site-packages` 目录下。之后你修改 `src/` 里的源码，需要重新安装才能生效。

`-e` 表示**可编辑模式（editable / development mode）**：不会复制包，而是在 `site-packages` 里创建一个指向当前项目 `src/` 目录的链接。这样：

- 你修改 `src/pyprocess/*.py` 后，**无需重新安装**，直接生效
- `import pyprocess` 会使用当前目录下的源码
- 适合开发和调试

简单理解：`-e` 让 Python 直接运行你正在编辑的代码，而不是安装后的副本。

#### 2. `".[dev]`"：安装开发依赖

`pyproject.toml` 中定义了：

```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "ruff>=0.1.0",
]
```

`"."` 表示安装当前项目本身，`"[dev]"` 表示同时安装 `dev` 这组可选依赖。因此 `".[dev]"` 等价于：

- 安装 `pyprocess` 包（可编辑模式）
- 安装 `pytest`、`pytest-cov`、`ruff`

#### 常用变体

```bash
# 只安装包本身，不装开发依赖
pip install -e .

# 可编辑模式 + 开发依赖（推荐日常开发）
pip install -e ".[dev]"

# 普通安装（发布到环境时）
pip install .
```

### 安装后如何验证

```bash
python -c "import pyprocess; print(pyprocess.__version__)"
```

如果输出了版本号，说明安装成功。

### 如果有同名库冲突怎么办？

Python 导入模块时，会按 `sys.path` 列表**从左到右**搜索，第一个匹配的包会被使用。常见冲突场景：

#### 1. 已经用普通方式安装了旧版本

如果你之前执行过 `pip install pyprocess`（非 `-e`），又执行了 `pip install -e ".[dev]"`，通常后者会覆盖前者，优先使用当前目录的可编辑版本。但如果发现导入的不是本地代码，可以：

```bash
# 先卸载所有已安装的 pyprocess
pip uninstall pyprocess -y

# 再重新可编辑安装
pip install -e ".[dev]"
```

#### 2. 同时设置了 PYTHONPATH

如果你手动设置了 `PYTHONPATH=src`，且当前目录下也有 `src/pyprocess/`，Python 会按 `sys.path` 顺序决定用哪个。一般建议：

- 使用虚拟环境（`.venv`），避免全局污染
- 不要混用 `PYTHONPATH` 和 `pip install -e`，二选一即可
- 推荐用 `pip install -e .`，这样 `import pyprocess` 会自动指向当前项目源码

#### 3. 验证当前导入的是哪个路径

```bash
python -c "import pyprocess; print(pyprocess.__file__)"
```

输出应该是当前项目下的路径，例如：

```
/Users/tshua/respo/Code/PyProcess/src/pyprocess/__init__.py
```

如果指向了 `site-packages/` 或其他目录，说明存在优先级问题，需要检查 `sys.path` 或卸载冲突版本。

#### 4. 不同目录有两个同名项目

假设你在 `/project-a/` 和 `/project-b/` 都有 `pyprocess`，且分别 `pip install -e .` 过。由于可编辑安装只是在 `site-packages` 里放了一个指向源码的链接，后安装的会覆盖先安装的链接。因此：**同一虚拟环境里，同一个包名只能有一个有效的可编辑安装**。

需要切换项目时，建议：

```bash
pip uninstall pyprocess -y
cd /另一个项目
pip install -e ".[dev]"
```

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

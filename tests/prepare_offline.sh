#!/usr/bin/env bash
# Prepara a pasta do projeto para rodar os testes num ambiente sem acesso à internet.
#
# Roda num computador com internet. Deixa dentro da pasta do projeto tudo o que os testes precisam:
# o interpretador Python em .python/, as dependências em .venv/ e as extensões do DuckDB em .duckdb/.
# A pasta pode então ser copiada para qualquer caminho do ambiente de destino (mesma plataforma:
# Linux x86_64) e os testes rodam com `.venv/bin/python -m pytest`, sem uv e sem rede.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# O Python gerenciado pelo uv fica dentro do projeto, não em ~/.local/share/uv; o uv não pode usar
# um Python do sistema nem uma .venv anterior, que apontaria para fora da pasta.
export UV_PYTHON_INSTALL_DIR="$root/.python"
export UV_PYTHON_DOWNLOADS=automatic
export UV_MANAGED_PYTHON=1
# Os pacotes são copiados do cache do uv, e não ligados por hardlink a ele.
export UV_LINK_MODE=copy

rm -rf .venv
uv sync --group dev

# O uv cria .venv/bin/python como link absoluto para .python/; um link relativo sobrevive à mudança
# de caminho da pasta (o `home` de pyvenv.cfg fica desatualizado, e o Python não depende dele).
interpreter="$(ls -d .python/cpython-3.13.*-linux-x86_64-gnu | head -n 1)/bin/python3.13"
ln -sfn "../../$interpreter" .venv/bin/python

# Extensões do DuckDB que os testes carregam; `credential_chain` puxa a extensão aws.
.venv/bin/python - <<'PY'
import os
import duckdb

directory = os.path.join(os.getcwd(), ".duckdb")
duckdb.connect(config={"extension_directory": directory}).execute("INSTALL httpfs; INSTALL delta; INSTALL aws")
print("extensões do DuckDB", duckdb.__version__, "em", directory)
PY

.venv/bin/python -c "import deltalake, duckdb, pyarrow, boto3, pytest; print('pronto: deltalake', deltalake.__version__, 'duckdb', duckdb.__version__)"

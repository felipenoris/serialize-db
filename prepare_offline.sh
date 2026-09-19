#!/usr/bin/env bash
# Deixa a pasta do projeto autossuficiente para rodar o pacote e os testes num ambiente sem internet.
#
# Roda num computador com internet, da mesma plataforma do destino (Linux x86_64). Cria na raiz do
# projeto tudo o que o pacote e os testes precisam em execução:
#
#   .python/   interpretador Python gerenciado pelo uv, na versão de .python-version
#   .venv/     o pacote, suas dependências e as de todos os grupos do pyproject.toml (pytest etc.)
#   .duckdb/   extensões do DuckDB que o pacote e os testes carregam
#
# Todos os links dentro dessas pastas são relativos: a pasta inteira pode ser levada num .tar.gz
# (que preserva links e permissões) para qualquer caminho do destino e usada com `.venv/bin/python`,
# sem uv e sem rede. Revise este script quando surgir uma dependência nova fora do pyproject.toml (extensão do
# DuckDB, versão do Python, binário); os pacotes Python entram sozinhos pelo `uv sync`.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$root"

# O Python gerenciado pelo uv fica dentro do projeto, não em ~/.local/share/uv; o uv não pode usar
# um Python do sistema nem uma .venv anterior, que apontaria para fora da pasta.
export UV_PYTHON_INSTALL_DIR="$root/.python"
export UV_PYTHON_DOWNLOADS=automatic
export UV_MANAGED_PYTHON=1
# Os pacotes são copiados do cache do uv, e não ligados por hardlink a ele.
export UV_LINK_MODE=copy

rm -rf .venv
# Resolve e instala o pacote, as dependências de execução e todos os grupos (dev inclusive).
uv sync --all-groups

# O uv cria links absolutos: .venv/bin/python para o interpretador e .python/cpython-3.13-... para a
# pasta da versão completa. Links relativos sobrevivem à mudança de caminho da pasta (o `home` de
# pyvenv.cfg fica desatualizado, e o Python não depende dele quando o link resolve).
version_dir="$(basename "$(ls -d .python/cpython-3.*.*-linux-x86_64-gnu | head -n 1)")"
ln -sfn "../../.python/$version_dir/bin/python3" .venv/bin/python
for link in .python/cpython-*-linux-x86_64-gnu; do
    if [ -L "$link" ]; then
        ln -sfn "$version_dir" "$link"
    fi
done

# Extensões do DuckDB que o pacote e os testes carregam; `credential_chain` puxa a extensão aws.
# Acrescente aqui toda extensão nova que o código passar a usar.
duckdb_extensions="httpfs delta aws"
.venv/bin/python - "$root/.duckdb" $duckdb_extensions <<'PY'
import sys
import duckdb

directory, *extensions = sys.argv[1:]
connection = duckdb.connect(config={"extension_directory": directory})
for extension in extensions:
    connection.execute(f"INSTALL {extension}")
print("extensões do DuckDB", duckdb.__version__, "em", directory + ":", ", ".join(extensions))
PY

.venv/bin/python -c "import serialize_db, deltalake, duckdb, pyarrow, boto3, pytest; print('pronto: python', __import__('sys').version.split()[0], 'deltalake', deltalake.__version__, 'duckdb', duckdb.__version__)"
find .venv .python .duckdb -type l -lname '/*' -print | sed 's/^/link absoluto restante: /'

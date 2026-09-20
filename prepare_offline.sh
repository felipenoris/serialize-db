#!/usr/bin/env bash
# Deixa a pasta do projeto autossuficiente para rodar o pacote e os testes num ambiente sem internet.
#
# Roda num computador com internet, dentro da pasta do projeto:
#
#     ./prepare_offline.sh
#
# A pasta só serve ao destino quando preparada na mesma plataforma (Linux x86_64 para o SageMaker);
# em outra plataforma, fica pronta para uso local. O script recria a `.venv/` a cada execução,
# resolve as dependências na hora (o `uv.lock` não é versionado) e cria na raiz do projeto tudo o
# que o pacote e os testes precisam em execução:
#
#   .python/   interpretador Python gerenciado pelo uv, na versão de .python-version
#   .venv/     o pacote, suas dependências e as de todos os grupos do pyproject.toml (pytest etc.)
#   .duckdb/   extensões do DuckDB que o pacote e os testes carregam
#
# As três pastas estão no `.gitignore`. Rode o script de novo sempre que uma dependência mudar;
# revise-o antes quando a dependência nova estiver fora do pyproject.toml (extensão do DuckDB,
# versão do Python, binário), porque os pacotes Python entram sozinhos pelo `uv sync`.
#
# Variáveis de ambiente
#
# Sem proxy, nenhuma variável é necessária. Atrás de um proxy com autenticação:
#
#   HTTP_PROXY   endereço do proxy, com ou sem as credenciais embutidas:
#                http://usuário:senha@proxy01.exemplo.net:8080 ou http://proxy01.exemplo.net:8080
#   username     usuário do proxy
#   password     senha do proxy, sem URL-encode
#
# O `uv` lê `HTTP_PROXY` sozinho, nas duas grafias e com as credenciais embutidas. O DuckDB lê só a
# grafia maiúscula e recusa o endereço com credenciais ("Failed to parse http_proxy ... into a host
# and port"), por isso o bloco das extensões separa endereço, usuário e senha em três configurações
# da sessão. Sem `username` e `password`, ele usa as credenciais embutidas em `HTTP_PROXY`,
# com URL-decode.
#
# Levar a pasta ao destino
#
# Todos os links dentro das três pastas são relativos: a pasta inteira vai para qualquer caminho do
# destino e roda com `.venv/bin/python`, sem uv e sem rede. O `tar` preserva os links simbólicos, as
# permissões e os demais metadados do sistema de arquivos; copiar arquivo a arquivo por uma pasta
# montada do S3 perde os links, e sem eles `.venv/bin/python` só funciona no caminho de origem.
# Na pasta do projeto:
#
#     tar czf serialize-db.tar.gz --exclude=serialize-db/.git -C .. serialize-db
#
# No destino, em qualquer caminho:
#
#     tar xzf serialize-db.tar.gz
#     cd serialize-db
#     SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente .venv/bin/python -m pytest
#
# Os comandos de `.venv/bin/` (como `pytest`) guardam o caminho original no cabeçalho, por isso a
# chamada é `python -m pytest`; o pacote roda do mesmo jeito, com `.venv/bin/python -m serialize_db`.
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

# O uv cria links absolutos: .venv/bin/python para o interpretador e .python/cpython-3.13-<plataforma>
# para a pasta da versão completa. Links relativos sobrevivem à mudança de caminho da pasta (o `home` de
# pyvenv.cfg fica desatualizado, e o Python não depende dele quando o link resolve). A pasta da versão
# completa é a que não é link, em qualquer plataforma; sem ela o script para, em vez de deixar um
# .venv/bin/python que aponta para o nada.
version_dir=""
for candidate in .python/cpython-3.*.*-*; do
    if [ -d "$candidate" ] && [ ! -L "$candidate" ]; then
        version_dir="$(basename "$candidate")"
        break
    fi
done
if [ -z "$version_dir" ]; then
    echo "prepare_offline.sh: nenhum interpretador em .python/; o uv sync não instalou o Python gerenciado" >&2
    exit 1
fi
ln -sfn "../../.python/$version_dir/bin/python3" .venv/bin/python
for link in .python/cpython-*; do
    if [ -L "$link" ]; then
        ln -sfn "$version_dir" "$link"
    fi
done

# Extensões do DuckDB que o pacote e os testes carregam; `credential_chain` puxa a extensão aws.
# Acrescente aqui toda extensão nova que o código passar a usar.
duckdb_extensions="httpfs delta aws"
.venv/bin/python - "$root/.duckdb" $duckdb_extensions <<'PY'
import os
import sys
import urllib.parse

import duckdb

directory, *extensions = sys.argv[1:]
connection = duckdb.connect(config={"extension_directory": directory})

# O INSTALL baixa a extensão por HTTP. O DuckDB lê HTTP_PROXY só na grafia maiúscula e recusa o
# endereço com as credenciais embutidas, que é como o proxy corporativo costuma aparecer no
# ambiente: o endereço vai sem elas em http_proxy, e o usuário e a senha, sem URL-encode, em
# http_proxy_username e http_proxy_password.
proxy = next(
    (os.environ[name] for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy") if os.environ.get(name)),
    "",
)
if proxy:
    parts = urllib.parse.urlsplit(proxy if "//" in proxy else "//" + proxy, scheme="http")
    if not parts.hostname:
        sys.exit(f"prepare_offline.sh: proxy sem host em {proxy!r}")
    address = parts.hostname + (f":{parts.port}" if parts.port else "")
    user = os.environ.get("username") or urllib.parse.unquote(parts.username or "")
    secret = os.environ.get("password") or urllib.parse.unquote(parts.password or "")
    connection.execute("SET http_proxy = ?", [address])
    if user:
        connection.execute("SET http_proxy_username = ?", [user])
        connection.execute("SET http_proxy_password = ?", [secret])
    print("proxy do DuckDB:", address, "com usuário e senha" if user else "sem credenciais")

for extension in extensions:
    connection.execute(f"INSTALL {extension}")
print("extensões do DuckDB", duckdb.__version__, "em", directory + ":", ", ".join(extensions))
PY

.venv/bin/python -c "import serialize_db, deltalake, duckdb, pyarrow, boto3, pytest; print('pronto: python', __import__('sys').version.split()[0], 'deltalake', deltalake.__version__, 'duckdb', duckdb.__version__)"
find .venv .python .duckdb -type l -lname '/*' -print | sed 's/^/link absoluto restante: /'

"""[public] A raiz do banco nos dois armazenamentos da biblioteca, a pasta local e o S3, pelo
sistema de arquivos do PyArrow.

A raiz do banco é a pasta local ou o prefixo ``s3://`` sob o qual ficam as tabelas Delta de cada
ambiente, em ``<raiz>/<ambiente>/<tabela>``. ``Storage`` guarda três coisas dela: a URI que o
delta-rs e o DuckDB recebem, o sistema de arquivos do ``pyarrow.fs`` que lista, lê, copia e apaga
nos dois armazenamentos, e o caminho da raiz nesse sistema de arquivos. Os métodos recebem
caminhos relativos à raiz, montados com ``/`` por ``join``. ``relative`` faz o caminho inverso:
leva uma URI sob a raiz ao caminho relativo.

Dois grupos de métodos têm um ramo por armazenamento:

- A leitura e a escrita condicional do arquivo de controle, por ``read_text``, ``create_text`` e
  ``write_text``. No S3, elas usam ``get_object`` e ``put_object`` do ``boto3``, este com
  ``IfNoneMatch`` ou ``IfMatch``, porque o ``pyarrow.fs`` não tem a condição nem devolve a etag.
  Na pasta local, ``read_text`` lê o arquivo, ``create_text`` o cria com ``O_EXCL`` e
  ``write_text`` confere a impressão digital dele e o troca por ``os.replace``.
- A cópia, por ``copy``. No S3, ela usa a transferência gerenciada do ``boto3``, porque o
  ``CopyObject`` único do ``copy_file`` do PyArrow é abandonado pelo SDK da AWS depois de 3
  segundos sem resposta num objeto grande.

Os outros métodos ligam a raiz ao delta-rs e ao DuckDB:

- ``storage_options`` monta a cada chamada as opções do delta-rs, sem credencial alguma. A cadeia
  padrão do delta-rs resolve as credenciais e as renova no ``DeltaTable`` que a execução segura.
- ``duckdb_connect`` abre uma conexão do DuckDB com as extensões da pasta configurada.
- ``duckdb_setup`` carrega as extensões e cria o secret do S3 numa conexão já aberta, com a chave
  da credencial do ``boto3`` naquele momento. O motor DuckDB recria o secret quando a chave troca.
- ``prepare_environment`` acerta as variáveis que o delta-rs lê, antes da primeira abertura de
  tabela.

Exemplo, numa pasta local:

.. code-block:: python

    from serialize_db.storage import Storage

    storage = Storage.for_uri("/dados/delta")
    path = storage.join("prd", "_serialize_db", "snapshots.json")
    fingerprint = storage.create_text(path, "{}")
    text, fingerprint = storage.read_text(path)
    storage.write_text(path, '{"snapshots": {}}', if_match=fingerprint)
    storage.relative("/dados/delta/prd/cad_operacoes")   # "prd/cad_operacoes"
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
import re
import stat
import sys
import urllib.parse
import urllib.request
import uuid
from collections.abc import Mapping, MutableMapping
from pathlib import Path

import boto3
import botocore.credentials
import botocore.exceptions
import duckdb
import pyarrow as pa
import pyarrow.fs as pafs

from serialize_db.errors import ConflictError
from serialize_db.schema import literal

__all__ = ["Storage", "prepare_environment"]

# O delta-rs tenta de novo por até 10 s: contra um endereço que não responde, a chamada desistiu em
# 10,3 s, contra 57,0 s no padrão, e contra uma porta fechada em 0,6 s (leitura de 2026-09-23); um
# erro passageiro do S3 ainda ganha até três novas tentativas dentro dos 10 s.
_RETRY_OPTIONS = {"max_retries": "3", "retry_timeout": "10s"}

# As variáveis de criptografia do object_store, o cliente de armazenamento do delta-rs, e a chave
# de storage_options de cada uma. Sem elas, o S3 aplica a criptografia padrão do bucket.
_SSE_VARIABLES = {
    "AWS_SERVER_SIDE_ENCRYPTION": "aws_server_side_encryption",
    "AWS_SSE_KMS_KEY_ID": "aws_sse_kms_key_id",
    "AWS_SSE_BUCKET_KEY_ENABLED": "aws_sse_bucket_key_enabled",
}

# O nome do secret do S3 que duckdb_setup cria e renew_duckdb_secret recria.
_DUCKDB_SECRET = "serialize_db_s3"


def _region() -> str | None:
    """[private] A região da AWS, lida de ``AWS_REGION`` ou, sem ela, de ``AWS_DEFAULT_REGION``;
    ``None`` sem as duas."""
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def _endpoint() -> str | None:
    """[private] O endpoint de ``AWS_ENDPOINT_URL``, o endereço de um serviço compatível com o S3;
    ``None`` sem a variável ou com ela vazia."""
    return os.environ.get("AWS_ENDPOINT_URL") or None


def _extension_directory() -> str | None:
    """[private] A pasta de extensões do DuckDB, para ``duckdb_connect``. É a de
    ``SERIALIZE_DB_DUCKDB_EXTENSIONS`` quando a variável está definida; sem ela, ``.duckdb/`` ao
    lado do ambiente virtual, a pasta que ``prepare_offline.sh`` cria, quando existe; senão
    ``None``, e o DuckDB usa a pasta padrão dele."""
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured
    prepared = Path(sys.prefix).parent / ".duckdb"
    return str(prepared) if prepared.is_dir() else None


def _proxy_settings(
    environ: Mapping[str, str],
) -> dict[str, str]:
    """[private] As configurações de proxy do DuckDB, lidas de ``HTTP_PROXY`` em ``environ``, as
    variáveis de ambiente (``os.environ`` em ``duckdb_setup``); vazias sem a variável ou sem um
    host no endereço.

    O DuckDB recusa um endereço de proxy com as credenciais embutidas, e o erro atinge o acesso ao
    S3. Por isso o endereço vai sem elas em ``http_proxy``, e o usuário e a senha vão em
    ``http_proxy_username`` e ``http_proxy_password``. Os dois vêm das variáveis ``username`` e
    ``password`` e, sem elas, do próprio endereço, como ``probelib.duckdb_proxy`` faz nos probes.
    """
    url = (environ.get("HTTP_PROXY") or "").strip()
    if not url:
        return {}
    # Sem "//", o urlsplit leria o endereço como caminho.
    if "//" not in url:
        url = "//" + url
    parts = urllib.parse.urlsplit(url, scheme="http")
    if not parts.hostname:
        return {}
    address = parts.hostname
    if parts.port:
        address += f":{parts.port}"
    settings = {"http_proxy": address}
    user = environ.get("username") or urllib.parse.unquote(parts.username or "")
    password = environ.get("password") or urllib.parse.unquote(parts.password or "")
    if user:
        settings["http_proxy_username"] = user
        settings["http_proxy_password"] = password
    return settings


def _duckdb_secret_options() -> list[str]:
    """[private] As opções do secret S3 do DuckDB fora da credencial; para
    ``_create_duckdb_secret``.

    A lista traz sempre a região. Com ``AWS_ENDPOINT_URL``, traz também o endereço sem o esquema,
    o endereço por caminho (``URL_STYLE 'path'``) e, num endpoint ``http``, ``USE_SSL false``. O
    DuckDB não lê ``AWS_ENDPOINT_URL``. Sem ``URL_STYLE 'path'`` o bucket vira subdomínio do
    endereço, que num IP não resolve. Sem ``USE_SSL false`` a conexão a um endpoint ``http`` tenta
    TLS e falha (sonda de 2026-09-23 contra o moto). O endereço por caminho é o que o delta-rs e o
    PyArrow usam com um endpoint próprio.
    """
    options = [f"REGION {literal(_region())}"]
    endpoint = _endpoint()
    if not endpoint:
        return options
    parts = urllib.parse.urlsplit(endpoint)
    options.append(f"ENDPOINT {literal(parts.netloc or endpoint)}")
    options.append("URL_STYLE 'path'")
    if parts.scheme == "http":
        options.append("USE_SSL false")
    return options


def _create_duckdb_secret(
    connection: duckdb.DuckDBPyConnection,
    credentials: botocore.credentials.ReadOnlyCredentials,
) -> None:
    """[private] Cria o secret do S3 ``serialize_db_s3``, ou o substitui, com a chave de
    ``credentials`` e as opções de ``_duckdb_secret_options``; para ``duckdb_setup`` e
    ``renew_duckdb_secret``.

    A chave, o segredo e o token vão como parâmetros do comando, fora do texto, porque o erro de
    sintaxe do DuckDB repete o texto do comando."""
    options = ["TYPE s3", "KEY_ID ?", "SECRET ?"]
    parameters = [credentials.access_key, credentials.secret_key]
    if credentials.token:
        options.append("SESSION_TOKEN ?")
        parameters.append(credentials.token)
    options.extend(_duckdb_secret_options())
    connection.execute(
        f"CREATE OR REPLACE SECRET {_DUCKDB_SECRET} ({', '.join(options)})", parameters
    )


def _secret_key_id(
    connection: duckdb.DuckDBPyConnection,
) -> str | None:
    """[private] A chave de acesso que o secret do S3 guarda, para ``renew_duckdb_secret``; ``None``
    sem o secret ou sem ``key_id`` no texto dele.

    A chave é lida do ``secret_string`` de ``duckdb_secrets()``, que no DuckDB 1.5.5 mostra o
    ``key_id`` sem redação."""
    rows = connection.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = ?", [_DUCKDB_SECRET]
    ).fetchall()
    if not rows:
        return None
    found = re.search(r"(?:^|;)key_id=([^;]*)", rows[0][0])
    if found is None:
        return None
    return found.group(1)


def aws_credentials() -> botocore.credentials.Credentials:
    """[protected] A credencial da cadeia padrão do ``boto3``, resolvida numa sessão nova; para o
    motor DuckDB, que a guarda enquanto a execução dura.

    A cadeia padrão é a busca que o ``boto3`` faz por uma credencial no papel, nas variáveis
    ``AWS_*`` e no perfil. A credencial do contêiner é uma ``RefreshableCredentials``: o
    ``get_frozen_credentials`` dela pede outra ao endpoint quando faltam menos de 15 minutos para
    a expiração. A credencial das variáveis ``AWS_*`` sem expiração é fixa.

    :return: a credencial da cadeia.
    :raises botocore.exceptions.NoCredentialsError: a cadeia não achou papel, variáveis ``AWS_*``
        nem perfil.
    """
    credentials = boto3.Session().get_credentials()
    if credentials is None:
        raise botocore.exceptions.NoCredentialsError()
    return credentials


def renew_duckdb_secret(
    connection: duckdb.DuckDBPyConnection,
    credentials: botocore.credentials.Credentials,
) -> bool:
    """[protected] Recria o secret do S3 quando a chave guardada nele não é a que ``credentials`` dá
    agora; para o motor DuckDB, que a chama na entrada de cada sessão.

    O secret guarda a chave do momento em que foi criado, e o ``delta_scan`` não o renova. No
    ambiente alvo, o ``delta_scan`` falhou depois que a chave guardada expirou, com a credencial
    do ``boto3`` já em outra chave (2026-09-25).

    :param connection: a conexão do DuckDB, com o ``httpfs`` carregado; o secret vale para a
        instância do banco, com todos os cursores dela.
    :param credentials: a credencial que o secret acompanha, a de ``aws_credentials``.
    :return: ``True`` quando o secret foi recriado, ``False`` quando a chave não mudou.
    """
    frozen = credentials.get_frozen_credentials()
    if _secret_key_id(connection) == frozen.access_key:
        return False
    _create_duckdb_secret(connection, frozen)
    return True


def _fingerprint(
    content: bytes,
) -> str:
    """[private] A impressão digital de um conteúdo na pasta local: o ``sha256`` dele, que faz o
    papel da etag do S3."""
    return hashlib.sha256(content).hexdigest()


def _write_new_file(
    full: Path,
    content: bytes,
) -> None:
    """[private] Grava ``content`` num arquivo local que ainda não existe; para ``_create_local`` e
    ``_replace_local``.

    A abertura é exclusiva (``O_EXCL``), e o arquivo nasce no modo de um arquivo novo: 0o666 menos
    a umask do processo, sem execução. Um arquivo que já existe levanta ``FileExistsError``, e nada
    é gravado."""
    descriptor = os.open(full, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(content)


@dataclasses.dataclass(frozen=True)
class Storage:
    """[public] A raiz do banco num dos dois armazenamentos, a pasta local ou o S3, com o sistema
    de arquivos que a percorre.

    ``for_uri`` constrói o armazenamento a partir da raiz. Cada método recebe caminhos relativos à
    raiz, como ``prd/cad_operacoes``, e ``uri_of`` devolve a URI completa de um deles.

    Exemplo:

    .. code-block:: python

        storage = Storage.for_uri("s3://bucket/projeto/delta")
        storage.uri                      # "s3://bucket/projeto/delta"
        storage.path                     # "bucket/projeto/delta"
        storage.list_files("prd/cad_operacoes", ".parquet")
    """

    uri: str
    """[public] A raiz como o delta-rs e o DuckDB a recebem, sem barra final:
    ``s3://bucket/prefixo`` no S3, ou o caminho absoluto da pasta local, escrito com ``/`` também
    no Windows (``C:/dados/delta``)."""
    filesystem: pafs.FileSystem
    """[public] O sistema de arquivos do ``pyarrow.fs`` que percorre a raiz: ``S3FileSystem`` no
    S3, ``LocalFileSystem`` na pasta local."""
    path: str
    """[public] A raiz na forma que ``filesystem`` recebe: ``bucket/prefixo`` no S3, sem o
    ``s3://``, ou o caminho absoluto da pasta local."""

    @staticmethod
    def for_uri(
        uri: str,
    ) -> Storage:
        """[public] O armazenamento da raiz, dada como ``s3://bucket/prefixo``, como um caminho
        local ou como uma URI ``file://``.

        No S3, o ``S3FileSystem`` recebe a região de ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``, a
        mesma que ``storage_options`` passa ao delta-rs, e o ``endpoint_override`` de
        ``AWS_ENDPOINT_URL``. Nada toca a rede na construção.

        Exemplo:

        .. code-block:: python

            Storage.for_uri("file:///dados/delta").uri   # "/dados/delta"

        :param uri: a raiz do banco. Um caminho local é resolvido para absoluto, com ``/`` como
            separador também no Windows, onde o PyArrow lista os arquivos com ``/``. O caminho de
            uma URI ``file://`` tem o ``%XX`` decodificado, como em ``Path.from_uri``.
        :return: o armazenamento da raiz.
        :raises ValueError: no S3 sem região, porque o PyArrow a buscaria na rede e o delta-rs
            cairia em ``us-east-1``; e numa URI de outro esquema.
        """
        if uri.startswith("s3://"):
            return _s3_storage(uri)
        if "://" in uri and not uri.startswith("file://"):
            raise ValueError(
                f"{uri}: esquema fora dos armazenamentos da biblioteca; "
                "use uma pasta local, file:// ou s3://"
            )
        return _local_storage(uri)

    @property
    def is_s3(self) -> bool:
        """[public] Se a raiz está no S3 (``uri`` começa por ``s3://``)."""
        return self.uri.startswith("s3://")

    # ------------------------------------------------------------ caminhos

    def join(
        self,
        *parts: str,
    ) -> str:
        """[public] O caminho relativo à raiz, com as partes juntadas por ``/``.

        Exemplo:

        .. code-block:: python

            storage.join("prd/", "cad_operacoes", "_delta_log")   # "prd/cad_operacoes/_delta_log"

        :param parts: os trechos do caminho. As barras nas pontas de cada trecho saem, e um
            trecho vazio é ignorado.
        :return: o caminho, sem barras nas pontas.
        """
        pieces = []
        for part in parts:
            stripped = part.strip("/")
            if stripped:
                pieces.append(stripped)
        return "/".join(pieces)

    def relative(
        self,
        uri: str,
    ) -> str:
        """[public] O caminho relativo à raiz de uma URI que está sob ela, o inverso de ``uri_of``.

        Exemplo:

        .. code-block:: python

            storage.relative(storage.uri + "/prd/cad_operacoes")   # "prd/cad_operacoes"

        :param uri: a URI sob a raiz, com ou sem barra final.
        :return: o caminho relativo; a própria raiz dá ``""``.
        :raises ValueError: a URI fora da raiz, porque as primitivas de ``serialize_db.delta``
            só tocam o que está sob a raiz do banco.
        """
        target = uri.rstrip("/")
        if target == self.uri:
            return ""
        if not target.startswith(self.uri + "/"):
            raise ValueError(f"{uri} fora da raiz {self.uri}")
        return target.removeprefix(self.uri + "/")

    def uri_of(
        self,
        path: str,
    ) -> str:
        """[public] A URI de um caminho relativo à raiz, na forma que o delta-rs e o DuckDB
        recebem.

        Exemplo:

        .. code-block:: python

            storage.uri_of("prd/cad_operacoes")   # "s3://bucket/delta/prd/cad_operacoes"

        :param path: o caminho relativo à raiz; ``""`` é a própria raiz.
        :return: a URI, sem barra final.
        """
        return f"{self.uri}/{path}" if path else self.uri

    def _full(
        self,
        path: str,
    ) -> str:
        """[private] O caminho completo no sistema de arquivos de um caminho relativo à raiz:
        ``path`` seguido dele; ``""`` dá a própria raiz."""
        return f"{self.path}/{path}" if path else self.path

    def _local_parent(
        self,
        path: str,
    ) -> None:
        """[private] Cria a pasta que contém o arquivo ``path``, na pasta local; no S3 não há pasta
        a criar."""
        if not self.is_s3:
            Path(self._full(path)).parent.mkdir(parents=True, exist_ok=True)

    def ensure_folder(
        self,
        path: str,
    ) -> None:
        """[public] Cria uma pasta sob a raiz, na pasta local; no S3 não há pasta a criar.

        O ``COPY`` do DuckDB para um arquivo não cria a pasta dele; ``ensure_folder`` a cria antes.

        Exemplo:

        .. code-block:: python

            storage.ensure_folder("prd/cad_operacoes/data_str=2026-08-31")

        :param path: a pasta, relativa à raiz; as pastas acima dela também são criadas.
        """
        if not self.is_s3:
            Path(self._full(path)).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------ listagem, cópia e exclusão

    def exists(
        self,
        path: str,
    ) -> bool:
        """[public] Se o arquivo ou a pasta existe sob a raiz.

        Exemplo:

        .. code-block:: python

            storage.exists("prd/cad_operacoes/_delta_log")   # True numa tabela criada

        :param path: o arquivo ou a pasta, relativo à raiz.
        :return: ``True`` quando existe.
        """
        info = self.filesystem.get_file_info(self._full(path))
        return info.type != pafs.FileType.NotFound

    def size(
        self,
        path: str,
    ) -> int | None:
        """[public] O tamanho de um arquivo, em bytes.

        Exemplo:

        .. code-block:: python

            storage.size("prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet")
            # 4096

        :param path: o arquivo, relativo à raiz.
        :return: o tamanho em bytes, ou ``None`` quando ele não existe ou não é arquivo.
        """
        info = self.filesystem.get_file_info(self._full(path))
        if info.type != pafs.FileType.File:
            return None
        return info.size

    def list_files(
        self,
        prefix: str,
        suffix: str = "",
    ) -> list[str]:
        """[public] Os arquivos sob ``prefix`` cujo nome termina em ``suffix``.

        A listagem desce as subpastas e deixa de fora a pasta ``_delta_log/``, o log de commits
        da tabela Delta, onde os checkpoints também terminam em ``.parquet``.

        Exemplo:

        .. code-block:: python

            storage.list_files("prd/cad_operacoes", ".parquet")
            # ["prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet", ...]

        :param prefix: a pasta, relativa à raiz; uma pasta que não existe dá a lista vazia.
        :param suffix: o fim do nome, como ``.parquet``; vazio aceita todo arquivo.
        :return: os caminhos relativos à raiz, ordenados pelo caminho.
        """
        selector = pafs.FileSelector(self._full(prefix), recursive=True, allow_not_found=True)
        files = []
        for info in self.filesystem.get_file_info(selector):
            relative = info.path.removeprefix(self.path + "/")
            in_log = "_delta_log" in relative.split("/")
            if info.type == pafs.FileType.File and relative.endswith(suffix) and not in_log:
                files.append(relative)
        return sorted(files)

    def copy(
        self,
        source: str,
        destination: str,
    ) -> None:
        """[public] Copia um arquivo dentro da raiz, sem passar os dados pelo Python.

        No S3, a cópia é a transferência gerenciada do ``boto3``: um ``CopyObject`` até 8 MiB e,
        acima disso, um upload multipart de ``UploadPartCopy`` em partes de 8 MiB, em paralelo e
        com a repetição por parte do botocore. O ``copy_file`` do PyArrow não serve no S3: ele é
        um ``CopyObject`` só, que o SDK da AWS abandona depois de 3 segundos sem byte de resposta
        (``curlCode: 28``). Esse abandono interrompeu a cópia do arquivo de 32.218.190 linhas de
        ``cad_lancamentos`` no ambiente alvo em 2026-09-24. Na pasta local, a cópia é o
        ``copy_file``, com a pasta do destino criada antes.

        Exemplo:

        .. code-block:: python

            storage.copy("prd/cad_operacoes/data_str=2026-08-31/exec-42_ab12.parquet",
                         "prd/arquivo/2026T3/cad_operacoes/data_str=2026-08-31/"
                         "exec-42_ab12.parquet")

        :param source: o arquivo de origem, relativo à raiz.
        :param destination: o caminho do arquivo copiado, relativo à raiz; um arquivo que já
            existe nele é substituído.
        """
        if self.is_s3:
            source_bucket, source_key = self._bucket_and_key(source)
            bucket, key = self._bucket_and_key(destination)
            self._s3_client().copy({"Bucket": source_bucket, "Key": source_key}, bucket, key)
            return
        self._local_parent(destination)
        self.filesystem.copy_file(self._full(source), self._full(destination))

    def delete(
        self,
        paths: list[str],
    ) -> None:
        """[public] Apaga os arquivos listados.

        Exemplo:

        .. code-block:: python

            storage.delete(storage.list_files("prd/staging/exec-42"))

        :param paths: os arquivos, relativos à raiz; um caminho ausente não é erro.
        """
        for path in paths:
            try:
                self.filesystem.delete_file(self._full(path))
            except FileNotFoundError:
                continue

    def open_input_file(
        self,
        path: str,
    ) -> pa.NativeFile:
        """[public] O arquivo aberto para leitura aleatória, a forma que ``pq.ParquetFile`` recebe.

        No S3, o rodapé do Parquet, os metadados no fim do arquivo, é lido por um GET de
        intervalo, sem baixar o arquivo inteiro.

        Exemplo:

        .. code-block:: python

            with storage.open_input_file(path) as source:
                rows = pq.ParquetFile(source).metadata.num_rows

        :param path: o arquivo, relativo à raiz.
        :return: o arquivo aberto do ``pyarrow.fs``, que quem chama fecha.
        """
        return self.filesystem.open_input_file(self._full(path))

    def open_output_stream(
        self,
        path: str,
    ) -> pa.NativeFile:
        """[public] O arquivo aberto para escrita sequencial, a forma que ``pq.ParquetWriter``
        recebe.

        No S3, a escrita é um upload em partes, que só termina no ``close``.

        Exemplo:

        .. code-block:: python

            with storage.open_output_stream("prd/staging/exec-42/lote.parquet") as sink:
                pq.write_table(table, sink)

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta do arquivo é criada.
        :return: o fluxo de saída do ``pyarrow.fs``, que quem chama fecha.
        """
        self._local_parent(path)
        return self.filesystem.open_output_stream(self._full(path))

    # ------------------------------------------------------------ a escrita condicional

    def read_text(
        self,
        path: str,
    ) -> tuple[str, str]:
        """[public] O texto de um arquivo e a impressão digital dele.

        A impressão digital identifica o conteúdo lido: ``write_text`` a recebe em ``if_match``
        para gravar só se o arquivo não mudou desde a leitura.

        Exemplo:

        .. code-block:: python

            text, fingerprint = storage.read_text("prd/_serialize_db/snapshots.json")

        :param path: o arquivo, relativo à raiz, em UTF-8.
        :return: o texto e a impressão digital, a etag no S3 e o ``sha256`` do conteúdo na pasta
            local.
        :raises FileNotFoundError: o arquivo ausente, nos dois armazenamentos.
        """
        if self.is_s3:
            return self._read_s3(path)
        full = Path(self._full(path))
        content = full.read_bytes()
        return content.decode("utf-8"), _fingerprint(content)

    def create_text(
        self,
        path: str,
        text: str,
    ) -> str:
        """[public] Grava o texto num arquivo que ainda não existe.

        No S3, a gravação é um ``put_object`` com ``IfNoneMatch="*"``, atômico no servidor. Na
        pasta local, é a abertura com ``O_EXCL``, no modo de um arquivo novo, 0o666 menos a umask.

        Exemplo:

        .. code-block:: python

            path = "prd/_serialize_db/snapshots.json"
            first = storage.create_text(path, "{}")
            storage.create_text(path, "{}")   # ConflictError: o arquivo já existe

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta dele é criada.
        :param text: o conteúdo, gravado em UTF-8.
        :return: a impressão digital do arquivo gravado.
        :raises serialize_db.errors.ConflictError: o arquivo já existe, ou, no S3, o 409 de outra
            operação no objeto durante a gravação; nada foi gravado.
        """
        if self.is_s3:
            return self._put_s3(path, text, {"IfNoneMatch": "*"})
        return self._create_local(path, text)

    def write_text(
        self,
        path: str,
        text: str,
        if_match: str | None = None,
    ) -> str:
        """[public] Grava o texto por inteiro, no lugar do arquivo, com a condição opcional de ele
        não ter mudado desde a leitura.

        No S3, a gravação é um ``put_object``, com ``IfMatch=<etag>`` quando a condição vem,
        atômico no servidor. Na pasta local, a impressão digital do arquivo é comparada com a
        condição, e um arquivo temporário entra no lugar dele por ``os.replace``. Essa sequência
        não é atômica entre processos, o que basta à pasta local, o ambiente dos testes e do
        desenvolvimento. O arquivo substituído mantém o modo, e o arquivo novo nasce como em
        ``create_text``. A escrita ou a troca que falha não deixa o temporário na pasta.

        Exemplo:

        .. code-block:: python

            text, fingerprint = storage.read_text(path)
            storage.write_text(path, '{"snapshots": {}}', if_match=fingerprint)

        :param path: o arquivo, relativo à raiz; na pasta local, a pasta dele é criada.
        :param text: o conteúdo, gravado em UTF-8.
        :param if_match: a impressão digital que ``read_text`` devolveu; a gravação só acontece
            se a impressão digital atual do arquivo é essa. ``None`` grava sem condição.
        :return: a impressão digital nova.
        :raises serialize_db.errors.ConflictError: a condição falhou, ou, no S3, o 409 de outra
            operação no objeto durante a gravação; nada foi gravado.
        """
        if not self.is_s3:
            return self._replace_local(path, text, if_match)
        condition = {}
        if if_match is not None:
            condition["IfMatch"] = if_match
        return self._put_s3(path, text, condition)

    def _create_local(
        self,
        path: str,
        text: str,
    ) -> str:
        """[private] O ramo de ``create_text`` na pasta local: cria a pasta do arquivo, grava pela
        abertura exclusiva ``O_EXCL`` e devolve a impressão digital; o arquivo que já existe é
        ``ConflictError``."""
        full = Path(self._full(path))
        full.parent.mkdir(parents=True, exist_ok=True)
        content = text.encode("utf-8")
        try:
            _write_new_file(full, content)
        except FileExistsError:
            raise ConflictError(f"{path} já existe") from None
        return _fingerprint(content)

    def _replace_local(
        self,
        path: str,
        text: str,
        if_match: str | None,
    ) -> str:
        """[private] O ramo de ``write_text`` na pasta local: confere a impressão digital quando
        ``if_match`` vem, grava um temporário ao lado do arquivo e o põe no lugar por
        ``os.replace``; a impressão digital que não confere é ``ConflictError``."""
        full = Path(self._full(path))
        full.parent.mkdir(parents=True, exist_ok=True)
        if if_match is not None:
            current = _fingerprint(full.read_bytes()) if full.exists() else None
            if current != if_match:
                raise ConflictError(f"{path} mudou desde a leitura")
        content = text.encode("utf-8")
        # O arquivo temporário na mesma pasta, que os.replace troca de uma vez, nasce como um
        # arquivo novo e recebe o modo do arquivo que substitui, quando ele existe.
        temporary = full.with_name(f"{full.name}.{uuid.uuid4().hex}.tmp")
        # A escrita ou a troca que falha não deixa o temporário na pasta, onde list_files o
        # listaria; o nome com o uuid é só deste escritor, e a abertura exclusiva não o encontra.
        try:
            _write_new_file(temporary, content)
            if full.exists():
                os.chmod(temporary, stat.S_IMODE(full.stat().st_mode))
            os.replace(temporary, full)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return _fingerprint(content)

    def _bucket_and_key(
        self,
        path: str,
    ) -> tuple[str, str]:
        """[private] O bucket e a chave de um objeto do S3, a partir de um caminho relativo à
        raiz."""
        bucket, _, key = self._full(path).partition("/")
        return bucket, key

    def _s3_client(self) -> object:
        """[private] O cliente S3 do ``boto3``, criado a cada chamada com a região e o endpoint das
        variáveis. A região vai explícita porque o botocore não lê ``AWS_REGION`` e, sem região,
        iria ao endpoint global."""
        return boto3.client("s3", region_name=_region(), endpoint_url=_endpoint())

    def _read_s3(
        self,
        path: str,
    ) -> tuple[str, str]:
        """[private] O ramo de ``read_text`` no S3: o texto e a etag do objeto, por ``get_object``;
        o objeto ausente (``NoSuchKey`` ou 404) é ``FileNotFoundError``."""
        bucket, key = self._bucket_and_key(path)
        try:
            response = self._s3_client().get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(self.uri_of(path)) from None
            raise
        return response["Body"].read().decode("utf-8"), response["ETag"]

    def _put_s3(
        self,
        path: str,
        text: str,
        condition: Mapping[str, str],
    ) -> str:
        """[private] O ramo de ``create_text`` e de ``write_text`` no S3: o ``put_object`` com a
        condição, ``IfNoneMatch`` ou ``IfMatch``, que devolve a etag nova. O 412, a condição que
        falhou, e o 409, outra operação no objeto durante a gravação, viram ``ConflictError``."""
        bucket, key = self._bucket_and_key(path)
        try:
            response = self._s3_client().put_object(
                Bucket=bucket, Key=key, Body=text.encode("utf-8"), **condition
            )
        except botocore.exceptions.ClientError as error:
            # O 412 é a condição que falhou; o 409, outra operação no objeto durante a gravação,
            # depois da qual o modelo do S3 no botocore manda reler a etag e repetir.
            refusals = ("PreconditionFailed", "412", "ConditionalRequestConflict", "409")
            if error.response["Error"]["Code"] in refusals:
                raise ConflictError(f"{self.uri_of(path)}: {error}") from None
            raise
        return response["ETag"]

    # ------------------------------------------------------------ delta-rs e DuckDB

    def storage_options(self) -> dict[str, str]:
        """[public] As opções que o delta-rs recebe para abrir uma tabela da raiz, montadas a cada
        chamada.

        As opções não levam credencial. A cadeia padrão do delta-rs resolve as credenciais e as
        renova no ``DeltaTable`` que a execução segura. Um trio congelado, chave, segredo e token,
        expiraria em cerca de uma hora e circularia num dicionário que um log imprime.

        Exemplo:

        .. code-block:: python

            Storage.for_uri("s3://bucket/delta").storage_options()
            # {"AWS_REGION": "us-east-1", "max_retries": "3", "retry_timeout": "10s"}

        :return: vazias na pasta local. No S3, ``AWS_REGION``, ``AWS_ENDPOINT_URL`` quando
            presente, ``max_retries`` e ``retry_timeout``, para uma rede morta falhar em segundos,
            e as chaves de SSE, a criptografia no servidor, das variáveis do object_store quando
            configuradas.
        """
        if not self.is_s3:
            return {}
        options = {"AWS_REGION": _region()}
        endpoint = _endpoint()
        if endpoint:
            options["AWS_ENDPOINT_URL"] = endpoint
        options.update(_RETRY_OPTIONS)
        for variable, key in _SSE_VARIABLES.items():
            if os.environ.get(variable):
                options[key] = os.environ[variable]
        return options

    def duckdb_setup(
        self,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        """[public] Prepara uma conexão aberta do DuckDB para ler a raiz: carrega as extensões que
        ela pede e, no S3, cria o secret com a chave da credencial do ``boto3``.

        Na pasta local, o preparo é só ``LOAD delta``. No S3, ele faz, nesta ordem:

        - ``LOAD httpfs`` e ``LOAD delta``;
        - o proxy de ``HTTP_PROXY`` nas configurações da conexão, sem as credenciais no endereço;
        - o secret ``serialize_db_s3`` com a chave, o segredo e o token que a cadeia do ``boto3``
          resolve agora, mais a região e o endpoint.

        O secret guarda essa chave até ser recriado. Uma conexão que dura mais que a chave precisa
        recriá-lo, como o motor DuckDB faz. As extensões vêm da pasta configurada na conexão; nada
        é baixado.

        Exemplo:

        .. code-block:: python

            connection = duckdb.connect(config={"extension_directory": ".duckdb"})
            storage.duckdb_setup(connection)
            connection.execute(f"SELECT count(*) FROM delta_scan('{uri}')")

        :param connection: a conexão aberta do DuckDB que recebe as extensões e o secret.
        :raises duckdb.Error: uma extensão ausente da pasta configurada na conexão, ou o secret
            recusado.
        :raises botocore.exceptions.NoCredentialsError: no S3, a cadeia do ``boto3`` não achou
            credencial.
        """
        if not self.is_s3:
            connection.execute("LOAD delta")
            return
        for extension in ("httpfs", "delta"):
            connection.execute(f"LOAD {extension}")
        for name, value in _proxy_settings(os.environ).items():
            connection.execute(f"SET {name} = {literal(value)}")
        _create_duckdb_secret(connection, aws_credentials().get_frozen_credentials())

    def duckdb_connect(
        self,
        database: str = ":memory:",
        config: Mapping[str, object] | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """[public] Abre uma conexão do DuckDB pronta para ler a raiz: as extensões vêm da pasta
        configurada, sem instalação automática, e ``duckdb_setup`` já foi aplicado.

        A pasta das extensões é a de ``SERIALIZE_DB_DUCKDB_EXTENSIONS``; sem a variável,
        ``.duckdb/`` ao lado do ambiente virtual, quando existe; senão, a padrão do DuckDB. A
        instalação e a carga automáticas de extensões ficam desligadas. Com elas ligadas, o
        ``LOAD`` de uma extensão conhecida a baixaria para ``~/.duckdb`` sem aviso, e o ambiente
        alvo não tem internet.

        Exemplo:

        .. code-block:: python

            connection = storage.duckdb_connect()
            uri = storage.uri_of("prd/cad_operacoes")
            connection.execute(f"SELECT count(*) FROM delta_scan('{uri}')")

        :param database: o arquivo do banco do DuckDB; o padrão, um banco em memória, é
            ``:memory:``.
        :param config: as opções da abertura (``threads``, ``temp_directory``,
            ``memory_limit``), acrescentadas às da biblioteca.
        :return: a conexão aberta, que quem chama fecha.
        :raises duckdb.Error: uma extensão ausente da pasta configurada ou o secret recusado; a
            conexão é fechada antes de a exceção subir.
        :raises botocore.exceptions.NoCredentialsError: no S3, a cadeia do ``boto3`` não achou
            credencial; a conexão é fechada antes de a exceção subir.
        """
        settings: dict[str, object] = {
            "autoinstall_known_extensions": False,
            "autoload_known_extensions": False,
        }
        directory = _extension_directory()
        if directory:
            settings["extension_directory"] = directory
        settings.update(config or {})
        connection = duckdb.connect(database, config=settings)
        try:
            self.duckdb_setup(connection)
        except (duckdb.Error, botocore.exceptions.BotoCoreError):
            connection.close()
            raise
        return connection


def _s3_storage(
    uri: str,
) -> Storage:
    """[private] O ramo de ``for_uri`` no S3: o ``S3FileSystem`` com a região e o endpoint das
    variáveis; sem região, ``ValueError``."""
    region = _region()
    if not region:
        raise ValueError(f"{uri}: o S3 precisa da região em AWS_REGION ou AWS_DEFAULT_REGION")
    path = uri.removeprefix("s3://").strip("/")
    options: dict[str, object] = {"region": region}
    endpoint = _endpoint()
    if endpoint:
        options["endpoint_override"] = endpoint
    return Storage(f"s3://{path}", pafs.S3FileSystem(**options), path)


def _local_storage(
    uri: str,
) -> Storage:
    """[private] O ramo de ``for_uri`` numa pasta local: o ``LocalFileSystem`` sobre o caminho
    absoluto, resolvido e escrito com ``/``. O caminho de uma URI ``file://`` sai de
    ``_path_from_file_uri``, que decodifica o ``%XX``."""
    local_path = _path_from_file_uri(uri) if uri.startswith("file://") else Path(uri)
    # O as_posix troca o \ do Windows por /, o separador dos caminhos que o PyArrow lista e que
    # join e relative montam; no Linux e no macOS, o texto não muda.
    resolved = local_path.expanduser().resolve().as_posix()
    return Storage(resolved, pafs.LocalFileSystem(), resolved)


def _path_from_file_uri(
    uri: str,
) -> Path:
    """[private] O caminho local de uma URI ``file://``, com o ``%XX`` decodificado. No Python 3.13
    em diante, ``Path.from_uri`` faz a conversão; no 3.12, que não o tem, o ``url2pathname`` do
    caminho da URI a faz, lendo no Windows a unidade de ``file:///D:/dados``."""
    if sys.version_info >= (3, 13):
        return Path.from_uri(uri)
    return Path(urllib.request.url2pathname(urllib.parse.urlsplit(uri).path))


def prepare_environment(
    environ: MutableMapping[str, str] = os.environ,
) -> dict[str, str]:
    """[public] Acerta as variáveis de ambiente que o delta-rs lê, antes da primeira abertura de
    tabela.

    Os acertos:

    - Exporta ``NO_PROXY`` a partir de ``no_proxy`` quando a maiúscula está ausente ou vazia. O
      cliente HTTP do delta-rs lê ``NO_PROXY`` antes de ``no_proxy``, e uma ``NO_PROXY`` vazia
      anula as exceções do proxy e manda ao proxy a chamada ao endpoint de credenciais do
      contêiner.
    - Copia a região entre ``AWS_REGION`` e ``AWS_DEFAULT_REGION``, nos dois sentidos, porque o
      botocore lê só ``AWS_DEFAULT_REGION`` e o delta-rs lê as duas.

    Uma segunda chamada não muda nada. ``serialize_db.execution.Database`` a chama na construção.

    Exemplo:

    .. code-block:: python

        prepare_environment()   # {"NO_PROXY": "169.254.170.2,localhost"} na primeira chamada

    :param environ: as variáveis, alteradas no lugar; o padrão é ``os.environ``, o que o
        delta-rs lê.
    :return: as variáveis que mudaram, com o valor novo, para o log.
    """
    changes: dict[str, str] = {}
    # Vazia conta como ausente: get devolve "" e a condição a substitui.
    if not environ.get("NO_PROXY") and environ.get("no_proxy"):
        environ["NO_PROXY"] = environ["no_proxy"]
        changes["NO_PROXY"] = environ["no_proxy"]
    region = environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION")
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        if region and environ.get(name) != region:
            environ[name] = region
            changes[name] = region
    return changes

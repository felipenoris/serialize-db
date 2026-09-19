# Esquema a partir dos modelos ORM

Gerar o DDL a partir dos modelos. O DDL gerado cobre as tabelas do sandbox no DuckDB e no
Redshift. As vantagens do DDL manual, revisão explícita e opções físicas, vêm de
dois mecanismos:

- Opções físicas no próprio modelo, num espaço de nomes da biblioteca em `Table.info`. A chave de
  ordenação vira `SORTKEY` no Redshift e `ORDER BY` na gravação dos arquivos.
- Arquivos `.sql` com o DDL gerado por backend, versionados no
  repositório do pipeline e comparados por um teste. Uma mudança no modelo aparece no diff.

```python
class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = {
        "info": {
            "serialize_db": {
                "particionamento": {"coluna": "data_ref", "transformacao": "month"},
                "chave_ordenacao": ["data_ref", "id_operacao"],
                "redshift": {"diststyle": "KEY", "distkey": "id_cliente"},
            }
        }
    }

    id_operacao: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    data_ref: Mapped[date] = mapped_column(primary_key=True)
    id_cliente: Mapped[int] = mapped_column(BigInteger)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(String(200))
```

O `sqlalchemy-redshift` aceita `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey` e
`redshift_interleaved_sortkey` como argumentos de `Table`. Guardar as opções em `info` mantém os
modelos neutros quando o dialeto do Redshift não está instalado.

## Política de restrições

| Restrição               | Sandbox DuckDB | Sandbox Redshift |
| ----------------------- | -------------- | ---------------- |
| `NOT NULL`              | Declarada. | Declarada e aplicada pelo banco. |
| `PRIMARY KEY`, `UNIQUE` | Omitida; a auditoria verifica os meses novos. | Declarada quando auditada; informativa.|
| `FOREIGN KEY` | Omitida; auditoria opcional. | Declarada quando auditada; informativa. |

Unicidade, chave primária e chave estrangeira são informativas no Redshift. O planejador usa essas
chaves para decorrelacionar subconsultas, ordenar e eliminar joins, e supõe que elas são válidas. Com
chaves inválidas, consultas retornam resultados errados; a documentação cita um `SELECT DISTINCT` que
devolve duplicatas. `NOT NULL` é aplicado.

A medição publicada na documentação do DuckDB, com 554 milhões de linhas, justifica omitir chaves no
DuckDB:

| Operação | Tempo |
| --- | --- |
| Carga com chave primária | 461,6 s |
| Carga sem chave primária | 121,0 s |
| Criação da chave primária após a carga | 242,0 s |

A documentação recomenda não declarar restrições no DuckDB, exceto para garantir integridade. Índices
ART precisam caber em memória durante a criação.

## Tipos no contrato

| SQLAlchemy | Arrow | Delta | DuckDB | Redshift | Observação |
| --- | --- | --- | --- | --- | --- |
| `SmallInteger` | `int16` | `short` | `SMALLINT` | `SMALLINT` | O Parquet grava `int16` no tipo físico `INT32`. |
| `Integer` | `int32` | `integer` | `INTEGER` | `INTEGER` | |
| `BigInteger` | `int64` | `long` | `BIGINT` | `BIGINT` | Tipos sem sinal do Arrow e do DuckDB ficam fora do contrato. |
| `Boolean` | `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Double` | `float64` | `double` | `DOUBLE` | `DOUBLE PRECISION` | Descarregar e recarregar pelo Redshift pode perder precisão. |
| `Numeric(p, s)` | `decimal128(p, s)` | `decimal(p, s)` | `DECIMAL(p, s)` | `DECIMAL(p, s)` | `p` até 38. O delta-rs e o DuckDB gravam `DECIMAL(18, 2)` no tipo físico `INT64`; o PyArrow, em `FIXED_LEN_BYTE_ARRAY`. |
| `String(n)` | `string` | `string` | `VARCHAR` | `VARCHAR(n)` | `n` em bytes no Redshift; auditoria de tamanho. |
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | `TEXT` no Redshift vira `VARCHAR(256)`. |
| `Date` | `date32` | `date` | `DATE` | `DATE` | |
| `DateTime` | `timestamp[us]` | `timestamp_ntz` | `TIMESTAMP` | `TIMESTAMP` | Cast explícito de nanossegundos (padrão do pandas) para microssegundos; o delta-rs aceita nanossegundos e grava microssegundos em silêncio. `timestamp_ntz` exige o recurso `timestampNtz` (leitor 3, escritor 7), que o delta-rs habilita ao gravar e o DuckDB lê como `TIMESTAMP`. |
| `DateTime(timezone=True)` | `timestamp[us, tz=UTC]` | `timestamp` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Gravar sempre em UTC; o `timestamp` do Delta é ajustado a UTC, e um fuso diferente entra como o mesmo instante. O `UNLOAD` descarta o fuso. |
| `Uuid` | `string` | `string` | `VARCHAR` | `VARCHAR(36)` | O Redshift não tem tipo UUID. |
| `JSON().with_variant(SUPER(), "redshift")` | `string` (ou a extensão `arrow.json`) | `string` | `JSON` | `SUPER` | Texto JSON é a forma de troca; a validação é do DuckDB na carga e do `JSON_PARSE` no Redshift. Detalhes na seção seguinte. |
| `LargeBinary`, `ARRAY`, `Interval` | | | | | Fora do contrato até haver um caso de uso. |

### Campos JSON

Um campo JSON guarda um documento sem esquema fixo; chaves com esquema fixo viram colunas. O
tratamento em cada camada, verificado em 2026-09-19:

| Camada | Tipo | Comportamento |
| --- | --- | --- |
| Modelo | `sa.JSON().with_variant(SUPER(), "redshift")`, com `SUPER` de `sqlalchemy_redshift.dialect`. | O DDL compila `JSON` no DuckDB e `SUPER` no Redshift; `isinstance(tipo, sa.JSON)` continua verdadeiro, e `tipo_arrow` o reconhece. |
| Arrow | `pa.json_(pa.string())`, extensão `arrow.json`, ou `pa.string()`. | Um `dict` do pandas vira `struct`; a biblioteca serializa com `json.dumps` antes do cast. O Arrow não valida o texto. |
| Delta | `string`, com `ARROW:extension:name = arrow.json` nos metadados do campo quando o esquema Arrow traz a extensão. | O delta-rs grava o arquivo com o tipo lógico `String`; `schema().to_arrow()` devolve `string` simples. |
| Parquet | `BYTE_ARRAY` com tipo lógico `JSON` quando gravado pelo PyArrow ou pelo DuckDB, `String` quando gravado pelo delta-rs. | Os dois entram na mesma tabela Delta e são lidos pelos dois leitores. |
| DuckDB | `JSON` nas tabelas do sandbox; `VARCHAR` no `delta_scan`. | `::JSON` valida na materialização (`Malformed JSON` para texto inválido); `->>`, `json_extract` e `json_valid` funcionam sobre `VARCHAR`; a saída em Arrow volta como `string`. |
| Redshift | `SUPER`. | A staging recebe `VARCHAR(65535)` e o `INSERT ... SELECT` aplica `JSON_PARSE`; o `UNLOAD` devolve texto com `JSON_SERIALIZE`. Documentos acima de 65.535 bytes dependem do `COPY` direto em `SUPER`, pendente da prova de conceito. |

A auditoria confere `json_valid` no DuckDB antes de publicar, porque nem o Arrow nem o Delta validam
o texto.

## Portabilidade de SQL entre DuckDB e Redshift

- As consultas usam construções do SQLAlchemy, não SQL em texto.
- Funções com nomes ou semânticas diferentes nos dois bancos ganham uma regra `@compiles` por dialeto.
  A lista sai do código atual do pipeline.
- O DuckDB aceita construções do PostgreSQL ausentes no Redshift, como arrays e `ON CONFLICT`. Uma
  consulta que roda no DuckDB pode falhar no Redshift, então as consultas do pipeline precisam de
  testes de integração no Redshift com uma amostra pequena.

# Guia geral de implementação

## Boas práticas de ETL aplicáveis

### Partições imutáveis e substituição idempotente

O mês (`YYYY-MM`) é a unidade de escrita das tabelas particionadas. Uma reexecução ou correção grava o
mês inteiro de novo e substitui o anterior. Reexecuções ficam idempotentes, e a exportação incremental
se reduz a publicar os meses gravados.

### Write-Audit-Publish

Os dados novos são gravados numa área invisível aos leitores, auditados (contagem de linhas, nulos,
unicidade de chaves, limites de tipo) e só então publicados. Uma auditoria reprovada interrompe a
execução sem alterar as tabelas permanentes. O [sandbox da execução](#sandbox-da-execução) é essa área.

### Contrato de esquema

O esquema vive num único lugar, os modelos ORM, e dele derivam o DDL, o esquema Arrow, o esquema
Iceberg e as auditorias. O [etl-cookbook-tutorial](https://github.com/felipenoris/etl-cookbook-tutorial)
chama esse uso de "modelos como contrato".

### Commit atômico nos metadados

O S3 não renomeia diretórios de forma atômica, e listar um prefixo mistura arquivos antigos, novos e
parciais. A escrita condicional do S3 (`If-None-Match`, `If-Match`) e o log do Delta Lake, que a usa,
respondem a pergunta em [estrategia.md](estrategia.md).


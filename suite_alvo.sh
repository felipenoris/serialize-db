#!/usr/bin/env bash
# A bateria do ambiente alvo num comando só: os probes e as suítes, a migração, a publicação, a
# credencial expirando, o acesso de leitura, a exportação e o compact, a sonda da base publicada,
# as sondas de consistência e as da operação, na ordem em que estão abaixo. As variáveis que os
# passos leem estão em SUITE_ALVO.md, seção "Lista de variáveis de ambiente", que também diz
# como empacotar os resultados.
#
# Uso, com as variáveis exportadas:
#
#     cd ~/work/projects/serialize-db
#     ./suite_alvo.sh
#
# O script entra na pasta do repositório e para antes do primeiro passo quando alguma variável
# falta. O log abre com a hora, o commit da cópia do repositório (hash, ramo, data e título) e o
# que a cópia tem além dele, e o valor de cada variável; sem git ou sem a pasta .git, diz isso e
# segue. Cada passo é ecoado com a hora antes de rodar e com o código de saída e a duração
# depois; um passo que falha não interrompe os seguintes, e Ctrl-C vai ao passo em curso e
# encerra o script depois dele. Tudo o que os passos mandam ao terminal, a saída de erro
# incluída, vai também para probes/output/suite_alvo_<data-hora>.txt, ao lado dos relatórios
# que as sondas gravam, e o arquivo termina com a tabela dos passos e o código de saída de cada
# um. Código de saída do script: 0 quando todo passo saiu com 0, 1 quando algum não, 2 sem
# alguma variável, 130 quando interrompido.

cd "$(dirname "$0")" || exit 1

# As variáveis que os passos leem, exportadas antes de rodar o script.
VARIABLES=(
    SERIALIZE_DB_TEST_LOCAL_ROOT
    SERIALIZE_DB_TEST_S3_ROOT
    SERIALIZE_DB_REDSHIFT_WORKGROUP
    SERIALIZE_DB_REDSHIFT_DATABASE
    SERIALIZE_DB_REDSHIFT_SHARE_DATABASE
    SERIALIZE_DB_REDSHIFT_SCHEMA
    SERIALIZE_DB_TEST_REDSHIFT_SCHEMA
    AWS_DEFAULT_REGION
    SOURCE_PATH
    TARGET_ROOT_PATH
)

# Sem uma variável, nada roda: cada passo a leria vazia.
missing=()
for name in "${VARIABLES[@]}"; do
    if [ -z "${!name}" ]; then
        missing+=("$name")
    fi
done
if [ "${#missing[@]}" -ne 0 ]; then
    echo "suite_alvo.sh: variáveis de ambiente sem valor: ${missing[*]}" >&2
    exit 2
fi

# O que cada passo deixou, para a tabela do fim: o comando, o código de saída e a duração.
STEP_COMMANDS=()
STEP_STATUSES=()
STEP_SECONDS=()

section() {
    # O título de um bloco da bateria, destacado no log.
    echo
    echo "################################################################################"
    echo "# $*"
    echo "################################################################################"
}

run() {
    # Roda um passo: ecoa o comando com a hora, executa e registra o código de saída e a
    # duração. O passo que falha não interrompe os seguintes. O passo entra na tabela antes de
    # rodar, com o código e a duração em branco, para constar nela se o Ctrl-C chegar durante ele.
    local command
    command=$(printf '%q ' "$@")
    command=${command% }
    STEP_COMMANDS+=("$command")
    STEP_STATUSES+=("-")
    STEP_SECONDS+=("-")
    STEP_STARTED=$SECONDS
    echo
    echo "==> passo ${#STEP_COMMANDS[@]}, $(date '+%Y-%m-%d %H:%M:%S %z'): $command"
    "$@"
    finish_step $?
}

finish_step() {
    # Registra o código de saída e a duração do passo em curso, o último da tabela.
    local status=$1
    local last=$(( ${#STEP_COMMANDS[@]} - 1 ))
    STEP_STATUSES[last]=$status
    STEP_SECONDS[last]=$(( SECONDS - STEP_STARTED ))
    echo "<== passo $(( last + 1 )): código de saída $status, ${STEP_SECONDS[last]} s"
}

summary() {
    # A tabela dos passos com o código de saída e a duração de cada um. Devolve 1 quando algum
    # passo saiu com código diferente de 0.
    section "Resumo dos passos"
    local failed=0
    echo "PASSO  CÓDIGO  SEGUNDOS  COMANDO"
    for index in "${!STEP_COMMANDS[@]}"; do
        printf '%5d  %6s  %8s  %s\n' \
            "$(( index + 1 ))" "${STEP_STATUSES[index]}" "${STEP_SECONDS[index]}" \
            "${STEP_COMMANDS[index]}"
        if [ "${STEP_STATUSES[index]}" != 0 ]; then
            failed=$(( failed + 1 ))
        fi
    done
    echo
    echo "${#STEP_COMMANDS[@]} passo(s), $failed com código de saída diferente de 0; log em $LOG"
    [ "$failed" -eq 0 ]
}

# shellcheck disable=SC2329
interrupt() {
    # Ctrl-C: o passo em curso recebeu o sinal junto, e $? é o código com que ele terminou; a
    # tabela fecha o log e o script sai com 130.
    local status=$?
    echo
    echo "suite_alvo.sh: interrompido por Ctrl-C"
    if [ "${STEP_STATUSES[-1]}" = "-" ]; then
        finish_step "$status"
    fi
    summary
    exit 130
}

repository_lines() {
    # O commit em que a cópia do repositório está e o que ela tem além dele, para o log dizer a
    # versão que rodou; sem git ou sem a pasta .git, a linha diz isso e a bateria segue.
    local commit
    if ! command -v git >/dev/null; then
        echo "commit: desconhecido, git não encontrado no PATH"
        return
    fi
    if ! commit=$(git rev-parse HEAD 2>/dev/null); then
        echo "commit: desconhecido, a pasta não é uma cópia git"
        return
    fi
    local branch
    branch=$(git rev-parse --abbrev-ref HEAD)
    git log -1 --format="commit: $commit ($branch, %cI): %s"
    if [ -z "$(git status --porcelain)" ]; then
        echo "cópia de trabalho: igual ao commit"
        return
    fi
    echo "cópia de trabalho: com alterações sobre o commit (git status --porcelain):"
    git status --porcelain | sed 's/^/    /'
}

main() {
    trap interrupt INT

    section "Bateria do ambiente alvo"
    echo "início: $(date '+%Y-%m-%d %H:%M:%S %z'); log: $LOG"
    repository_lines
    for name in "${VARIABLES[@]}"; do
        echo "$name=${!name}"
    done

    section "Probes"

    run .venv/bin/python probes/redshift.py "$SERIALIZE_DB_TEST_S3_ROOT"
    run .venv/bin/python probes/space.py
    run .venv/bin/python probes/bucket.py "$SERIALIZE_DB_TEST_S3_ROOT"
    run .venv/bin/python probes/diagnose_aws.py "$SERIALIZE_DB_TEST_S3_ROOT"
    run .venv/bin/python probes/catalog.py

    run env SERIALIZE_DB_TEST_REPORT=probes/output/suite_s3.json \
        .venv/bin/python -m pytest -m "not redshift"
    run env SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_1.json \
        .venv/bin/python -m pytest -m redshift
    run env SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_2.json \
        .venv/bin/python -m pytest -m redshift
    run env SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_1.json \
        .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
    run env SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_2.json \
        .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
    run env SERIALIZE_DB_TEST_REPORT=probes/output/publication_1.json \
        .venv/bin/python -m pytest -m redshift tests/test_publication.py
    run env SERIALIZE_DB_TEST_REPORT=probes/output/publication_2.json \
        .venv/bin/python -m pytest -m redshift tests/test_publication.py

    section "Migração Parquet -> Delta"

    # Avalia estrutura dos arquivos parquet de origem.
    run .venv/bin/python probes/parquet_source.py "$SOURCE_PATH"

    # Efetua migração.
    run .venv/bin/python scripts/migrate_parquet_to_delta.py \
        --metadata client_model:Base.metadata \
        --environment prd \
        --source "$SOURCE_PATH" \
        --root "$TARGET_ROOT_PATH" \
        --ignore-partitions 2025-09-30 \
        --report probes/output/carga_inicial_delta.json

    run .venv/bin/serialize-db audit \
        --root "$TARGET_ROOT_PATH" \
        --environment prd \
        --metadata client_model:Base.metadata \
        --table cad_lancamentos \
        --partitions 2026-01-31 \
        --foreign-keys

    run .venv/bin/serialize-db history --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --table cad_lancamentos
    run .venv/bin/serialize-db snapshot --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --name carga-2026-09-24
    run .venv/bin/serialize-db vacuum --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata
    run .venv/bin/serialize-db archive --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --name carga-2026-09-24

    # Benchmark threads do duckdb.
    run .venv/bin/python probes/duckdb_threads.py "$TARGET_ROOT_PATH/prd"

    section "Publicação Delta -> Redshift"

    # 1. Uma vez por esquema: a tabela de controle serialize_db_publications.
    #    Sem ela, publish_redshift recusa com PublicationError; a segunda chamada falha
    #    porque a tabela já existe (sem IF NOT EXISTS, por decisão sua).
    run .venv/bin/serialize-db publish_redshift --init

    # 2. O snapshot da carga e o canal default apontado para ele: a publicação e o leitor
    #    Delta sem argumento leem esse snapshot. "serialize-db channel" sem opções lista os canais.
    run .venv/bin/serialize-db snapshot --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --name carga-2026-09-25
    run .venv/bin/serialize-db channel --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --name default --snapshot carga-2026-09-25
    run .venv/bin/serialize-db channel --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata

    # 3. Primeiro uma tabela pequena, para validar o caminho no alvo; publish_redshift exige
    #    --snapshot <nome> ou --channel <nome> (default, current).
    run .venv/bin/serialize-db publish_redshift --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --tables cad_contas --channel default

    # 4. A base inteira, uma conexão por tabela em paralelo.
    run .venv/bin/serialize-db publish_redshift --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --max-workers 4 --channel default

    # 5. O estado: versão publicada, versão atual e partições pendentes por tabela.
    run .venv/bin/serialize-db publish_redshift --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --status

    # 6. A versão atual sem snapshot (--channel current) e a volta a um snapshot pelo nome, que
    #    troca só as partições alteradas entre as duas versões.
    run .venv/bin/serialize-db publish_redshift --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --channel current
    run .venv/bin/serialize-db publish_redshift --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --snapshot carga-2026-09-25 --tables cad_contas

    section "Teste credencial expirando (leva 1h)"

    # probe credentials: 1h de leitura
    run .venv/bin/python probes/credentials.py "$TARGET_ROOT_PATH/prd/cad_contas"

    section "Acesso de leitura"

    # O leitor Delta sobre o snapshot do canal default e o leitor Redshift sobre as tabelas
    # publicadas, com o mesmo statement: o tempo de abertura das 12 views, as versões e a
    # contagem de cad_contas por cada leitor, que a sonda confere iguais. O relatório vai ao
    # terminal e a probes/output/operacao_leitores_<data-hora>.txt; código de saída 1 quando as
    # contagens diferem.
    run .venv/bin/python probes/operacao/probe_readers.py "$TARGET_ROOT_PATH"

    section "Exportação e Compact"

    # export por cópia (o padrão): os mesmos bytes dos arquivos da versão atual, sem o log
    run .venv/bin/serialize-db export --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --table cad_lancamentos \
        --destination "$TARGET_ROOT_PATH/prd/exportacao/carga-2026-09-24/cad_lancamentos"

    # export por reescrita: um arquivo por partição pelo COPY do DuckDB, com os limites do
    # ambiente
    run .venv/bin/serialize-db export --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --table cad_lancamentos \
        --destination \
            "$TARGET_ROOT_PATH/prd/exportacao/carga-2026-09-24-reescrita/cad_lancamentos" \
        --mode rewrite

    # compact: exige --partitions numa tabela particionada e recusa a tabela com um snapshot na
    # versão atual; numa partição de um arquivo só, não grava nada
    run .venv/bin/serialize-db compact --root "$TARGET_ROOT_PATH" --environment prd \
        --metadata client_model:Base.metadata --table cad_lancamentos --partitions 2026-03-31

    section "Sonda da base publicada"

    # O EXPLAIN do join de prd_cad_lancamentos com prd_cad_contas por id_conta, a primeira
    # partição de cad_lancamentos refeita num snapshot novo e a ida e a volta pelo canal default,
    # com o tempo e o pico de RSS de cada tabela trocada. A sonda grava na própria base: a versão
    # nova de cad_lancamentos, o snapshot refeito-<execution_id> e os manifestos ficam, e o canal
    # default e as tabelas prd_* voltam ao snapshot de antes. Ela roda depois da exportação, que
    # lê a versão atual e assim exporta a da carga, não a da partição refeita. Ela pede cada
    # tabela, publicada e atual, na versão do snapshot do canal default, como a publicação acima
    # deixa; fora disso, como numa segunda rodada sobre a mesma base, para com o código 2 antes de
    # gravar. O relatório vai ao terminal e a probes/output/operacao_base_publicada_<data-hora>.txt;
    # código de saída 1 quando alguma checagem reprova.
    run .venv/bin/python probes/operacao/probe_published_base.py "$TARGET_ROOT_PATH"

    section "Sondas de consistência"

    # As sete sondas da pasta local gravam sob $SERIALIZE_DB_TEST_LOCAL_ROOT/consistencia/<sonda>/,
    # que cada uma apaga no fim, e imprimem o relatório no terminal e em
    # probes/output/consistencia_<sonda>_<data-hora>.txt; código de saída 1 quando alguma checagem
    # reprova. O sinal do zero, as estatísticas que deep_copy não registra e as atualizações
    # perdidas do arquivo de controle (.claude/memory/OPEN_QUESTIONS.md) saem como leituras
    # conhecidas, não como reprovação.
    run .venv/bin/python probes/consistencia/probe_types.py
    run .venv/bin/python probes/consistencia/probe_stream.py
    run .venv/bin/python probes/consistencia/probe_execution.py
    run .venv/bin/python probes/consistencia/probe_delta_ops.py
    run .venv/bin/python probes/consistencia/probe_reader.py
    run .venv/bin/python probes/consistencia/probe_load.py
    run .venv/bin/python probes/consistencia/probe_pandas.py

    # A sonda do motor Redshift roda pelo pytest com as fixtures das suítes: grava sob
    # $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-poc/<id>/ e publica no esquema da suíte num ambiente
    # poc<id> próprio, cujas tabelas poc<id>_* e linhas de controle saem no fim; -s imprime as
    # leituras e os problemas.
    run env SERIALIZE_DB_TEST_REPORT=probes/output/consistencia_redshift.json \
        .venv/bin/python -m pytest -p conftest -m redshift -s \
        probes/consistencia/probe_redshift_test.py

    # A sonda de dois escritores na mesma tabela do sandbox roda pelo pytest nos dois motores:
    # -m local no DuckDB, sob $SERIALIZE_DB_TEST_LOCAL_ROOT/serialize-db-poc/<id>/, e -m redshift
    # no alvo, com o Delta e o staging sob $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-poc/<id>/ e as
    # tabelas do sandbox da execução poc-<id> no esquema da suíte, apagadas no fim. O desfecho de
    # cada escrita (entrou, ou conflito) é leitura; a checagem é a consistência do que ficou.
    run .venv/bin/python -m pytest -p conftest -m local -s \
        probes/consistencia/probe_append_test.py
    run env SERIALIZE_DB_TEST_REPORT=probes/output/consistencia_append_redshift.json \
        .venv/bin/python -m pytest -p conftest -m redshift -s \
        probes/consistencia/probe_append_test.py

    section "Sondas da operação"

    # As sondas que recebem $SOURCE_PATH leem cad_lancamentos nele, fora da partição 2025-09-30,
    # que a carga recusa (--ignore-partitions, como na carga). Cada sonda grava sob
    # $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-operacao/<sonda>-<id>/, que apaga no fim, e imprime
    # o relatório no terminal e em probes/output/operacao_<sonda>_<data-hora>.txt; código de
    # saída 1 quando alguma checagem reprova.

    # A carga das três primeiras partições, encerrada por SIGKILL quando o arquivo da segunda
    # aparece, e o mesmo comando de novo.
    run .venv/bin/python probes/operacao/probe_load_resume.py "$SOURCE_PATH" \
        --ignore-partitions 2025-09-30

    # O archive de um snapshot das três partições, encerrado por SIGKILL depois da primeira
    # partição copiada, e o mesmo comando de novo.
    run .venv/bin/python probes/operacao/probe_archive_resume.py "$SOURCE_PATH" \
        --ignore-partitions 2025-09-30

    # O vacuum --full de dois órfãos com a retenção padrão, com --retention-hours 0 e com --apply.
    run .venv/bin/python probes/operacao/probe_vacuum_orphans.py "$SOURCE_PATH" \
        --ignore-partitions 2025-09-30

    # O compact da primeira partição repartida em cerca de 32 arquivos, com o tempo e o pico de
    # RSS.
    run .venv/bin/python probes/operacao/probe_compact_memory.py "$SOURCE_PATH" \
        --ignore-partitions 2025-09-30

    # O UNLOAD da exportação com PARALLEL OFF e em paralelo, de 1, 5, 10 e 20 milhões de linhas e
    # da primeira partição inteira, três vezes cada; as tabelas exec_operacao_<id>_* saem no fim.
    run .venv/bin/python probes/operacao/probe_unload_parallel.py "$SOURCE_PATH" \
        --ignore-partitions 2025-09-30

    # O ganho das APIs com threads sobre a execução em série no motor DuckDB, nos pools de
    # tabelas, no motor Redshift e na publicação no Redshift, três medidas de cada forma, sobre
    # quatro tabelas de 5 milhões de linhas que a sonda gera, com o tempo de cada tipo de comando
    # em cada medida da publicação; as tabelas exec_* do sandbox e as poc<id>_* publicadas, com
    # as linhas de controle delas, saem no fim.
    run .venv/bin/python probes/operacao/probe_parallel_gain.py

    summary
}

export PYTHONPATH=tests
# Com a saída num pipe, o Python guardaria o que imprime até encher o buffer ou sair; sem buffer,
# cada linha chega ao terminal e ao log na hora, na ordem em que foi impressa.
export PYTHONUNBUFFERED=1

mkdir -p probes/output "$SERIALIZE_DB_TEST_LOCAL_ROOT"
LOG=probes/output/suite_alvo_$(date +%Y%m%d-%H%M%S).txt

# Tudo passa pelo tee, que segue até o fim do log quando Ctrl-C chega, para a tabela dos passos
# entrar nele; o código de saída é o de main, não o do tee.
main 2>&1 | tee --ignore-interrupts "$LOG"
exit "${PIPESTATUS[0]}"

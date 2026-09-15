# Fluxo endurecido de retrieve

Este documento descreve o comportamento operacional do worker e os limites de
falha entre etapas. O identificador `correlation_id` acompanha um pedido nos logs;
`unit_id`, `order_id`, `transfer_id`, `move_kind`, `attempt` e `command_status`
permitem reconstruir cada execução sem registrar dados clínicos em texto livre.

## Fluxo principal

1. **Ingestão PLERES (`orders.api.*`)**
   - consulta a API por unidade;
   - valida presença, formato, tamanho e caracteres dos identificadores;
   - persiste cada pedido em savepoint próprio;
   - confirma a leitura de modo idempotente e mantém ACKs falhos pendentes;
   - os ACKs rodam fora do ciclo principal, em lotes concorrentes por unidade;
   - cada resposta é persistida imediatamente, sem esperar o restante do lote;
   - um job lento de confirmação não bloqueia C-FIND, C-MOVE ou outra unidade.
2. **Localização no PACS (`dicom.find`)**
   - consulta pedidos em `watching` no intervalo configurado;
   - `exit 0` sem Study UID significa apenas “ainda não encontrado”;
   - timeout, erro de processo ou `exit != 0` são falhas técnicas e geram retry;
   - metadados são normalizados antes de entrar no PostgreSQL;
   - quando encontrado, agenda primeiro e segundo retrieve a partir do C-FIND.
3. **Claim de C-MOVE (`dicom.move.claim`)**
   - respeita `max_parallel_moves` da unidade;
   - usa bloqueio de linha com `SKIP LOCKED`, evitando claim duplicado quando há
     mais de um worker;
   - move manual tem prioridade, seguido do exame atual e do histórico.
4. **Retrieve atual (`dicom.move`)**
   - primeiro e segundo retrieve possuem até três tentativas cada;
   - retries usam espera de 60 e 300 segundos;
   - uma exceção inesperada é persistida imediatamente, sem aguardar o recovery
     de lock;
   - o segundo retrieve continua baseado no horário calculado após o C-FIND.
5. **Retrieve histórico (`dicom.move.prior`)**
   - o C-FIND histórico sem séries conclui como “nenhum exame anterior” e não
     dispara C-MOVE;
   - cada Series UID possui checkpoint persistente em `historical_series`;
   - séries concluídas não são repetidas quando outra série falha;
   - a saída diagnóstica mantida em memória e nos eventos é limitada.
6. **Recepção (`dicom.store.*`)**
   - existe um `storescp` supervisionado por unidade;
   - queda, configuração inválida ou binário ausente ficam isolados na unidade;
   - o supervisor reinicia processos encerrados e mantém somente a cauda
     sanitizada da saída;
   - um estudo recebido sem pedido é registrado de forma idempotente por unidade
     e Study UID, depois de tentar a associação com o histórico;
   - o recebimento direto substitui o primeiro retrieve e respeita a regra da
     modalidade para agendar ou não o segundo;
   - identificadores obrigatórios ausentes ou conflito entre accession e Study
     UID enviam o arquivo para quarentena, sem criar pedido ou enviar à nuvem;
   - pedidos arquivados do mesmo Study UID são restaurados em vez de duplicados.
7. **Compactação (`dicom.compact.*`)**
   - processa no máximo `COMPACT_BATCH_SIZE` arquivos por ciclo;
   - a fila é lida com `scandir` e a varredura para assim que o lote é preenchido,
     evitando ordenar e manter dezenas de milhares de caminhos em memória;
   - unidades são compactadas em jobs independentes e a concorrência total dos
     codecs é limitada por `COMPACT_GLOBAL_WORKERS`;
   - cada arquivo é executado isoladamente; uma exceção não cancela os demais;
   - arquivos que provocam exceção inesperada são movidos para quarentena, para
     não reaparecerem indefinidamente no início de cada lote;
   - o codec grava em arquivo oculto temporário e só publica a saída com rename
     atômico, portanto o uploader nunca lê um DICOM parcialmente escrito;
   - arquivos já codificados exatamente no perfil solicitado preservam o pixel
     data e não executam novamente o `dcmcjpeg`, desde que nenhuma regra tenha
     alterado o dataset;
   - a origem recebida nunca é regravada: metadados são preparados em arquivo
     temporário e uma falha mantém o original íntegro para diagnóstico;
   - metadados e vínculos são pré-carregados e persistidos em pequenos lotes;
     conflito ou erro no lote aciona automaticamente o fallback por arquivo;
   - temporários abandonados são removidos apenas depois de uma idade segura;
   - arquivos inválidos seguem para o diretório de erro e descartes são auditados.
8. **Envio (`cloud.upload.*`)**
   - roda em job próprio por unidade, sem esperar compactação ou outra unidade;
   - processa no máximo `SEND_BATCH_SIZE` arquivos por ciclo;
   - falhas usam backoff exponencial persistente;
   - o circuit breaker é independente por unidade;
   - o sucesso remoto é commitado antes da exclusão do arquivo local;
   - se a exclusão local falhar, o próximo ciclo limpa a sobra sem reenviar.

## Contenção e recuperação

Cada manutenção global e cada combinação unidade/etapa abre uma sessão de banco
independente. Uma falha em ingestão, C-FIND, claim, compactação ou envio não impede
as etapas seguintes nem as outras unidades. Jobs do executor também abrem sessão
própria e sempre tentam finalizar o estado persistente após exceções.

Locks órfãos são recuperados respeitando os timeouts configurados. Cancelamentos
são recusados durante C-MOVE atual ou histórico; solicitações manuais ainda na fila
são canceladas junto com o pedido. Isso evita que uma thread conclua por cima do
estado `cancelled`.

## Ações de log para alertas

- `worker.stage status=failure`: falha contida de uma etapa/unidade;
- `dicom.find status=retry`: PACS ou comando indisponível, não “não encontrado”;
- `orders.api.ack.batch`: início e resumo de cada lote de confirmações PLERES;
- `orders.api.ack status=retry`: confirmação individual mantida para nova tentativa;
- `dicom.move.job.persist_failure`: falha crítica ao salvar a recuperação do job;
- `dicom.move status=failure`: três tentativas do retrieve atual esgotadas;
- `dicom.move.prior status=failure`: tentativas do histórico esgotadas;
- `dicom.compact.batch status=partial`: um ou mais arquivos falharam;
- `dicom.compact.persist.batch status=fallback`: conflito no lote de banco,
  tratado novamente de forma isolada por arquivo;
- `dicom.compact.temp.cleanup status=failure`: temporário antigo não pôde ser removido;
- `order.storescp.ingest`: pedido direto criado, associado, reutilizado ou restaurado;
- `dicom.inbound.reject`: objeto direto rejeitado por identidade incompleta ou conflitante;
- `dicom.inbound.quarantine status=failure`: falha ao mover objeto rejeitado para erro;
- `cloud.upload.batch status=partial`: falha parcial ou total de envio;
- `cloud.circuit status=open`: unidade temporariamente suspensa para envio;
- `cloud.upload.persist status=failure`: resposta remota não pôde ser registrada.

Sucessos por imagem ficam em nível `DEBUG`; em `INFO`, os resumos de lote preservam
visibilidade sem inundar o log. Para investigação temporária, use `LOG_LEVEL=DEBUG`.

## Atualização em produção

A atualização é compatível com pedidos existentes: nenhum estado de pedido é
reiniciado. Na primeira inicialização, `init_db()` cria a tabela aditiva
`historical_series`; históricos em andamento passam a preencher checkpoints a
partir da próxima tentativa. Faça backup do PostgreSQL antes do rollout e suba o
serviço web antes do worker, como já definido no Compose.

Variáveis novas e seus padrões:

- `FIND_BATCH_SIZE=10`
- `COMPACT_BATCH_SIZE=250`
- `COMPACT_GLOBAL_WORKERS=8`
- `COMPACT_DB_BATCH_SIZE=25`
- `COMPACT_TEMP_MAX_AGE_SECONDS=3600`
- `DCMCJPEG_TIMEOUT_SECONDS=300`
- `COMPACT_UNIT_SCHEDULERS=4`
- `SEND_UNIT_SCHEDULERS=4`
- `DASHBOARD_FILE_COUNT_CACHE_SECONDS=30`
- `SEND_BATCH_SIZE=250`
- `ORDERS_API_ACK_BATCH_SIZE=32`
- `ORDERS_API_ACK_CONCURRENCY=8`
- `ORDERS_API_ACK_UNIT_WORKERS=4`

## Teste de integração PostgreSQL

A suíte normal permanece rápida e o contrato específico de produção é opt-in.
Use um banco descartável cujo nome contenha `test`:

```powershell
$env:TEST_POSTGRES_URL = "postgresql+psycopg://usuario:senha@host/retrieve_test"
python -m unittest tests.test_postgres_integration -v
```

O teste cria um schema aleatório, valida a tabela de checkpoints e confirma que
`FOR UPDATE SKIP LOCKED` impede dois workers de selecionar o mesmo pedido. O schema
temporário é removido no final; o teste se recusa a executar em banco sem `test` no
nome.

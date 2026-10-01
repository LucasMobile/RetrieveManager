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
   - C-FIND em pynetdicom: status final Success sem Study UID significa apenas
     “ainda não encontrado”;
   - associação recusada, sem conexão, tempo esgotado ou status de falha são
     falhas técnicas e geram retry (`dicom.find status=retry error_type=...`);
   - metadados são normalizados antes de entrar no PostgreSQL;
   - cada resposta `Pending` é lida separadamente; mais de um Study UID ou
     PatientID/nascimento/accession divergente leva o pedido a `error`
     (`dicom.find status=conflict`) sem agendar C-MOVE;
   - quando encontrado, agenda primeiro e segundo retrieve a partir do C-FIND.
3. **Claim de C-MOVE (`dicom.move.claim`)**
   - respeita `max_parallel_moves` da unidade;
   - usa bloqueio de linha com `SKIP LOCKED`, evitando claim duplicado quando há
     mais de um worker;
   - move manual tem prioridade, seguido do exame atual e do histórico.
4. **Retrieve atual (`dicom.move`)**
   - C-MOVE em pynetdicom com o Calling AET da unidade como destino; sucesso
     exige status final Success **e** nenhuma sub-operação com falha. Falha
     parcial (0xB000), destino desconhecido (0xA801) ou tempo esgotado (a
     associação é abortada) contam como tentativa com erro; o evento e o log
     trazem as contagens enviadas/falhas/avisos;
   - primeiro e segundo retrieve possuem até três tentativas cada;
   - retries usam espera de 60 e 300 segundos;
   - uma exceção inesperada é persistida imediatamente, sem aguardar o recovery
     de lock;
   - o segundo retrieve continua baseado no horário calculado após o C-FIND.
   - o segundo retrieve é incremental: só as séries em que o PACS tem mais
     SOPs do que os já recebidos; sem contagem confiável, o estudo inteiro;
     sem nada novo, termina sem C-MOVE (evento "2º C-MOVE dispensado").
5. **Retrieve histórico (`dicom.move.prior`)**
   - o C-FIND histórico sem séries conclui como “nenhum exame anterior” e não
     dispara C-MOVE;
   - cada Series UID possui checkpoint persistente em `historical_series`;
   - séries concluídas não são repetidas quando outra série falha;
   - a saída diagnóstica mantida em memória e nos eventos é limitada.
6. **Recepção (`dicom.store.*`)**
   - o serviço `receiver` mantém um Store SCP pynetdicom por unidade habilitada
     e o recria quando porta, AET, pastas, IPs ou AEs autorizados mudam;
   - associação de IP fora da lista (endereços ou faixas CIDR; vazio aceita
     todos), com Called AET diferente ou Calling AET fora da lista é recusada
     com A-ASSOCIATE-RJ (`dicom.receive.association status=rejected`);
   - o objeto é gravado byte a byte como recebido (temporário oculto + rename),
     com o SHA-256 do dataset, e registrado em `dicom_instances` antes do
     sucesso; os registros pendentes compartilham um commit por vez;
   - banco indisponível, commit acima de `RECEIVER_COMMIT_TIMEOUT_SECONDS` ou
     disco abaixo de `RECEIVER_MIN_FREE_MB` respondem `0xA700` (o PACS reenvia);
     o arquivo gravado fica para a adoção do worker;
   - o mesmo SOP e conteúdo é duplicata (confirmada sem nova compactação); o
     mesmo SOP com conteúdo diferente vira `conflict` na pasta de erro;
   - uma linha por associação resume gravados, duplicatas, conflitos e falhas
     (`dicom.receive.association status=released`);
   - a compactação reconfere o AE de origem gravado no meta header
     (`dicom.inbound.reject error_type=UnauthorizedSender`);
   - um estudo recebido sem pedido é registrado de forma idempotente por unidade
     e Study UID, depois de tentar a associação com o histórico;
   - o recebimento direto substitui o primeiro retrieve e respeita a regra da
     modalidade para agendar ou não o segundo;
   - identificadores obrigatórios ausentes ou conflito entre accession e Study
     UID enviam o arquivo para quarentena, sem criar pedido ou enviar à nuvem;
   - pedidos arquivados do mesmo Study UID são restaurados em vez de duplicados.
7. **Compactação (`dicom.compact.*`)**
   - reserva até `COMPACT_BATCH_SIZE` instâncias `received` com `SKIP LOCKED`,
     sem varrer a pasta; o estado final (`compacted`, `discarded`, `rejected`,
     `error`) é gravado na mesma transação da transferência;
   - reservas de um worker que caiu voltam à fila após o timeout; arquivo
     ausente vira `missing`;
   - arquivos sem registro na pasta de recebimento são adotados após
     `RECEIVE_ADOPT_MIN_AGE_SECONDS` (`dicom.receive.adopt`);
   - a fila é lida com `scandir` e a varredura para assim que o lote é preenchido,
     evitando ordenar e manter dezenas de milhares de caminhos em memória;
   - unidades são compactadas em jobs independentes e a concorrência total dos
     codecs é limitada por `COMPACT_GLOBAL_WORKERS`;
   - cada arquivo é executado isoladamente; uma exceção não cancela os demais;
   - arquivos que provocam exceção inesperada são movidos para quarentena, para
     não reaparecerem indefinidamente no início de cada lote;
   - o codec grava em arquivo oculto temporário e só publica a saída com rename
     atômico, portanto o uploader nunca lê um DICOM parcialmente escrito;
   - o charset declarado é preservado; `ISO_IR 100` só é gravado quando o
     arquivo não declara charset ou declara ASCII;
   - regras não podem alterar tags de identidade, pixel ou do sistema; regras
     nessas tags não são executadas;
   - pixel data que já chega comprimido é preservado sem recompressão (só os
     metadados são regravados);
   - o codec JPEG 2000 roda em processo isolado e nunca altera o SOP Instance
     UID, Rows, Columns, frames ou bits; imagens lossy são marcadas em
     `LossyImageCompression`;
   - a origem recebida nunca é regravada: metadados são preparados em arquivo
     temporário e uma falha mantém o original íntegro para diagnóstico;
   - metadados e vínculos são pré-carregados e persistidos em pequenos lotes;
     conflito ou erro no lote aciona automaticamente o fallback por arquivo;
   - temporários abandonados são removidos apenas depois de uma idade segura;
   - arquivos inválidos seguem para o diretório de erro e descartes são auditados.
8. **Envio (`cloud.upload.*`)**
   - roda em job próprio por unidade, sem esperar compactação ou outra unidade;
   - a fila é só a tabela: a pasta de envio não é varrida;
   - o artefato é registrado (nome e SHA-256, estado `publishing`) antes de entrar
     na pasta de envio; a recuperação confere o hash ou recompacta da origem;
   - fluxo contínuo em ordem FIFO: `send_workers` uploads em andamento, cada um
     reposto ao terminar, com uma única sessão HTTP por até `SEND_DRAIN_SECONDS`;
   - limita a concorrência somada das unidades com `SEND_GLOBAL_CONCURRENCY`;
   - persiste os resultados em lotes e volta ao modo individual se um lote falhar;
   - falhas esperam 10 s, 30 s, 1, 2, 5 e 10 min (±20%; depois 30 min se o limite
     for maior), filtradas no SQL;
     após `SEND_MAX_ATTEMPTS` a imagem vai para `send_error`; a cada 5 minutos
     um único upload de teste decide se todas voltam para a fila, e o botão de
     reenvio (pedido ou unidade, auditado) faz o mesmo manualmente;
   - o circuit breaker é independente por unidade;
   - o sucesso remoto é commitado antes da exclusão do arquivo local; se a
     exclusão falhar, a sobra não é reenviada porque a linha já está `uploaded`;
   - indisponibilidade da pasta/volume não altera os estados pendentes no banco.

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
- `dicom.inbound.reject error_type=UnauthorizedSender`: Calling AET fora da lista
  autorizada da unidade (campo `calling_aet` no log);
- `dicom.find status=conflict`: C-FIND ambíguo ou de outro paciente; pedido em erro;
- `dicom.find.prior status=rejected`: séries do histórico ignoradas por identidade divergente;
- `dicom.inbound.quarantine status=failure`: falha ao mover objeto rejeitado para erro;
- `cloud.upload.batch status=partial`: falha parcial ou total de envio;
- `cloud.upload.persist_batch status=fallback`: erro de lote isolado por arquivo;
- `cloud.send.directory status=failure`: pasta/volume de envio indisponível;
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
- `COMPACT_FILE_TIMEOUT_SECONDS=300`
- `COMPACT_MAX_ENCODE_BYTES=268435456`
- `COMPACT_UNIT_SCHEDULERS=4`
- `SEND_UNIT_SCHEDULERS=4`
- `DASHBOARD_FILE_COUNT_CACHE_SECONDS=30`
- `SEND_DB_BATCH_SIZE=50`
- `SEND_MAX_ATTEMPTS=7`
- `SEND_GLOBAL_CONCURRENCY=32`
- `SEND_DRAIN_SECONDS=20`
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

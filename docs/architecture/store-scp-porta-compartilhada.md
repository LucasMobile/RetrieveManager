# Store SCP com porta compartilhada entre unidades

Status: **implementado** na branch `rc-1` (pynetdicom 3.0.4). A seção 10 lista o que mudou em relação ao plano.

## 1. Objetivo

Permitir que duas ou mais unidades usem **a mesma porta e o mesmo AE Title chamado**
no Store SCP. A unidade de destino de cada associação é decidida pelo **AE Title do
remetente** (Calling AET).

```
PACS "serverPacs1" ──┐                              ┌─► Empresa1 → full_images1
                     ├─► :445  called "MOBILEMED" ──┤
PACS "serverPacs2" ──┘                              └─► Empresa2 → full_images2
```

Fora do escopo:
- Rotear por conteúdo do objeto (tags DICOM). A decisão é tomada **uma vez por
  associação**, antes de receber qualquer imagem.
- Dois PACS que se apresentam com o mesmo AE Title. Isso continua exigindo portas
  diferentes.

## 2. Como funciona hoje (fatos verificados no código)

| Ponto | Onde | Comportamento |
|---|---|---|
| Um listener por unidade | `app/receiver.py` `Receiver._start_server` | Cada unidade abre um `AE.start_server` próprio. Os handlers recebem a `ReceiverRoute` da unidade como argumento fixo. |
| Porta exclusiva | `app/routes/units.py` `_port_taken` | O cadastro recusa porta já usada por outra unidade não arquivada, inclusive pausada. |
| Pastas | `app/validation.py` | Valida que a pasta está dentro de `ALLOWED_DATA_ROOTS`. **Não** valida que as pastas são diferentes entre unidades. Hoje isso não causa problema só porque a porta é exclusiva. |
| Filtro de remetente | `Receiver._on_requested` | Rejeita (A-ASSOCIATE-RJ) se o IP não está liberado, se o AET chamado difere ou se o remetente está fora da lista. |
| Reconcile | `Receiver.reconcile` | A cada `WORKER_INTERVAL_SECONDS`, se a rota de uma unidade mudou, **derruba e recria** o listener. |
| Parada do listener | pynetdicom `AssociationServer.shutdown` | Fecha o socket de escuta, mas **não aborta** associações em andamento. Elas continuam com a rota antiga. |
| `EVT_REQUESTED` | pynetdicom `events.trigger` | É evento de *notificação*: exceção no handler é **engolida e logada**, e a associação **é aceita** mesmo assim. |
| Limite de associações | pynetdicom `acse` | `maximum_associations` é por objeto `AE`, que hoje é um por unidade. |
| Recheck na compactação | `app/pipeline/compact.py:568` | A compactação reconfere o remetente gravado no arquivo contra `store_allowed_aets` da unidade. Remetente fora da lista vai para a pasta de erro. |
| Adoção de arquivos | `compact._adopt_receive_files` | Arquivo sem linha no banco dentro de `receive_dir` é adotado **pela unidade dona da pasta**. |
| Dashboard | `netutil.port_listening` | "Porta escutando" é só um `connect()` na porta e não sabe se a unidade está roteável. |
| Destino do C-MOVE | `app/dicom_net.py:315` | `destination_aet = unit.calling_aet`. Cada PACS precisa ter esse AET apontando para host:porta. |

## 3. Desenho proposto

### 3.1 Chave de roteamento

```
(porta de bind, AET chamado normalizado, AET remetente normalizado) → unidade
```

- A normalização é a mesma usada hoje: `strip()` + `upper()`, no máximo 16
  caracteres. Uma **única função** atende o cadastro, o receiver e a compactação
  (`store_allowed_senders`).
- O **IP não entra na chave**. Ele é um filtro aplicado **depois** de escolher a
  unidade. Colocar faixas CIDR na chave tornaria a detecção de ambiguidade complexa
  (sobreposição de redes) sem ganho real.
- Como o AET chamado entra na chave, a mesma porta pode atender AETs chamados
  diferentes sem custo extra.

### 3.2 Regras de cadastro

Consideram todas as unidades **não arquivadas, inclusive pausadas**. Assim, ativar
uma unidade pausada nunca cria conflito.

1. **Grupo** = unidades com a mesma porta (após `STORE_PORT_MAP`) e o mesmo AET
   chamado.
2. Grupo com **uma** unidade: tudo como hoje, e a lista vazia aceita qualquer
   remetente.
3. Grupo com **duas ou mais** unidades:
   - toda unidade do grupo precisa ter "AE Titles autorizados" **preenchido**. Não
     existe unidade "pega-tudo";
   - as listas precisam ser **disjuntas**, e a mensagem de erro informa a unidade e
     o AET em conflito;
   - recomendação forte (ver decisão D2): "IPs autorizados" preenchido.
4. **Pastas únicas**: `receive_dir`, `send_dir` e `error_dir` de todas as unidades
   precisam ser diferentes entre si e **não aninhadas**, comparando
   `Path.resolve()`. Isso vale para qualquer unidade, compartilhando porta ou não.
   É a principal proteção contra vazamento entre empresas (ver I1).
5. O `_port_taken` atual sai e entra `store_route_conflicts(...)`.

### 3.3 Receiver: um listener por porta e tabela imutável

```
Receiver
 ├── _listeners: {bind_port: AssociationServer}   ← muda só quando uma porta aparece ou some
 ├── _table: RoutingTable (imutável, frozen)      ← trocada por atribuição atômica
 └── reconcile(units)
       1. table = build_routing_table(units)      ← função pura, um único snapshot do banco
       2. self._table = table                     ← swap atômico de referência
       3. abre listeners de portas novas, fecha os de portas sem nenhuma unidade
       4. aborta associações cuja rota fixada deixou de existir ou mudou (C4)
```

`RoutingTable` (dataclass frozen):
- `routes: dict[(port, called, calling), ReceiverRoute]`
- `open_groups: dict[(port, called), ReceiverRoute]` para grupos de uma unidade com
  lista vazia
- `conflicts: dict[(port, called, calling), tuple[unit_id, ...]]` para estados
  ambíguos detectados no snapshot
- `generation: int`

`ReceiverRoute` ganha `generation`. Uma alteração material da unidade (pasta,
listas, pausa) gera uma rota diferente.

### 3.4 Fluxo de uma associação

1. `EVT_REQUESTED` → `_on_requested(event, port)`, **todo envolvido em `try/except`**:
   qualquer exceção resulta em A-ASSOCIATE-RJ + log ERROR (ver C2).
2. Lê `table = self._table` **uma única vez** e passa a usar só essa referência local.
3. Procura `(port, called, calling)` em `routes`; se não encontrar, procura
   `(port, called)` em `open_groups`.
   - Chave em `conflicts`: rejeita, log ERROR `AmbiguousStoreRoute`. Nunca escolhe
     uma unidade por id.
   - Nenhuma rota: rejeita (`CallingAETitleNotRecognized` ou
     `CalledAETitleNotRecognized`).
4. Aplica o filtro de IP da rota escolhida.
5. Aplica o limite de associações **da unidade** (C9).
6. **Fixa a rota no próprio objeto `Association`** (`assoc.retrieve_route = route`),
   e não num dict por `id(assoc)` (ver C10).
7. `EVT_C_STORE` → `_on_store(event)` lê `event.assoc.retrieve_route`.
   - Sem rota fixada: **falha fechada**, sem gravar nada, responde `0xA700` + log
     ERROR (C2).
   - Rota fixada não é mais a rota atual da unidade: responde `0xA700` (C4).
8. O resto do `_on_store` não muda: grava, `InstanceWriter`, ack só após o commit.

### 3.5 Web

- Create e update chamam `store_route_conflicts` **dentro da transação, depois de
  `pg_advisory_xact_lock(<constante>)`** (C5).
- Formulário:
  - corrigir o texto de ajuda atual de "AE Titles autorizados", que diz que objetos
    de outros AEs vão para a pasta de erro, mas o receiver rejeita a associação;
  - novo texto: "Obrigatório quando outra unidade usa a mesma porta e AET; define
    para qual unidade as imagens vão".
- Unidade e dashboard: mostrar "Porta compartilhada com: Empresa2" e alertar quando
  houver conflito, usando a mesma função pura em vez de só `port_listening`.

### 3.6 Banco

Não há migração de schema. Regras sobre listas separadas por vírgula não cabem em
constraint SQL; quem garante é a validação com lock no web, mais a detecção de
conflito no receiver (defesa em profundidade).

## 4. Problemas de concorrência antecipados

| # | Cenário | O que daria errado | Mitigação |
|---|---|---|---|
| **C1** | O reconcile troca a tabela enquanto associações estão sendo aceitas ou recebendo. | Leitura de estrutura parcialmente atualizada, ou uma associação usando rotas de duas gerações diferentes. | Tabela imutável construída inteira antes do swap. Swap por atribuição de referência (atômico no CPython). O handler lê `self._table` uma vez. A rota é fixada na associação no `EVT_REQUESTED`. |
| **C2** | Exceção dentro de `_on_requested` (bug, IP malformado, tabela inconsistente). | pynetdicom engole a exceção e **aceita a associação sem rota**. No desenho novo, o C-STORE não teria pasta nem unidade. Hoje o problema fica escondido porque a rota vem fixa no handler. | `try/except` total no `_on_requested` que rejeita no erro. `_on_store` falha fechado se a associação não tiver rota. Teste específico forçando exceção. |
| **C3** | Editar a Empresa1 derruba e recria o listener (comportamento atual). | Com a porta compartilhada, **a Empresa2 também cai**: conexões recusadas durante o restart, e o rebind pode falhar. | Listener vive enquanto existir ao menos uma unidade na porta. Edição só troca a tabela, sem restart. |
| **C4** | Associação em andamento da Empresa1 quando ela é pausada, arquivada ou tem a pasta alterada. | Hoje a associação continua gravando com a rota antiga (pasta antiga, unidade pausada) até o PACS fechar. Num estudo grande isso dura minutos. | Ao trocar a tabela, aborta (A-ABORT) as associações cuja rota fixada não existe mais ou mudou. Além disso, `_on_store` responde `0xA700` se `route != table.route_for(unit_id)`. O PACS reenvia, e a idempotência por SHA-256 já existe. As associações da Empresa2 não são tocadas. |
| **C5** | Dois admins (ou duas abas) salvam ao mesmo tempo unidades que conflitam. | As duas validações passam (TOCTOU) e ficam duas unidades com o mesmo remetente. O `_port_taken` atual tem o mesmo problema. | `pg_advisory_xact_lock` no create e no update antes de validar, para serializar as alterações de roteamento. |
| **C6** | Estado inválido no banco mesmo assim (SQL manual, versão antiga do web, restore de backup). | O receiver escolheria uma unidade arbitrária e **mandaria imagens para a empresa errada**. | O receiver detecta ambiguidade no snapshot e **rejeita** apenas as chaves ambíguas, com log ERROR e alerta no dashboard. As demais chaves continuam funcionando. |
| **C7** | Janela entre salvar no web e o próximo reconcile (até `WORKER_INTERVAL_SECONDS`). | Rota velha por alguns segundos. Ao mover `serverPacs1` da Empresa1 para a Empresa2, é preciso salvar duas vezes (a regra de disjunção obriga), e no meio o remetente é rejeitado. | Aceitável: falha fechada e o PACS repete. Documentar o procedimento. Opcional: `LISTEN/NOTIFY` do Postgres para reconcile imediato, numa fase posterior. |
| **C8** | Mover um remetente de unidade com arquivos ainda não compactados na unidade antiga. | O recheck da compactação (`compact.py:568`) manda esses arquivos para a **pasta de erro da unidade antiga**. Não é vazamento, mas surpreende. | Aviso no formulário quando a unidade tem fila de recebimento pendente. O procedimento é esperar a fila zerar. Não muda o recheck, que é uma proteção boa. |
| **C9** | `maximum_associations` passa a ser por porta, e não por unidade. | O PACS da Empresa1 abre 10 associações e o da Empresa2 é rejeitado: um cliente derruba o outro. | Contador por unidade, protegido por lock, no `EVT_REQUESTED`, com rejeição transitória (result 0x02, *local limit exceeded*). O limite da AE da porta passa a ser a soma dos limites das unidades do grupo. O decremento acontece no `EVT_RELEASED`/`EVT_ABORTED` e **também** quando a própria rejeição acontece. |
| **C10** | Estado por associação indexado por `id(assoc)` (o `_stats` atual). | Se um `EVT_RELEASED`/`ABORTED` não disparar, a entrada vaza e o `id()` pode ser **reutilizado** por outra associação. Se a rota viesse desse dict, seria roteamento para a empresa errada. | A rota fica como atributo do objeto `Association`. O `_stats` migra para o mesmo lugar, ou passa a ser limpo também em rejeições. |
| **C11** | Falha no bind da porta (outro processo usando, `TIME_WAIT`). | Todas as unidades da porta ficam sem recebimento. | O comportamento atual já repete no próximo reconcile (`allow_reuse_address=True` no pynetdicom). O log passa a usar `resource=port:445` e listar as unidades afetadas, e o dashboard mostra o status por porta. |
| **C12** | `InstanceWriter` único e `_free_space` compartilhados. | — | Já são compartilhados entre todas as unidades hoje (um processo receiver). Nada muda. |
| **C13** | O mesmo SOP chega pelas duas empresas. | — | A unique é `(unit_id, sop_uid, sha256)`, então são linhas independentes. Nada muda. |
| **C14** | SIGTERM com associações abertas. | — | Igual a hoje: o ack só sai após o commit, e o que não foi confirmado é reenviado pelo PACS. |

## 5. Isolamento entre empresas

| # | Risco | Mitigação |
|---|---|---|
| **I1** | Pastas iguais ou aninhadas entre unidades. A adoção de arquivos e o "reprocessar erros" atribuem o arquivo **à unidade dona da pasta**, então as imagens da Empresa2 iriam para a nuvem da Empresa1. | Regra 3.2.4, validada com `resolve()`. Limitação: dois bind mounts diferentes do mesmo diretório do host não são detectáveis de dentro do container. Documentar. |
| **I2** | AE Title falsificado: qualquer equipamento na rede pode dizer que é `serverPacs1`. | Filtro de IP por unidade (decisão D2). |
| **I3** | Unidade com lista vazia num grupo compartilhado vira um "pega-tudo" silencioso, por exemplo quando o PACS muda de AET. | Proibido pela regra 3.2.3. |
| **I4** | Normalização divergente entre cadastro, receiver e compactação (maiúsculas, espaços, mais de 16 caracteres). | Uma única função de normalização, com teste de propriedade cobrindo as três camadas. |
| **I5** | O PACS usa nas sub-operações de C-MOVE um Calling AET diferente do esperado. | Não vaza (é rejeitado), mas quebra o retrieve. Diagnóstico pelo log `dicom.receive.association` com `calling_aet`. Opcional: mostrar "últimos remetentes rejeitados" na tela da unidade. |

## 6. Riscos operacionais

- **O1. Publicação da porta.** O container roda com `cap_drop: ALL`, sem
  `NET_BIND_SERVICE`, então a porta 445 precisa de mapeamento:
  `STORE_PORT_MAP: "444:10444,445:10445"` e `ports: "445:10445"` no serviço
  `receiver`, além da liberação no firewall do host.
- **O2. Ordem de deploy.** Subir o **receiver** antes do web. Se o web novo permitir
  porta compartilhada com o receiver antigo rodando, o receiver antigo falha no bind
  da segunda unidade, e essa unidade fica sem recebimento, mas sem vazamento. Com
  `docker compose up -d` tudo sobe junto, então o risco fica restrito a deploys
  parciais.
- **O3. Rollback seguro.** Uma versão antiga com unidades compartilhando a porta
  abre o listener da unidade de menor id. Como a lista dessa unidade é obrigatória e
  disjunta, o remetente da outra é **rejeitado**, sem vazamento. A outra empresa fica
  parada até separar as portas.
- **O4. Permissões.** As pastas novas (`full_images1`, `full_images2`) precisam de
  `rwx` para o UID/GID `10001`.
- **O5. Dashboard.** O "porta escutando" verde não basta. É preciso mostrar se a
  unidade está **roteável** (sem conflito, com o listener ativo).

## 7. Etapas de implementação

Cada etapa é um commit revisável e pode ir para produção sozinha, nesta ordem.

1. **`app/store_routing.py` (função pura).** `normalize_aet`,
   `build_routing_table(units)` e `store_route_conflicts(units, candidate)`.
   Inclui as regras de pastas únicas. Testes unitários puros, sem rede e sem banco.
2. **Receiver.** Listener por porta, tabela imutável, rota fixada na associação,
   falha fechada (C2), abort por mudança de rota (C4), limite por unidade (C9) e
   estado no objeto da associação (C10). Até aqui o web ainda bloqueia porta
   duplicada, então o comportamento em produção é igual ao atual. Este é o ponto de
   validação mais seguro.
3. **Web.** Troca `_port_taken` pela validação nova com advisory lock, passa a
   exigir pastas únicas e mostra mensagens de conflito.
4. **UI e dashboard.** Textos de ajuda, "Porta compartilhada com…", alerta de
   conflito e status roteável.
5. **Operação.** `docker-compose.yml`, README e procedimento para mover um
   remetente entre unidades.

## 8. Plano de testes

**Unitários (etapa 1)**
- Grupo de uma unidade com lista vazia aceita qualquer remetente; grupo de duas com
  lista vazia gera conflito.
- Listas sobrepostas, com variação de maiúsculas e espaços, geram conflito.
- AETs chamados diferentes na mesma porta não conflitam.
- Unidade pausada participa do conflito; unidade arquivada não.
- Pastas iguais, aninhadas, com barra final ou com `..` geram conflito.

**Integração com pynetdicom real (etapa 2, no padrão de `tests/test_receiver.py`)**
- Duas unidades na mesma porta: `serverPacs1` cai só em `full_images1`
  (`unit_id`, `source_path`), e `serverPacs2` só em `full_images2`.
- Remetente desconhecido é rejeitado e nenhum arquivo é criado.
- IP fora da lista da unidade escolhida é rejeitado, mesmo com o AET correto.
- Editar a Empresa1 durante uma associação aberta da Empresa2 não interrompe a
  Empresa2.
- Pausar a Empresa1 durante uma associação aberta dela faz o próximo C-STORE
  responder `0xA700` (ou abortar), e nada é gravado depois da pausa.
- Exceção forçada no roteamento faz a associação ser rejeitada e não grava nada.
- O limite por unidade satura a Empresa1 sem impedir a Empresa2.
- Conflito injetado direto no banco faz só a chave ambígua ser rejeitada.

**Stress e concorrência**
- Oito threads enviando para as duas unidades na mesma porta enquanto outra thread
  chama `reconcile` em loop alternando campos irrelevantes e a pausa de uma terceira
  unidade. Asserção: **zero objetos com `unit_id` diferente do dono do remetente**,
  nenhum arquivo fora da pasta da unidade e todo ack com linha no banco.

**Web (etapa 3)**
- Duas requisições concorrentes conflitantes (dois `Session`s ou duas threads):
  apenas uma é gravada.

**Homologação manual**
- Dois PACS reais ou `fake_pacs.py` com AETs diferentes, C-MOVE de cada unidade e
  conferência das pastas, dos pedidos e do envio para a nuvem certa.

## 9. Decisões em aberto

- **D1.** Permitir AETs chamados diferentes na mesma porta? Recomendação: **sim**,
  porque sai de graça com a chave proposta.
- **D2.** Exigir "IPs autorizados" quando a porta é compartilhada? Recomendação:
  **exigir**. O AET sozinho é fácil de falsificar e o IP dos PACS é conhecido.
- **D3.** Abortar associações em andamento quando a unidade é pausada, arquivada ou
  tem a rota alterada (C4)? Recomendação: **sim**. Isso muda o comportamento atual
  também para unidades sem porta compartilhada.
- **D4.** Reconcile imediato via `LISTEN/NOTIFY` (C7)? Recomendação: **depois**, numa
  fase separada.

## 10. Implementação

Decisões tomadas: D1 sim, D2 **não exigir IP**, D3 sim, D4 implementado por último.

| Peça | Onde |
|---|---|
| Regras puras (normalização, plano de roteamento, conflitos de cadastro) | `app/store_routing.py` |
| Listener por porta, tabela imutável, rota fixada na associação, abort, limite por unidade | `app/receiver.py` (`RoutingTable`, `Receiver._admit`, `Receiver.reconcile`) |
| Trigger `units_changed` + `LISTEN` no receiver (D4) | `app/db.py` (`ensure_units_changed_trigger`), `app/receiver.py` (`UnitChangeListener`) |
| Validação com `pg_advisory_xact_lock` | `app/routes/units.py` (`_store_route_problems`) |
| Formulário e dashboard | `units_form.html`, `dashboard_partial.html`, `app/routes/dashboard.py` |
| Testes | `tests/test_store_routing.py`, `tests/test_receiver_shared_port.py`, `tests/test_store_routing_web.py` |

Diferenças em relação ao plano:

- **Pastas da mesma unidade.** A regra 3.2.4 vale só **entre unidades**. Pastas
  iguais dentro da mesma unidade continuam aceitas, como antes, porque não misturam
  empresas.
- **Todas as unidades de uma porta bloqueadas.** Se nenhuma unidade da porta pode
  receber (pausada ou com conflito), o listener fecha e a conexão é recusada no TCP,
  em vez de receber A-ASSOCIATE-RJ.
- **Unidade bloqueada por conflito.** As unidades envolvidas num conflito de pasta,
  ou num remetente reivindicado duas vezes, ficam inteiras sem receber
  (`UnitUnavailable`, rejeição transitória). Uma unidade com lista vazia num grupo
  compartilhado só bloqueia a si mesma; os remetentes das outras continuam
  roteados.
- **Ordem do log de rejeição.** O log de rejeição é gravado **antes** do
  A-ASSOCIATE-RJ, porque o remetente pode ver a rejeição antes de `kill()` retornar.

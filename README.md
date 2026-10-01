# Retrieve Manager

Programa único (tela no navegador + worker + receptor DICOM) que substitui os scripts bash/python de retrieve, storescp, compactação e envio à nuvem.

Cada hospital vira uma **unidade** cadastrada na tela. A lista começa vazia.

HTTP 200 da API de envio confirma gravação no storage; o arquivo local só é
excluído depois de persistir `uploaded` no banco.

## O que ele faz

1. Consulta a API Pedido PLERES de cada unidade a cada 30 segundos
2. Filtra pedidos ainda não lidos e, quando configurado, pelo ID Posto
3. Grava cada pedido na fila local e confirma `mirthReaded=true` junto com o `empresa_id` cadastrado na unidade
4. Faz C-FIND no PACS (Accession + data de nascimento)
5. Opcionalmente busca os exames dos últimos três anos do mesmo paciente e modalidade
6. Espera 15 min (CT/MR) ou 10 min (resto) e faz o C-MOVE do exame atual pelo Study UID
7. CT/MR têm um 2º C-MOVE ~90 min depois, incremental: um C-FIND por série
   compara `NumberOfSeriesRelatedInstances` com o que já foi recebido e pede só as
   séries incompletas (ou nenhuma). Se o PACS não informar as contagens, pede o
   estudo inteiro. Imagens perdidas ou com erro contam como faltantes.
8. Recebe os exames no receptor DICOM próprio (pynetdicom) na porta/AET da unidade
9. Associa exames recebidos diretamente ao pedido/histórico correto ou cria um
   pedido idempotente pelo Study UID quando ainda não há pedido
10. Grava o token da unidade no DICOM (`0008,1040`), compacta em JPEG 2000 (pydicom, em processos isolados) e envia para o endpoint de nuvem configurado na própria unidade

`storescp` do host antigo, `compacta_retrieve.py` e `envio_ret.py` **não precisam mais rodar**. Compacta/envio da nuvem que já existiam fora deste fluxo continuam independentes só se você quiser — este programa cobre a cadeia de retrieve.

O PACS precisa conhecer o **Calling AET** da unidade como destino de store e o IP
deste servidor. O mesmo AET é usado no C-FIND, no C-MOVE (como destino) e pelo
receptor.

O receptor (`python -m app.receiver`, serviço `receiver` do Compose) grava cada
objeto exatamente como chegou, sem decodificar o pixel data, calcula o SHA-256 e
registra a instância na tabela `dicom_instances`. O PACS só recebe sucesso depois
desse commit; falha de banco ou disco com menos de `RECEIVER_MIN_FREE_MB` livres
responde `0xA700` para o PACS reenviar. O mesmo SOP com o mesmo conteúdo (2º
retrieve, reenvio do PACS) é confirmado sem ser compactado ou enviado de novo; o
mesmo SOP com conteúdo diferente é guardado na pasta de erro como `conflict`, sem
substituir a primeira versão. A compactação consome essa tabela em vez de varrer
a pasta de recebimento.

O campo **Empresa ID**, na seção Pedidos PLERES do cadastro da unidade, é
obrigatório. Todos os PUTs de confirmação enviam `mirthReaded` e `empresa_id` no
corpo JSON, com o Token de Integração da respectiva unidade no header `token`.
O Token da unidade usado no DICOM é uma configuração separada.

Valores conhecidos: São Cristóvão (posto 25), `1582`; CDB (posto 48), `4232`.
Enquanto o campo estiver vazio ou inválido, os pedidos continuam sendo lidos e
persistidos, mas seus PUTs ficam bloqueados e pendentes de confirmação. A lista
de unidades sinaliza o cadastro incompleto.

## Subir no Linux

```bash
cd retrieve-manager
cp .env.example .env
# edite SECRET_KEY, RETRIEVE_ADMIN_PASSWORD e POSTGRES_PASSWORD (obrigatória)
# garanta leitura/escrita das pastas montadas para o UID/GID 10001
docker compose up -d --build
```

O Compose inicia um PostgreSQL dedicado, usado em conjunto pelos serviços `web`
e `worker`. Os dados do banco ficam no volume `postgres-data`; o volume
`retrieve-data` continua reservado aos dados locais da aplicação. Não remova o
volume do PostgreSQL ao recriar os containers.

O PostgreSQL é o único banco suportado. A conexão é montada a partir de
`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER` e
`POSTGRES_PASSWORD` (ou `POSTGRES_PASSWORD_FILE`), as mesmas variáveis do
container do banco; a senha pode conter `@`, `:`, `/` ou qualquer caractere, sem
codificação. No Compose, `POSTGRES_HOST` já aponta para o serviço `postgres`.

### Servidor sem acesso ao Docker Hub

O erro `lookup registry-1.docker.io: i/o timeout` **não é do Dockerfile**. O host não resolve/alcança `registry-1.docker.io`, então não baixa `python:3.14-slim-bookworm`.

Em uma máquina **com internet**:

```bash
docker pull python:3.14-slim-bookworm
docker save python:3.14-slim-bookworm | gzip > python-3.14-slim-bookworm.tar.gz
```

No servidor:

```bash
gunzip -c python-3.14-slim-bookworm.tar.gz | docker load
cd /opt/retrieve-manager
COMPOSE_BAKE=false docker compose -p retrieve up -d --build
```

O aviso `buildx isn't installed` é só aviso; `COMPOSE_BAKE=false` desliga o Bake.

Se o `apt` e o `pip` também não saírem à internet, gere a imagem da aplicação
numa máquina com internet e leve-a pronta. Não há binários externos: tudo vem do
`requirements.txt` (wheels do pip) e dos pacotes `ca-certificates` e `tzdata`.

```bash
docker compose build
docker save retrieve-manager:latest postgres:18-bookworm | gzip > retrieve-images.tar.gz
```

No servidor:

```bash
gunzip -c retrieve-images.tar.gz | docker load
docker compose -p retrieve up -d --no-build
```

Configure um proxy reverso HTTPS para o serviço web na porta 8080 e defina
`PUBLIC_ORIGIN=https://seu-dominio` no `.env`. Preserve o header `Host` no proxy.
Abra a URL HTTPS configurada. O Compose publica a porta 8080 somente em
`127.0.0.1` (`APP_BIND_ADDRESS`), portanto o proxy deve rodar no próprio host.
Se o proxy estiver em outra máquina, informe o IP da interface interna em
`APP_BIND_ADDRESS` e restrinja a porta no firewall.
Para testes sem domínio, use a configuração HTTP por IP descrita abaixo.

O middleware limita cada IP a 100 requisições por minuto. No login, cinco falhas
em quinze minutos bloqueiam temporariamente novas tentativas daquele IP. Todas as
respostas incluem `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Reset`, `X-RateLimit-Window` e `X-RateLimit-Scope`; respostas `429`
também incluem `Retry-After`.

O endereço considerado é o cliente validado pelo Uvicorn. Atrás de proxy reverso,
configure `FORWARDED_ALLOW_IPS` com o IP ou a sub-rede **somente do proxy** para
que `X-Forwarded-For` seja aceito com segurança. Não use `*` se a porta 8080 puder
ser alcançada diretamente, pois clientes poderiam falsificar o IP e contornar o
limite.

O Compose atual executa um único processo web, portanto a janela em memória é
consistente nessa implantação. Se forem adicionados múltiplos workers ou réplicas,
o estado do rate limit deve ser movido para Redis ou aplicado no gateway/proxy.
Para ataques volumétricos, mantenha também limitação no proxy, WAF ou provedor de
borda; o middleware protege os recursos da aplicação, mas não substitui defesa de
rede.

Login inicial (se o banco estiver vazio):

- usuário: `admin` (ou `RETRIEVE_ADMIN_USER`)
- senha: o valor obrigatório de `RETRIEVE_ADMIN_PASSWORD`

Troque a senha clicando na conta exibida no rodapé do menu lateral. A troca
encerra as sessões abertas em outros navegadores; a redefinição feita por um
administrador ou pelo `reset-admin` encerra todas as sessões daquela conta.

`RETRIEVE_ADMIN_PASSWORD` inicializa o primeiro usuário, mas não sobrescreve uma
senha já armazenada. Para redefinir o acesso usando os valores atuais do `.env`:

```bash
docker compose run --rm --no-deps web python -m app.manage reset-admin
docker compose up -d web
```

O comando não imprime a senha e usa o mesmo volume de dados do serviço web. Se a
senha contiver `#`, `$`, espaços ou outros caracteres especiais, coloque o valor
entre aspas simples no `.env`.

O usuário inicial e o comando `reset-admin` sempre criam ou restauram uma conta
com perfil `admin`. Na aba **Usuários**, administradores podem criar outras contas
com um dos dois perfis:

- `admin`: acesso total à interface e a todas as alterações;
- `user`: acesso somente de leitura à Visão geral, Fila de pedidos e detalhes.

O sistema impede que um administrador altere a própria role ou exclua a própria
conta. Todos os usuários podem trocar a própria senha pelo rodapé do menu.

Portas publicadas: **8080** (tela) e **444** (receptor DICOM da primeira unidade).
Internamente, o listener usa a porta não privilegiada **10444**, conforme
`STORE_PORT_MAP=444:10444`; o cadastro e o PACS continuam usando **444**. Volume
`/mobilemed` é o mesmo das pastas cadastradas na unidade. Outra porta de store
deve entrar em `receiver.ports` e em `STORE_PORT_MAP` no `docker-compose.yml`.

O runtime executa como usuário não-root `10001:10001`, com filesystem raiz
somente leitura e capabilities removidas. A tradução Docker de 444 para 10444
evita conceder privilégio de bind ao receptor. Em bind mounts, ajuste previamente o
proprietário ou ACL de `/mobilemed` e `/opt/idr`.

Por padrão em produção, `SESSION_HTTPS_ONLY=true` e `PUBLIC_ORIGIN` com HTTPS são
obrigatórios; configurações inseguras impedem a inicialização. Para desenvolvimento
HTTP local, use `APP_ENV=development`, `SESSION_HTTPS_ONLY=false` e remova
`PUBLIC_ORIGIN` ou configure a origem local exata.

### Testes via HTTP pelo IP da máquina

No `.env`, configure:

```dotenv
ALLOW_HTTP_FOR_TESTS=true
SESSION_HTTPS_ONLY=false
PUBLIC_ORIGIN=
APP_BIND_ADDRESS=0.0.0.0
```

Recrie os serviços com `docker compose up -d --build` e abra
`http://IP-DA-MAQUINA:8080` (ou a porta definida em `APP_PORT`). A opção funciona
também no Docker Compose com `APP_ENV=production`. Com `PUBLIC_ORIGIN` vazio,
a validação compara a origem da requisição com o IP e a porta usados no acesso.
Se preferir fixar um endereço, use `PUBLIC_ORIGIN=http://192.168.1.100:8080`,
substituindo pelo IP real. CSRF, validações e exigência de senhas fortes continuam
ativos. HTTP transmite a sessão sem criptografia; use essa opção no ambiente de testes.

Ao habilitar HTTPS, volte para `ALLOW_HTTP_FOR_TESTS=false`,
`SESSION_HTTPS_ONLY=true` e configure `PUBLIC_ORIGIN=https://seu-dominio`.

Todos os POSTs exigem token CSRF da sessão, enviado no campo `csrf_token` ou
no header `X-CSRF-Token`. Clientes devem obter o formulário primeiro e preservar
o cookie de sessão. Recarregue formulários abertos antes desta atualização.

Saúde e logs:

- `GET /health/live`: processo web vivo.
- `GET /health`: readiness com consulta real ao PostgreSQL.
- O worker possui healthcheck por heartbeat atualizado a cada ciclo.
- `docker compose logs -f web worker`: eventos JSON com `correlation_id`.

Rede DICOM (C-ECHO, C-FIND, C-MOVE e o receptor) em **pynetdicom 3.0.4** e
compactação JPEG 2000 em **pydicom + pylibjpeg-openjpeg** — sem DCMTK, sem
dcm4che e sem binário no host. A imagem usa Python 3.14, pydicom 3.0.2 e aiohttp 3.14.3.

## Desenvolvimento (Windows / sem Docker)

Suba um PostgreSQL local (por exemplo `docker compose up -d postgres`, ou um
container avulso) e informe a senha dele:

```powershell
cd Documents\retrieve-manager
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
$env:POSTGRES_PASSWORD = "<senha>"
.\.venv\Scripts\python -m uvicorn app.main:app --reload --port 8080
```

Worker (outro terminal):

```powershell
.\.venv\Scripts\python -m app.worker
```

Receptor DICOM (outro terminal):

```powershell
.\.venv\Scripts\python -m app.receiver
```

A compactação não depende de binários externos: basta o `requirements.txt`.

Testes (sessões, CSRF, headers HTTP, receptor, PACS de teste): usam as mesmas
variáveis `POSTGRES_*`, apontando para um PostgreSQL descartável cujo banco tenha
`test` no nome. Cada teste cria e apaga o próprio schema:

```powershell
.\.venv\Scripts\pip install -r requirements-dev.txt
docker run -d --name retrieve-pg-test -e POSTGRES_PASSWORD=<senha> -e POSTGRES_DB=retrieve_test -p 127.0.0.1:55432:5432 postgres:18
$env:APP_ENV = "development"
$env:SESSION_HTTPS_ONLY = "false"
$env:PUBLIC_ORIGIN = ""
$env:POSTGRES_PORT = "55432"
$env:POSTGRES_DB = "retrieve_test"
$env:POSTGRES_USER = "postgres"
$env:POSTGRES_PASSWORD = "<senha>"
.\.venv\Scripts\python -m unittest discover -s tests -q
docker rm -f retrieve-pg-test
```

O GitHub Actions (`.github/workflows/ci.yml`) roda o ruff, a suíte em PostgreSQL
18 e o build da imagem a cada push na `main` e em pull requests.

## Retenção e desempenho dos pedidos

Pedidos não são apagados fisicamente. A ação **Arquivar** remove o registro da
fila operacional e o mantém, com eventos e vínculos de arquivos, em **Histórico
de pedidos**. Pedidos sem resultado no C-FIND após 24 horas também são arquivados
automaticamente. Pedidos concluídos são arquivados 14 dias após a data de
conclusão, desde que não exista retrieve histórico pendente ou com erro. Unidades
removidas seguem a mesma regra de preservação.

A fila e os Logs mantêm paginação por cursor para evitar o custo crescente de
`OFFSET`, mas apresentam controles numerados com acesso à primeira, última,
próxima e anterior. Nessas telas é possível exibir 10, 20, 30, 40 ou 50 itens por
página; o padrão é 30. O PostgreSQL recebe índices parciais para registros ativos,
histórico e limpeza automática, além de índices `pg_trgm` para busca por accession,
Patient ID e ID de origem. Os agregados da Visão geral têm cache curto de cinco
segundos para que vários navegadores não repitam as mesmas varreduras do banco a
cada atualização.

Na compactação, cada arquivo é lido, alterado (charset, token, regras) e
codificado em um processo persistente e isolado; um crash ou travamento do
codec derruba só aquele processo. O número de processos é
`COMPACT_GLOBAL_WORKERS` e o tempo máximo por arquivo,
`COMPACT_FILE_TIMEOUT_SECONDS`. Um crash é repetido uma vez em processo novo;
se repetir, ou em timeout, o original vai íntegro para a pasta de erro.
Persistências usam lotes pequenos configurados por `COMPACT_DB_BATCH_SIZE` e
voltam automaticamente ao modo individual se houver conflito. O resumo
`dicom.compact.batch` registra `files_per_second`, `lossless_count`,
`lossy_count` e `copy_count`, permitindo ajustar os workers com base na
produção.

O envio lê somente a tabela `image_transfers`; a pasta de envio nunca é
varrida. Antes de um arquivo entrar na pasta de envio, a compactação grava o
registro com o nome e o SHA-256 do artefato (estado `publishing`); só depois de
movido ele passa a `compressed` e fica visível ao envio. Se o processo cair entre
os dois passos, a compactação seguinte confere o arquivo pelo hash e o libera,
ou recompacta a partir da origem intacta.

As imagens seguem em ordem de chegada (FIFO) num fluxo contínuo: cada unidade
mantém `send_workers` uploads em andamento e repõe cada um assim que termina, de
modo que um upload lento ocupa só a própria conexão. Só HTTP 200 é sucesso. Cada
falha é tentada de novo após 10 s, 30 s, 1, 2, 5 e 10 min (±20%; 30 min a partir
da 7ª, se o limite for maior); depois de
`SEND_MAX_ATTEMPTS` tentativas (padrão 7, cerca de 19 min) a imagem fica em
`send_error`. A cada 5 minutos uma única imagem em `send_error` da unidade é
testada (alternando entre elas); se o upload funcionar, todas voltam para a fila
e o envio continua, senão aguardam a próxima verificação. Também podem ser
reenviadas pelo botão **Reenviar** no pedido ou **Reenviar falhas de envio** na
unidade. Resultados são persistidos em lotes, com fallback individual, e a
concorrência total entre unidades é limitada por `SEND_GLOBAL_CONCURRENCY`. Cada
ciclo reutiliza a mesma sessão HTTP por até `SEND_DRAIN_SECONDS` (padrão 20 s) e
então devolve o controle ao agendador.

## Cadastrar uma unidade

Campos principais:

- API Pedido PLERES: URL e Token de Integração obrigatórios, ID Posto opcional
- Pastas de recebimento DICOM, envio e erro
- PACS: AET, IP, porta e a opção **Usar * no Patient ID no C-FIND** (desmarcada
  por padrão)
- Calling AET e Dest AET
- Porta do receptor (única no servidor) e, opcionalmente, os **IPs autorizados a
  enviar** e os **AE Titles autorizados a enviar** (vazios aceitam qualquer
  remetente)
- Token (identificação na nuvem)
- Endpoint de envio para a nuvem
- Retrieve de exames anteriores (opcional) e timeout, com padrão de 30 minutos

Depois de salvar, cadastre o Dest AET + porta **no PACS**.

**AE Titles autorizados a enviar** restringe quem pode entregar exames ao receptor
da unidade. A associação de outro Calling AET é recusada já na abertura
(A-ASSOCIATE-RJ, "calling AE title not recognized"), sem diferenciar maiúsculas;
um Called AET diferente do Calling AET da unidade também é recusado. O receptor
grava o AE de origem no meta header (`SourceApplicationEntityTitle`, 0002,0016) e
a compactação confere de novo antes de qualquer processamento: objetos de outros
AEs vão para a pasta de erro com status `rejected_sender`, sem criar pedido. O AE
pode ser falsificado por quem alcança a porta, então prefira restringir também o
IP. Com o campo vazio, qualquer remetente é aceito.

**IPs autorizados a enviar** aceita endereços e faixas CIDR separados por vírgula
(ex.: `192.168.3.103, 10.10.0.0/24`). Conexões de outro endereço são recusadas na
abertura da associação (A-ASSOCIATE-RJ), antes de qualquer imagem ou C-ECHO, e
registradas no log como `CallingAddressNotAllowed` com o IP de origem. Vazio (o
padrão) aceita qualquer IP. O filtro compara o IP que chega ao container: com a
porta publicada pelo Docker no Linux (NAT do iptables) é o IP real do PACS, mas
se houver proxy de porta (userland-proxy, Docker Desktop, balanceador) o receptor
pode ver o IP do gateway — confira o `peer_ip` no log da primeira associação
antes de restringir. O firewall do host continua sendo a primeira barreira.

Arquivos que aparecem na pasta de recebimento sem registro no banco (por
exemplo, devolvidos por **Reprocessar erros**) são adotados pelo
worker depois de `RECEIVE_ADOPT_MIN_AGE_SECONDS` (60 s) e seguem o mesmo fluxo;
uma instância que falhou na compactação volta para a fila quando o mesmo conteúdo
chega de novo.

O receptor mantém cada objeto em memória enquanto o grava: o pico de memória fica
perto de `RECEIVER_MAX_ASSOCIATIONS` × maior objeto recebido. Em unidades com
tomossíntese ou multiframe muito grande, reduza `RECEIVER_MAX_ASSOCIATIONS`.

## Recriar o banco de desenvolvimento

O banco é criado do zero na primeira inicialização (tabelas, índices, usuário
administrador, tempos de retrieve e a regra padrão **Descartar Study ID SLRX**).
Não há migração de bancos anteriores: ao trocar de versão durante o
desenvolvimento, descarte o banco ou o volume antigo.

No Docker, `docker compose down -v` remove o volume `retrieve-data`. No ambiente
local, remova `data/retrieve.db`. A próxima inicialização cria o schema novo e a
conta administrativa definida no `.env`.

O worker envia o Token de Integração no header `token`. O GET deve retornar um
array JSON e os campos `patientId`, `accessionNumber`, `patientBirthdate` e
`examDate` são obrigatórios. As datas PLERES no formato
`MM/DD/YYYY HH:MM:SS` são convertidas para `YYYYMMDD` antes do C-FIND.

Pedidos com `mirthReaded=true` são ignorados. O `PUT` de confirmação só ocorre
depois do commit no banco local; se falhar, fica pendente e é repetido sem criar
outro pedido para o mesmo accession. As confirmações são executadas em segundo
plano, com concorrência limitada e commit individual, portanto uma API lenta não
interrompe C-FIND, C-MOVE, compactação nem o processamento das outras unidades.

Antes de aceitar o resultado do C-FIND, o worker lê cada resposta do PACS
separadamente, sem confundir as chaves ecoadas na requisição. Se o accession
retornar mais de um Study UID, ou se o `PatientID`, a data de nascimento ou o
accession devolvidos divergirem do pedido, o pedido vai para **erro** com a causa
na mensagem e nenhum C-MOVE é agendado. Revise o caso e use **Reprocessar**
depois de corrigir a origem.

Quando o C-FIND encontra o estudo atual, o worker consulta suas séries e escolhe a
primeira modalidade aceita pelo catálogo de compactação da unidade, desconsiderando
modalidades descartadas como SR e PR. O `BodyPartExamined (0018,0015)` vem da mesma
série selecionada; se nenhuma série clínica válida existir, o pedido continua em
observação e a consulta é repetida. Quando o retrieve de exames anteriores está
ativo, o worker executa outro C-FIND em nível de série usando paciente, nascimento,
essa modalidade, body part e o intervalo entre três anos atrás e ontem. O Patient
ID é consultado exatamente como veio do PLERES e body part vazio é permitido.
Cada série devolvida precisa trazer o mesmo `PatientID` e a mesma data de
nascimento do pedido; as demais são ignoradas e contadas no evento do pedido.
Se o PACS da unidade grava o ID com sufixo (ex.: `12345-1`), marque **Usar * no
Patient ID no C-FIND**: a consulta passa a usar `12345*` e são aceitos IDs que
começam com o do pedido. Os Study UIDs encontrados são registrados antes da
transferência, e cada série é recuperada por seus Study UID e Series UID exatos.
Se a consulta não encontrar exames anteriores, o histórico termina com sucesso e
nenhum C-MOVE é executado. O processo compartilha o limite de paralelismo da
unidade, mas não bloqueia um retrieve atual que já esteja no horário e haja
capacidade disponível. Cada série histórica concluída recebe um checkpoint e não
é repetida se outra série precisar de retry. O exame atual continua sendo
recuperado pelo Study UID; seus tempos e eventual segundo retrieve não se aplicam
aos exames anteriores.

Quando um estudo chega diretamente ao Store SCP sem pedido correspondente, o
recebimento conta como o primeiro retrieve. O worker cria somente um pedido por
unidade e Study UID, preenche-o com as tags do DICOM e agenda apenas o segundo
retrieve, quando ele estiver habilitado na regra da modalidade. Modalidades sem
segundo retrieve terminam sem um novo C-MOVE. Antes de criar, o worker prioriza um
pedido atual pelo Study UID, depois um histórico ativo e por fim um pedido anterior
do mesmo Study UID — ou um pedido ainda sem UID com o mesmo accession. Pedidos
arquivados do mesmo estudo são restaurados.
Study UID, accession, Patient ID e nascimento são obrigatórios para a criação;
arquivos sem esses identificadores ficam no diretório de erro e não são enviados.

O fluxo operacional, os limites de falha e o catálogo de logs estão detalhados em
[`RETRIEVE_HARDENING.md`](RETRIEVE_HARDENING.md).

Na página do pedido, **Retrieve agora** cria uma solicitação persistente para um
C-MOVE adicional apenas do Study UID do exame atual. A solicitação só fica
disponível depois que o C-FIND encontra o estudo, respeita o limite de movimentos
paralelos da unidade e não substitui os horários normais do primeiro e do segundo
retrieve. O botão **Atualizar** recarrega o estado e os eventos da página.

## Regras padrão (editáveis na tela)

| Modalidade | 1º retrieve | 2º retrieve |
|---|---|---|
| CT | 15 min | 90 min |
| MR | 15 min | 90 min |
| * (demais) | 10 min | não |

Compactação e descarte são configurados separadamente em cada unidade. Os perfis
são JPEG 2000 Lossless e JPEG 2000 Lossy. No Lossy a taxa depende de
BitsStored: até 8 bits, 10:1; de 9 a 12 bits, 5:1; acima de 12 bits (ou
imagem paleta, ou falha do encoder) a imagem sai em Lossless. O SOP Instance
UID nunca muda; imagens lossy recebem `LossyImageCompression=01`, a taxa real
e o método `ISO_15444_1`. Seguem sem recompressão: objetos sem imagem (SR,
PDF, KOS), pixel data que já chega comprimido, imagens com menos de 32 pixels
de lado, arquivos acima de `COMPACT_MAX_ENCODE_BYTES` e imagens que o encoder não
consegue comprimir. Se o codec cair ou estourar o tempo duas vezes, uma última
tentativa grava a imagem sem recompressão; só se ela também falhar a origem vai
para a pasta de erro. Uma modalidade pode
pertencer a somente um perfil; modalidades sem perfil explícito usam Lossless.
O descarte tem precedência e remove a modalidade de qualquer perfil de
compactação.

Uma unidade nova começa com CR, DX, MG, OT e XA em Lossy, o descarte de PR, PS,
SG, SR, RA e US e a regra padrão **Descartar Study ID SLRX**; tudo pode ser
alterado no cadastro de cada unidade.

## Regras DICOM

A página **Regras** permite avaliar tags DICOM padrão antes da compactação. Cada
regra pode ser vinculada a uma ou mais unidades, recebe uma prioridade e combina
suas condições com **E** ou **OU**. As comparações não diferenciam maiúsculas de
minúsculas.

Operadores disponíveis: igual, diferente, existe, não existe, contém, começa com,
termina com e começa com seguido por números. As ações podem excluir a imagem,
substituir/preencher o valor de uma tag padrão ou remover uma tag. Regras com menor
prioridade numérica executam primeiro; uma exclusão encerra o processamento daquele
arquivo. As aplicações ficam associadas ao registro da imagem para auditoria.

Algumas tags podem ser usadas nas condições, mas nunca substituídas ou removidas
por regra, porque identificam o paciente, o exame ou a imagem, definem os pixels
ou são gerenciadas pelo sistema: `SpecificCharacterSet`, `SOPClassUID`,
`SOPInstanceUID`, `AccessionNumber`, o token da unidade (`0008,1040`),
`PatientName`, `PatientID`, `IssuerOfPatientID`, `PatientBirthDate`,
`StudyInstanceUID`, `SeriesInstanceUID`, `FrameOfReferenceUID` e os grupos
`0002` (meta), `0028` (módulo de pixel) e `7FE0` (Pixel Data). A tela não aceita
regras nessas tags e, por segurança, o worker também não executa uma regra que
as altere (ela aparece com o aviso **Ignorada: tag protegida**).

O charset declarado pelo equipamento é preservado. Apenas quando o arquivo não
declara `SpecificCharacterSet` (ou declara ASCII) o sistema grava `ISO_IR 100`,
para que acentos em Latin-1 sejam exibidos corretamente. Arquivos em UTF-8
(`ISO_IR 192`) seguem em UTF-8, sem substituir caracteres por `?`.

Na primeira inicialização após a atualização, o prefixo de Study ID configurado
anteriormente é convertido em uma regra para as unidades existentes. Para `SLRX`,
o comportamento preservado corresponde a Study IDs iniciados por `SLRX` e seguidos
por números. O descarte simples por modalidade fica no card **Compactação** do
cadastro de cada unidade.

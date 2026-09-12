# Retrieve Manager

Programa único (tela no navegador + worker) que substitui os scripts bash/python de retrieve, storescp, compactação e envio à nuvem.

Cada hospital vira uma **unidade** cadastrada na tela. A lista começa vazia.

## O que ele faz

1. Consulta a API Pedido PLERES de cada unidade a cada 30 segundos
2. Filtra pedidos ainda não lidos e, quando configurado, pelo ID Posto
3. Grava cada pedido na fila local e confirma `mirthReaded=true` na API
4. Faz C-FIND no PACS (Accession + data de nascimento)
5. Opcionalmente busca os exames dos últimos três anos do mesmo paciente e modalidade
6. Espera 15 min (CT/MR) ou 10 min (resto) e faz o C-MOVE do exame atual pelo Study UID
7. CT/MR têm um 2º C-MOVE ~90 min depois
8. Sobe `storescp` na porta/AET da unidade
9. Grava o token da unidade no DICOM (`0008,1040`), compacta com `dcmcjpeg` e envia para `https://idr.mobilemed.com.br/api/router/send-image`

`storescp` do host antigo, `compacta_retrieve.py` e `envio_ret.py` **não precisam mais rodar**. Compacta/envio da nuvem que já existiam fora deste fluxo continuam independentes só se você quiser — este programa cobre a cadeia de retrieve.

O PACS precisa conhecer o **Calling AET** da unidade como destino de store e o IP
deste servidor. O mesmo AET é usado pelo `findscu`, `movescu` e `storescp`.

## Subir no Linux

```bash
cd retrieve-manager
cp .env.example .env
# edite SECRET_KEY e RETRIEVE_ADMIN_PASSWORD (o container recusa valores fracos)
# garanta leitura/escrita das pastas montadas para o UID/GID 10001
docker compose up -d --build
```

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

Se o `apt` e o `pip` também não saírem à internet, o build ainda trava no próximo passo. Aí o DCMTK pode ir em `vendor/dcmtk-3.7.0-linux-x86_64.tar.bz2` (ver `vendor/README.md`); o Python e os pacotes pip precisam da imagem/base já carregada.

Configure um proxy reverso HTTPS para o serviço web na porta 8080 e defina
`PUBLIC_ORIGIN=https://seu-dominio` no `.env`. Preserve o header `Host` no proxy.
Abra a URL HTTPS configurada; a porta 8080 deve ficar acessível apenas ao proxy.
Para testes sem domínio, use a configuração HTTP por IP descrita abaixo.

Login inicial (se o banco estiver vazio):

- usuário: `admin` (ou `RETRIEVE_ADMIN_USER`)
- senha: o valor obrigatório de `RETRIEVE_ADMIN_PASSWORD`

Troque a senha em **Nuvem e login**.

`RETRIEVE_ADMIN_PASSWORD` inicializa o primeiro usuário, mas não sobrescreve uma
senha já armazenada. Para redefinir o acesso usando os valores atuais do `.env`:

```bash
docker compose run --rm --no-deps web python -m app.manage reset-admin
docker compose up -d web
```

O comando não imprime a senha e usa o mesmo volume de dados do serviço web. Se a
senha contiver `#`, `$`, espaços ou outros caracteres especiais, coloque o valor
entre aspas simples no `.env`.

Portas publicadas: **8080** (tela) e **444** (`storescp` da primeira unidade).
Internamente, o listener usa a porta não privilegiada **10444**, conforme
`STORE_PORT_MAP=444:10444`; o cadastro e o PACS continuam usando **444**. Volume
`/mobilemed` é o mesmo das pastas cadastradas na unidade. Outra porta de store
deve entrar em `worker.ports` e em `STORE_PORT_MAP` no `docker-compose.yml`.

O runtime executa como usuário não-root `10001:10001`, com filesystem raiz
somente leitura e capabilities removidas. A tradução Docker de 444 para 10444
evita conceder privilégio de bind ao worker. Em bind mounts, ajuste previamente o
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
- `GET /health`: readiness com consulta real ao SQLite.
- O worker possui healthcheck por heartbeat atualizado a cada ciclo.
- `docker compose logs -f web worker`: eventos JSON com `correlation_id`.

DICOM no container: **DCMTK 3.7.0** (`findscu`, `movescu`, `storescp`, `dcmcjpeg`) — sem dcm4che e sem binário no host. A imagem usa Python 3.14, pydicom 3.0.2 e aiohttp 3.14.3.

## Desenvolvimento (Windows / sem Docker)

```powershell
cd Documents\retrieve-manager
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
.\.venv\Scripts\python -m uvicorn app.main:app --reload --port 8080
```

Worker (outro terminal):

```powershell
.\.venv\Scripts\python -m app.worker
```

Testes (incluindo sessões, CSRF e headers HTTP, com banco isolado):

```powershell
.\.venv\Scripts\pip install -r requirements-dev.txt
$env:APP_ENV = "development"
$env:SESSION_HTTPS_ONLY = "false"
$env:PUBLIC_ORIGIN = ""
$env:DATABASE_URL = "sqlite:///:memory:"
.\.venv\Scripts\python -m unittest discover -s tests -q
```

Sem os binários DCMTK no PATH o worker registra erro no pedido e a tela continua utilizável para cadastro. Em produção use o Docker, que já traz o DCMTK 3.7.0.

## Cadastrar uma unidade

Campos principais:

- API Pedido PLERES: URL e Token de Integração obrigatórios, ID Posto opcional
- Pastas de recebimento DICOM, envio e erro
- PACS: AET, IP, porta
- Calling AET e Dest AET
- Porta do storescp (única no servidor)
- Token (identificação na nuvem)
- Retrieve de exames anteriores (opcional) e timeout, com padrão de 30 minutos

Depois de salvar, cadastre o Dest AET + porta **no PACS**.

O worker envia o Token de Integração no header `token`. O GET deve retornar um
array JSON e os campos `patientId`, `accessionNumber`, `patientBirthdate` e
`examDate` são obrigatórios. As datas PLERES no formato
`MM/DD/YYYY HH:MM:SS` são convertidas para `YYYYMMDD` antes do C-FIND.

Pedidos com `mirthReaded=true` são ignorados. O `PUT` de confirmação só ocorre
depois do commit no banco local; se falhar, fica pendente e é repetido sem criar
outro pedido para o mesmo accession.

Quando o retrieve de exames anteriores está ativo, o C-FIND também obtém
`BodyPartExamined (0018,0015)`. Assim que encontra o exame atual, o worker agenda
um C-MOVE em nível de série usando paciente, nascimento, modalidade, body part e
o intervalo entre três anos atrás e ontem. O Patient ID é consultado com `*` no
final e body part vazio é permitido. Esse movimento compartilha o limite de
paralelismo da unidade e termina (ou esgota três tentativas) antes do primeiro
retrieve do exame atual. O exame atual continua sendo recuperado pelo Study UID;
seus tempos e eventual segundo retrieve não se aplicam aos exames anteriores.

## Regras padrão (editáveis na tela)

| Modalidade | 1º retrieve | 2º retrieve |
|---|---|---|
| CT | 15 min | 90 min |
| MR | 15 min | 90 min |
| * (demais) | 10 min | não |

Descarte no recebimento por modalidade (editável): PR, PS, SG, SR, RA, US.

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

Na primeira inicialização após a atualização, o prefixo de Study ID configurado
anteriormente é convertido em uma regra para as unidades existentes. Para `SLRX`,
o comportamento preservado corresponde a Study IDs iniciados por `SLRX` e seguidos
por números. O descarte simples por modalidade permanece em **Compactação**.

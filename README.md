# Retrieve Manager

Programa único (tela no navegador + worker) que substitui os scripts bash/python de retrieve, storescp, compactação e envio à nuvem.

Cada hospital vira uma **unidade** cadastrada na tela. A lista começa vazia.

## O que ele faz

1. Lê o arquivo de pedido `pat_id:acc:nasc:date_exam:tempo`
2. C-FIND no PACS (Accession + data de nascimento)
3. Espera 15 min (CT/MR) ou 10 min (resto) e faz C-MOVE
4. CT/MR têm um 2º C-MOVE ~90 min depois
5. Sobe `storescp` na porta/AET da unidade
6. Grava o token da unidade no DICOM (`0008,1040`), compacta com `dcmcjpeg` e envia para `https://idr.mobilemed.com.br/api/router/send-image`

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

O erro `lookup registry-1.docker.io: i/o timeout` **não é do Dockerfile**. O host não resolve/alcança `registry-1.docker.io`, então não baixa `python:3.12-slim-bookworm`.

Em uma máquina **com internet**:

```bash
docker pull python:3.12-slim-bookworm
docker save python:3.12-slim-bookworm | gzip > python-3.12-slim-bookworm.tar.gz
```

No servidor:

```bash
gunzip -c python-3.12-slim-bookworm.tar.gz | docker load
cd /opt/retrieve-manager
COMPOSE_BAKE=false docker compose -p retrieve up -d --build
```

O aviso `buildx isn't installed` é só aviso; `COMPOSE_BAKE=false` desliga o Bake.

Se o `apt` e o `pip` também não saírem à internet, o build ainda trava no próximo passo. Aí o DCMTK pode ir em `vendor/dcmtk-3.7.0-linux-x86_64.tar.bz2` (ver `vendor/README.md`); o Python e os pacotes pip precisam da imagem/base já carregada.

Abra `http://SERVIDOR:8080`

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

Se o painel estiver atrás de HTTPS, configure `SESSION_HTTPS_ONLY=true`. Em
acesso HTTP direto mantenha `false`, caso contrário o navegador não enviará o
cookie de sessão.

Saúde e logs:

- `GET /health/live`: processo web vivo.
- `GET /health`: readiness com consulta real ao SQLite.
- O worker possui healthcheck por heartbeat atualizado a cada ciclo.
- `docker compose logs -f web worker`: eventos JSON com `correlation_id`.

DICOM no container: **DCMTK 3.7.0** (`findscu`, `movescu`, `storescp`, `dcmcjpeg`) — sem dcm4che e sem binário no host. Python na imagem: pydicom 3.0.2 e aiohttp 3.14.3.

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

Sem os binários DCMTK no PATH o worker registra erro no pedido e a tela continua utilizável para cadastro. Em produção use o Docker, que já traz o DCMTK 3.7.0.

## Cadastrar uma unidade

Campos principais:

- Pastas de pedidos, sent, recebimento, envio e erro
- PACS: AET, IP, porta
- Calling AET e Dest AET
- Porta do storescp (única no servidor)
- Token (identificação na nuvem)

Depois de salvar, cadastre o Dest AET + porta **no PACS**.

## Regras padrão (editáveis na tela)

| Modalidade | 1º retrieve | 2º retrieve |
|---|---|---|
| CT | 15 min | 90 min |
| MR | 15 min | 90 min |
| * (demais) | 10 min | não |

Descarte no recebimento (editável): PR, PS, SG, SR, RA, US. StudyID `SLRX…` também é apagado.

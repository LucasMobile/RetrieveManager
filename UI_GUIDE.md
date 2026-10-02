# Sistema visual — Retrieve Manager

## Premissas

- O frontend permanece server-side com FastAPI, Jinja, HTML semântico, CSS próprio e JavaScript mínimo.
- As rotas, nomes de campos e regras de negócio existentes foram preservados.
- Não há framework CSS nem biblioteca de ícones no projeto; por isso, os componentes são locais e os ícones usam um único macro SVG.
- Nenhuma fonte, imagem ou script depende de rede externa, mantendo a interface funcional no container e em redes restritas.
- Há dois temas, claro e escuro. O escolhido fica em `localStorage` (`rm-theme`) e é aplicado por `app/static/theme.js` no `<head>`, antes da pintura, para não piscar o tema errado.

## Direção de design

1. A interface assume o tom de uma central de operação clínica: precisa, estável e silenciosa.
2. Azul-marinho profundo identifica navegação e marca sem competir com os dados.
3. Neutros frios organizam fundo, superfície e divisores; branco é reservado ao conteúdo acionável.
4. Verde, âmbar, vermelho e azul aparecem apenas para comunicar estados operacionais; cinza indica o que está inativo.
5. A hierarquia usa tamanho, peso e espaço antes de recorrer a cor ou decoração.
6. O ritmo parte de múltiplos de 4 px, com blocos de 16, 24, 32 e 40 px.
7. Bordas finas definem grupos; sombras são reservadas a elementos elevados e feedback transitório.
8. A linha vertical de status nas unidades é a assinatura visual do produto e facilita varredura rápida.
9. Textos são curtos, diretos e escritos em português do Brasil para quem opera o fluxo.
10. Todos os controles têm foco visível, altura mínima de 44 px e estados que não dependem apenas de cor.
11. Todo componente funciona nos dois temas usando apenas tokens; nenhum valor de cor fixo em componente.

## Design tokens

Os tokens ficam em `app/static/app.css`, dentro de `:root`. O tema escuro redefine os mesmos nomes em `html[data-theme="dark"]`; componentes nunca testam o tema, só consomem tokens. Os valores abaixo são do tema claro.

### Cor

| Grupo | Token | Valor / uso |
| --- | --- | --- |
| Marca | `--color-brand-900` | `#071b33`, sidebar e contexto de login |
| Marca profunda | `--color-brand-950` | `#051426`, logs e texto sobre primária no tema escuro |
| Primária | `--color-primary-700` | `#176fbf`, ações, links e status em andamento |
| Primária forte | `--color-primary-800` | `#105b9d`, texto azul sobre `--color-primary-100` |
| Primária suave | `--color-primary-100` | `#d9ecfb`, seleção e apoio |
| Texto | `--color-neutral-950` | `#10243b`, títulos e conteúdo principal |
| Texto secundário | `--color-neutral-600` | `#576b82`, descrições e metadados (≥ 4.5:1 em todas as superfícies) |
| Decorativo | `--color-neutral-500` | `#7c8fa3`, ícones, bordas de hover e placeholder; nunca texto |
| Fundo | `--color-neutral-50` | `#f4f8fc`, plano de fundo da aplicação |
| Borda | `--color-neutral-200` | `#cfe3f5`, separação estrutural |
| Superfície | `--color-surface`, `--color-surface-raised`, `--color-surface-subtle` | cards, elementos elevados e áreas de apoio |
| Sucesso | `--color-success-700/100` | ativo, concluído e disponível |
| Alerta | `--color-warning-700/100` | espera e atenção |
| Erro | `--color-danger-700/100` | falha e ação destrutiva |
| Informação | `--color-info-700/100` | aguardando e estado informativo |
| Inativo | `--color-idle-700/100` | `#4d5862` / `#eceff1`, cancelado, arquivado e desativado |

### Bordas semânticas

Contornos de componentes com estado usam tokens derivados da cor do estado misturada à superfície, nunca um tom pastel fixo. Assim a borda fica discreta nos dois temas.

| Token | Uso |
| --- | --- |
| `--border-success`, `--border-warning`, `--border-danger`, `--border-info`, `--border-primary` | 30% da cor do estado sobre a superfície: toasts, selos, badges de imagem, área de atenção, hover de card |
| `--border-warning-strong`, `--border-danger-strong` | 55%: hover de botões de alerta e de perigo |

### Tipografia, espaço e forma

| Grupo | Token | Valor / uso |
| --- | --- | --- |
| Fonte | `--font-sans` | Inter quando instalada, Segoe UI e fontes do sistema como fallback |
| Mono | `--font-mono` | valores de máquina comparados caractere a caractere: UIDs, AETs, IPs e portas, tags DICOM, caminhos, IDs e logs. Não use em URLs nem em texto corrido; placeholders de campos `.mono` ficam na fonte normal |
| Texto mínimo | `--text-min` | 12 px; nenhum texto menor que isso |
| Controle | `--control-h` | 44 px; altura de inputs, selects, botões (inclusive `.btn--sm`), botões de ícone e itens de menu |
| Espaço | `--space-1` a `--space-16` | escala de 4 a 64 px |
| Raios | `--radius-sm/md/lg` | 3, 4 e 6 px |

### Elevação e foco

| Token | Uso |
| --- | --- |
| `--shadow-xs` | cards; quase imperceptível |
| `--shadow-sm` | card de unidade em repouso |
| `--shadow-hover` | hover de cards e KPIs |
| `--shadow-raised` | barra de ação persistente do formulário |
| `--shadow-popover` | modal, menus e skip link; sempre com `--color-surface-raised` e borda de 1px `--color-neutral-200`, porque no tema escuro a sombra sozinha não separa o elemento do fundo |
| `--focus-ring` | halo azul do foco de campos |

As sombras têm valores próprios no tema escuro (base preta), porque a sombra azulada do claro some em fundo escuro.

## Inventário das telas

| Tela | Template | Mudança principal |
| --- | --- | --- |
| Login | `login.html` | Composição em dois planos, labels explícitas, revelar senha, erro acessível e logo por tema |
| Visão geral | `dashboard.html`, `dashboard_partial.html` | Saúde da operação, KPIs, cards de unidade com linha de estado e atualização com idade real |
| Fila de pedidos e histórico | `orders.html`, `orders_summary.html`, `orders_table.html`, `orders_partial.html` | Filtros alinhados, tabela consistente, ações por ícone com tooltip, vazio contextual e auto-refresh de 10 s na fila |
| Detalhe do pedido | `order_detail.html` | Dados e timeline em duas colunas, IDs monoespaçados, ações com rótulo e menu "Mais ações" |
| Instâncias com pendência | `instances.html` | Objetos DICOM fora do fluxo normal, com filtro por unidade e situação |
| Unidades | `units_list.html` | Lista limpa, estado legível, pausar e arquivar por linha |
| Cadastro de unidade | `units_form.html` | Oito seções numeradas com índice lateral, barra de ação persistente e área de atenção |
| Regras DICOM | `rules_dicom.html` | Regras por condição de tag DICOM, prioridade e unidades, com editor em seções |
| Tempo de retrieve | `rules_retrieve.html` | Regras por modalidade editáveis em tabela, com labels recuperadas no mobile |
| Usuários e senha | `users.html`, `user_form.html`, `account_password.html` | Perfis e permissões em tabela; troca de senha em formulário curto |
| Logs | `logs.html` | Auditoria filtrável por recurso, ação e texto |
| Erros HTTP | `error.html` | Página 403/404/405/422/500 consistente, com ID da solicitação e logo por tema |

## Telas e componentes importantes

### Estrutura global e navegação

- **Problema atual:** links sem ícones, pouca distinção de grupos e sidebar ocupando a tela inteira no mobile.
- **Decisão de design:** navegação escura agrupada por tarefa, item ativo com linha lateral e menu móvel com overlay e suporte a Escape.
- **Resultado (código):** `app/templates/base.html`, classes `.app-shell`, `.sidebar`, `.nav__item` e controles `data-sidebar-*`.
- **Por que ficou mais profissional:** mantém contexto, reduz ruído e apresenta uma hierarquia previsível em qualquer viewport.

### Login

- **Problema atual:** card isolado com aparência genérica e pouca orientação de contexto.
- **Decisão de design:** separar identidade do produto e tarefa de autenticação, mantendo o formulário como foco principal.
- **Resultado (código):** `app/templates/login.html`, `.login-shell`, `.login-visual`, `.login-panel` e `data-password-toggle`.
- **Por que ficou mais profissional:** transmite segurança e maturidade sem adicionar decoração gratuita.

### Visão geral e card de unidade

- **Problema atual:** status, métricas, caminhos e ações competiam entre si dentro do mesmo card.
- **Decisão de design:** organizar leitura em nome/status, quatro métricas, contagem de pastas e ações; a borda lateral resume a saúde operacional. "Ver fila" é secundário: a visão geral não tem um botão primário por card. Cada informação aparece uma vez: o estado do store fica no selo e os AETs na linha abaixo do nome, sem repetição no rodapé.
- **Resultado (código):** `dashboard.html`, `dashboard_partial.html`, `.unit-card`, `.metric` e `.poll-region`.
- **Por que ficou mais profissional:** o operador identifica exceções antes de ler detalhes e o polling não interrompe controles em foco.

### Atualização automática

- **Decisão de design:** a região com `data-poll` (URL) e `data-interval` (ms) é recarregada em segundo plano e mostra o `.live-indicator` ("Atualização automática a cada N segundos"). Telas sem `data-poll` não exibem indicador de atualização automática; nelas, o botão "Atualizar" é o mecanismo. O texto com `data-age` mostra a idade real do último conteúdo recebido ("Verificado há 40 s"). Passado 3× o intervalo, a região ganha `.is-stale` e os indicadores ficam em âmbar. O polling pausa com foco dentro da região, com a aba oculta ou com um `<dialog>` aberto (a confirmação aponta para um formulário da região), e recarrega ao voltar para a aba. Se a sessão expirar, a resposta redirecionada para o login é descartada e a região mostra o aviso de erro.
- **Troca parcial:** quando a região contém elementos com `data-poll-slot="nome"`, só esses blocos são substituídos pelos de mesmo nome na resposta; o resto (por exemplo, o formulário de filtros da fila) nunca é tocado. Sem slots, todo o conteúdo é trocado. A rota responde ao cabeçalho `X-Partial: 1` com o template parcial.
- **Onde há polling:** visão geral (5 s) e fila de pedidos (10 s, `ORDERS_POLL_SECONDS` em `app/routes/orders.py`). O histórico não tem, porque não muda sozinho.
- **Resultado (código):** `app/static/app.js` (`refreshPartial`, `renderPollAge`) e `.poll-region`.
- **Regra:** nunca escreva "agora" fixo em conteúdo atualizado por polling; use `data-age`.

### Filtros, tabela e paginação

- **Problema atual:** larguras inline, labels genéricas, ações com o mesmo peso e paginação pouco semântica.
- **Decisão de design:** um filtro responsivo, tabela com caption, cabeçalhos com `scope`, ações por ícone com tooltip nas linhas e paginação navegável.
- **Resultado (código):** `orders.html`, `units_list.html`, `pager.html`, `.filter-grid`, `.data-table` e `.pager`.
- **Por que ficou mais profissional:** cria um padrão único de página de dados e melhora leitura por teclado e tecnologia assistiva.

### Detalhe do pedido e timeline

- **Problema atual:** metadados extensos e logs disputavam espaço, dificultando localizar status e erro; as ações do pedido eram só ícones.
- **Decisão de design:** identificação à esquerda, histórico à direita e conteúdo técnico recolhido por padrão. Ações operacionais com rótulo visível ("Retrieve agora", "Verificar novas imagens agora", "Reprocessar"); ações que encerram ou removem ("Encerrar monitoramento", "Cancelar pedido", "Arquivar pedido") ficam no menu "Mais ações". Quando uma ação está indisponível, o motivo aparece como texto.
- **Resultado (código):** `order_detail.html`, `.order-data-list`, `.timeline`, `.log-box`, `.order-detail-actions__ops` e `.action-menu`.
- **Por que ficou mais profissional:** prioriza decisão operacional, evita clique errado por ícone ambíguo e conserva detalhe técnico sob demanda.

### Formulário de unidade

- **Problema atual:** formulário longo, campos sem `id/for`, pouco contexto e ação de salvar distante.
- **Decisão de design:** oito seções numeradas — identificação, conexão PACS, leitura de pedidos PLERES, exames anteriores, Store SCP e diretórios, compactação, envio para a nuvem e limites de processamento — com índice lateral que destaca a seção atual, helpers, limites HTML, barra de ação persistente e área de atenção separada. Não há "Voltar" no cabeçalho: o breadcrumb e o "Cancelar" da barra já cobrem a saída.
- **Resultado (código):** `units_form.html`, `.unit-form-nav` com `data-section-nav` (o JS marca o link atual com `aria-current="location"`), `.form-section`, `.form-grid`, `.field__hint`, `.form-actions` e `.danger-zone`.
- **Por que ficou mais profissional:** reduz carga cognitiva, previne erros de preenchimento e separa claramente manutenção de arquivamento.

### Regras DICOM e tempo de retrieve

- **Problema atual:** formulários dentro de células, estilos inline e opções técnicas digitadas livremente.
- **Decisão de design:** regras DICOM como cards com condição → ação e editor em seções; tempo de retrieve em tabela editável com switch de monitoramento.
- **Resultado (código):** `rules_dicom.html` (`.dicom-rule-card`, `.dicom-rule-form`) e `rules_retrieve.html` (`.retrieve-rules-table`, `.retrieve-switch`).
- **Por que ficou mais profissional:** mantém alta densidade no desktop e recupera labels explícitas no mobile.

### Feedback, loading e confirmação

- **Problema atual:** flashes sem ação de fechar, polling silencioso e `confirm()` nativo inconsistente.
- **Decisão de design:** toast semântico, spinner único em submit/polling e `<dialog>` para ações que encerram, cancelam, arquivam ou excluem.
- **Resultado (código):** `base.html`, `app/static/app.js`, `.toast`, `.is-loading` (aplicada pelo JS) e `.confirm-dialog`.
- **Por que ficou mais profissional:** cada ação informa estado, evita duplo envio e reserva fricção às ações que encerram algo.

### Telas de erro

- **Problema atual:** erros do framework apareciam como JSON fora do contexto visual do produto.
- **Decisão de design:** página concisa, ação de recuperação e request ID para suporte; respostas não HTML continuam em JSON.
- **Resultado (código):** `error.html` e handlers seletivos em `app/main.py`.
- **Por que ficou mais profissional:** mantém confiança em falhas e torna o atendimento rastreável sem expor detalhe interno.

## Guia rápido de componentes

### Cabeçalho de página

Use sempre `page-header`, com eyebrow **ou** breadcrumb (nunca os dois), `h1`, descrição curta e uma única ação primária em `page-header__actions`. O eyebrow repete o grupo da sidebar onde a tela está ("Operação" ou "Configurações"), e o `h1` repete o rótulo do item da sidebar. Se a lista estiver vazia e o estado vazio já oferecer a ação, a primária do cabeçalho é omitida.

```html
<header class="page-header">
  <div class="page-header__copy">
    <p class="eyebrow">Cadastro</p>
    <h1>Título</h1>
    <p class="page-header__description">Descrição objetiva.</p>
  </div>
  <div class="page-header__actions">...</div>
</header>
```

### Botões

- `.btn--primary`: uma ação principal por contexto. Não repita primária em cada item de uma lista ou grade.
- `.btn--secondary`: ação neutra com borda.
- `.btn--quiet`: navegação ou ação terciária.
- `.btn--warning` / `.btn--success`: par tingido para pausar e ativar; mesmo peso visual, só a cor muda.
- `.btn--danger`: somente ação que destrói dados. Arquivar não é destrutivo: use ícone `archive` e estilo neutro.
- `.btn--sm`: tipografia menor para tabelas e rodapés de card; a altura continua 44 px.
- `.action-icon`: botão só de ícone, 44 × 44 px, sempre com `aria-label` e `data-tooltip`. Use em linhas de tabela; ações principais de uma página têm rótulo visível.

### Menu de ações

Agrupa ações secundárias de uma página atrás de um botão "Mais ações". É um disclosure (botão com `aria-expanded` + painel), não um `role="menu"`: abre com foco no primeiro item e fecha com Escape, clique fora ou envio de um formulário de dentro dele.

```html
<div class="action-menu" data-menu>
  <button class="btn btn--secondary" type="button" aria-expanded="false" aria-controls="id-do-painel" data-menu-toggle>Mais ações …</button>
  <div class="action-menu__panel" id="id-do-painel" data-menu-panel hidden>
    <!-- post_action(..., 'action-menu__item', ..., text='Rótulo', confirm='...') -->
  </div>
</div>
```

### Faixa de resumo

Todas as telas de lista usam `.summary-strip` (`--three` ou `--four`, com `--surface`) e o macro `summary_item(icone, valor, rótulo, tom)` de `components/summary.html`. O tom opcional (`warning`, `success`, `danger`) tinge só o ícone. A visão geral é a única exceção: usa os cards maiores `.dashboard-kpi`, por ser a página de resumo da operação.

```jinja
{% from "components/summary.html" import summary_item %}
<section class="summary-strip summary-strip--three summary-strip--surface" aria-label="Resumo">
  {{ summary_item("check", summary.active, "regras ativas", "success") }}
</section>
```

### Formulários

Todo campo usa `.field`, `label[for]`, controle com `id` e helper opcional `.field__hint`. Use `<select>` nativo: o `app.js` o transforma no dropdown temático (`.custom-select--native`) mantendo o elemento original no formulário. Formulários longos usam `.form-section`; formulários curtos usam `.card` + `.form-stack`. Placeholder é exemplo, nunca substitui label. Campo editável sempre parece campo (borda e 44 px de altura); texto somente leitura não tem borda.

### Status

Use `.status-badge` com uma das variações abaixo. O ponto, o texto e o contraste comunicam o estado juntos.

| Variação | Aparência | Uso |
| --- | --- | --- |
| `--success` | verde tingido | concluído, ativo |
| `--warning` | âmbar tingido | na fila, pausado |
| `--danger` | vermelho tingido | erro |
| `--info` | azul tingido | aguardando PACS, monitorando |
| `--progress` | azul **preenchido** | retrieve ou recebimento em andamento |
| `--active` | azul tingido | rótulos de destaque (perfil, "Nova regra"); não é estado de pedido |
| `--neutral` | cinza (`--color-idle-*`) | cancelado, arquivado, inativo |

O mapeamento de status de pedido para badge fica em `badge_for()`, em `app/web.py` (nomes curtos legados: `ok`, `warn`, `err`, `info`, `progress`, `off`).

### Cards, tabela e vazio

- `.card`: conteúdo agrupado com padding.
- `.surface.card--flush`: tabela ou lista com conteúdo até a borda.
- `.data-table`: padrão único de tabela, sempre dentro de `.table-wrap`.
- `.empty-state`: ícone, título útil, explicação curta e CTA apenas quando existe próxima ação real.

### Modal e feedback

Barras de ação persistentes (`.form-actions`) ativam um `scroll-padding-bottom` na página, para que o campo focado por teclado não fique escondido atrás delas.

Adicione `data-confirm`, e opcionalmente `data-confirm-label`, ao formulário que encerra, cancela, arquiva ou exclui. Não crie `confirm()` inline. Mensagens persistentes do servidor usam o flash existente; o layout as apresenta como toast. Todo redirecionamento causado por erro (registro inexistente, ação bloqueada) leva um flash explicando o motivo.

### Logo por tema

Imagens de marca com versão para fundo claro e escuro ficam lado a lado no HTML, com `theme-logo--light` e `theme-logo--dark`; o CSS mostra só a do tema ativo.

```html
<img class="login-panel__logo theme-logo--light" src="/static/mobilemed-login.png?v=1" alt="Mobilemed" ...>
<img class="login-panel__logo theme-logo--dark" src="/static/mobilemed-login-dark.png?v=1" alt="Mobilemed" ...>
```

| Uso | Tema claro | Tema escuro |
| --- | --- | --- |
| Login | `mobilemed-login.png` | `mobilemed-login-dark.png` |
| Página de erro | `logo-white.png` | `logo-dark.png` |
| Sidebar (sempre escura) | `mobilemed-dark.png` | `mobilemed-dark.png` |

### Cache de assets

Ao alterar `app.css` ou `app.js`, incremente o `?v=` correspondente em `base.html`, `login.html` e `error.html`.

## Não misturar depois

- Não adicionar Bootstrap, Tailwind ou outro reset global sobre este CSS.
- Não introduzir cores fora dos tokens. Em componentes, nada de hex ou `rgb()` fixo: use `--color-*`, `--border-*` e `--shadow-*`, que já se ajustam ao tema escuro. A sidebar é a única exceção, porque é escura nos dois temas.
- Não usar gradientes, sombras grandes ou cards aninhados como decoração.
- Não criar botão primário para ações secundárias, nem botão vermelho para ações reversíveis como arquivar.
- Não usar emoji, ícones preenchidos ou bibliotecas com linguagem visual diferente do macro `icons.html`.
- Não voltar a estilos ou scripts inline; a CSP foi endurecida para bloqueá-los.
- Não reduzir controles abaixo de `--control-h` (44 px) nem texto abaixo de `--text-min` (12 px), não remover foco visível e não depender só de cor.
- Não usar `--color-neutral-500` para texto; texto secundário é `--color-neutral-600`.
- Não deixar ações principais de uma página apenas como ícone.
- Não criar páginas sem o padrão cabeçalho → filtros/ações → conteúdo → paginação.
- Não usar placeholder como label nem mensagens de erro genéricas quando a causa é conhecida.
- Não adicionar imagens decorativas em telas operacionais densas.
- Não publicar imagem de marca sem versão para o tema escuro quando ela aparece fora da sidebar.

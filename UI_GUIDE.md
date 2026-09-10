# Sistema visual — Retrieve Manager

## Premissas

- O frontend permanece server-side com FastAPI, Jinja, HTML semântico, CSS próprio e JavaScript mínimo.
- As rotas, nomes de campos e regras de negócio existentes foram preservados.
- Não há framework CSS nem biblioteca de ícones no projeto; por isso, os componentes são locais e os ícones usam um único macro SVG.
- Nenhuma fonte, imagem ou script depende de rede externa, mantendo a interface funcional no container e em redes restritas.

## Direção de design

1. A interface assume o tom de uma central de operação clínica: precisa, estável e silenciosa.
2. Azul-petróleo profundo identifica navegação e marca sem competir com os dados.
3. Neutros frios organizam fundo, superfície e divisores; branco é reservado ao conteúdo acionável.
4. Verde, âmbar, vermelho e azul aparecem apenas para comunicar estados operacionais.
5. A hierarquia usa tamanho, peso e espaço antes de recorrer a cor ou decoração.
6. O ritmo parte de múltiplos de 4 px, com blocos de 16, 24, 32 e 40 px.
7. Bordas finas definem grupos; sombras são reservadas a elementos elevados e feedback transitório.
8. A linha vertical de status nas unidades é a assinatura visual do produto e facilita varredura rápida.
9. Textos são curtos, diretos e escritos em português do Brasil para quem opera o fluxo.
10. Todos os controles têm foco visível, altura mínima de 44 px e estados que não dependem apenas de cor.

## Design tokens

Os tokens ficam em `app/static/app.css`, dentro de `:root`.

| Grupo | Token | Valor / uso |
| --- | --- | --- |
| Marca | `--color-brand-900` | `#102d3c`, sidebar e contexto de login |
| Primária | `--color-primary-700` | `#155c70`, ações e links |
| Primária suave | `--color-primary-100` | `#dceef2`, seleção e apoio |
| Texto | `--color-neutral-950` | `#10232f`, títulos e conteúdo principal |
| Texto secundário | `--color-neutral-600` | `#5c6d76`, descrições e metadados |
| Fundo | `--color-neutral-50` | `#f4f7f8`, plano de fundo da aplicação |
| Borda | `--color-neutral-200` | `#dce3e7`, separação estrutural |
| Sucesso | `--color-success-700/100` | ativo, concluído e disponível |
| Alerta | `--color-warning-700/100` | espera e atenção |
| Erro | `--color-danger-700/100` | falha e ação destrutiva |
| Informação | `--color-info-700/100` | observação e estado informativo |
| Fonte | `--font-sans` | Inter quando instalada, Segoe UI e fontes do sistema como fallback |
| Mono | `--font-mono` | UIDs, caminhos, correlation IDs e logs |
| Espaço | `--space-1` a `--space-16` | escala de 4 a 64 px |
| Raios | `--radius-sm/md/lg` | 3, 4 e 6 px |
| Elevação | `--shadow-xs` | cards; quase imperceptível |
| Elevação alta | `--shadow-popover` | modal e skip link |
| Foco | `--focus-ring` | anel azul-petróleo com contraste visível |

## Inventário das telas

| Tela | Mudança principal |
| --- | --- |
| Login | Composição em dois planos, labels explícitas, revelar senha e erro acessível |
| Visão geral | Cards operacionais com linha de estado, KPIs compactos e atualização não intrusiva |
| Fila de pedidos | Filtros alinhados, tabela consistente, ações hierarquizadas e vazio contextual |
| Detalhe do pedido | Dados e timeline em duas colunas, IDs monoespaçados e confirmação de cancelamento |
| Unidades | Lista limpa, estado legível e acesso direto à edição |
| Cadastro de unidade | Formulário longo dividido em quatro etapas lógicas e barra de ação persistente |
| Tempo de retrieve | Regras editáveis em grid, com labels recuperadas no mobile |
| Compactação e descarte | Perfis como escolhas restritas e confirmação apenas na remoção |
| Nuvem e acesso | Configurações separadas por responsabilidade e helpers objetivos |
| Erros HTTP | Página 403/404/405/422/500 consistente e com ID da solicitação |

## Telas e componentes importantes

### Estrutura global e navegação

- **Problema atual:** links sem ícones, pouca distinção de grupos e sidebar ocupando a tela inteira no mobile.
- **Decisão de design:** navegação escura agrupada por tarefa, item ativo com linha lateral e menu móvel com overlay e suporte a Escape.
- **Resultado (código):** `app/templates/base.html`, classes `.app-shell`, `.sidebar`, `.nav__item` e controles `data-sidebar-*`.
- **Por que ficou mais profissional:** mantém contexto, reduz ruído e apresenta uma hierarquia previsível em qualquer viewport.

### Login

- **Problema atual:** card isolado com aparência genérica e pouca orientação de contexto.
- **Decisão de design:** separar identidade do produto e tarefa de autenticação, mantendo o formulário como foco principal.
- **Resultado (código):** `app/templates/login.html`, `.login-shell`, `.login-context`, `.login-panel` e `data-password-toggle`.
- **Por que ficou mais profissional:** transmite segurança e maturidade sem adicionar decoração gratuita.

### Visão geral e card de unidade

- **Problema atual:** status, métricas, caminhos e ações competiam entre si dentro do mesmo card.
- **Decisão de design:** organizar leitura em nome/status, quatro métricas, contagem de pastas e ações; a borda lateral resume a saúde operacional.
- **Resultado (código):** `dashboard.html`, `dashboard_partial.html`, `.unit-card`, `.metric` e `.poll-region`.
- **Por que ficou mais profissional:** o operador identifica exceções antes de ler detalhes e o polling não interrompe controles em foco.

### Filtros, tabela e paginação

- **Problema atual:** larguras inline, labels genéricas, ações com o mesmo peso e paginação pouco semântica.
- **Decisão de design:** um filtro responsivo, tabela com caption, cabeçalhos com `scope`, ações primária/secundária/destrutiva e paginação navegável.
- **Resultado (código):** `orders.html`, `units_list.html`, `pager.html`, `.filter-grid`, `.data-table` e `.pager`.
- **Por que ficou mais profissional:** cria um padrão único de página de dados e melhora leitura por teclado e tecnologia assistiva.

### Detalhe do pedido e timeline

- **Problema atual:** metadados extensos e logs disputavam espaço, dificultando localizar status e erro.
- **Decisão de design:** manter identificação à esquerda, histórico à direita e conteúdo técnico recolhido por padrão.
- **Resultado (código):** `order_detail.html`, `.description-list`, `.timeline` e `.log-box`.
- **Por que ficou mais profissional:** prioriza decisão operacional e conserva detalhe técnico sob demanda.

### Formulário de unidade

- **Problema atual:** formulário longo, campos sem `id/for`, pouco contexto e ação de salvar distante.
- **Decisão de design:** quatro grupos numerados — identificação, PACS, store/diretórios e limites — com helpers, limites HTML e barra de ação persistente.
- **Resultado (código):** `units_form.html`, `.form-section`, `.form-grid`, `.field__hint`, `.form-actions` e `.danger-zone`.
- **Por que ficou mais profissional:** reduz carga cognitiva, previne erros de preenchimento e separa claramente manutenção de exclusão.

### Regras de retrieve, compactação e descarte

- **Problema atual:** formulários dentro de células, estilos inline e opções técnicas digitadas livremente.
- **Decisão de design:** grids editáveis próprios e `select` para os três perfis JPEG válidos.
- **Resultado (código):** `rules_retrieve.html`, `rules_compress.html`, `.rule-list`, `.rule-row` e `.inline-create`.
- **Por que ficou mais profissional:** mantém alta densidade no desktop e recupera labels explícitas no mobile.

### Feedback, loading e confirmação

- **Problema atual:** flashes sem ação de fechar, polling silencioso e `confirm()` nativo inconsistente.
- **Decisão de design:** toast semântico, spinner único em submit/polling e `<dialog>` apenas para cancelamento/exclusão/remoção.
- **Resultado (código):** `base.html`, `app/static/app.js`, `.toast`, `.is-loading` e `.confirm-dialog`.
- **Por que ficou mais profissional:** cada ação informa estado, evita duplo envio e reserva fricção às ações realmente destrutivas.

### Telas de erro

- **Problema atual:** erros do framework apareciam como JSON fora do contexto visual do produto.
- **Decisão de design:** página concisa, ação de recuperação e request ID para suporte; respostas não HTML continuam em JSON.
- **Resultado (código):** `error.html` e handlers seletivos em `app/main.py`.
- **Por que ficou mais profissional:** mantém confiança em falhas e torna o atendimento rastreável sem expor detalhe interno.

## Guia rápido de componentes

### Cabeçalho de página

Use sempre `page-header`, com eyebrow ou breadcrumb, `h1`, descrição curta e uma única ação primária em `page-header__actions`.

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

- `.btn--primary`: uma ação principal por contexto.
- `.btn--secondary`: ação neutra com borda.
- `.btn--quiet`: navegação ou ação terciária.
- `.btn--danger`: somente ação destrutiva.
- `.btn--sm`: apenas em tabelas e linhas compactas.

### Formulários

Todo campo usa `.field`, `label[for]`, controle com `id` e helper opcional `.field__hint`. Formulários longos usam `.form-section`; formulários curtos usam `.card` + `.form-stack`. Placeholder é exemplo, nunca substitui label.

### Status

Use `.status-badge` com uma das variações `--success`, `--warning`, `--danger`, `--info`, `--active` ou `--neutral`. O ponto, o texto e o contraste comunicam o estado juntos.

### Cards, tabela e vazio

- `.card`: conteúdo agrupado com padding.
- `.surface.card--flush`: tabela ou lista com conteúdo até a borda.
- `.data-table`: padrão único de tabela, sempre dentro de `.table-wrap`.
- `.empty-state`: ícone, título útil, explicação curta e CTA apenas quando existe próxima ação real.

### Modal e feedback

Adicione `data-confirm`, e opcionalmente `data-confirm-label`, ao formulário destrutivo. Não crie `confirm()` inline. Mensagens persistentes do servidor usam o flash existente; o layout as apresenta como toast.

## Não misturar depois

- Não adicionar Bootstrap, Tailwind ou outro reset global sobre este CSS.
- Não introduzir novas cores fora dos tokens para status ou ações.
- Não usar gradientes, sombras grandes ou cards aninhados como decoração.
- Não criar botão primário para ações secundárias nem botão vermelho para ações reversíveis.
- Não usar emoji, ícones preenchidos ou bibliotecas com linguagem visual diferente do macro `icons.html`.
- Não voltar a estilos ou scripts inline; a CSP foi endurecida para bloqueá-los.
- Não reduzir controles abaixo de 44 px, remover foco visível ou depender só de cor.
- Não criar páginas sem o padrão cabeçalho → filtros/ações → conteúdo → paginação.
- Não usar placeholder como label nem mensagens de erro genéricas quando a causa é conhecida.
- Não adicionar imagens decorativas em telas operacionais densas.

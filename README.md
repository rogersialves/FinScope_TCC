# FinScope – Pipeline de ML para Previsão de Demanda Intermitente

Projeto de TCC (MBA Data Science e Analytics – USP/ESALQ) que implementa um pipeline de Machine Learning para previsão de demanda intermitente e orçamentação de materiais de manutenção em indústria de fertilizantes.

**Pergunta de pesquisa:** Como um pipeline de ML integrado a análise ABC e classificação de criticidade pode estruturar a orçamentação anual de materiais de manutenção em uma indústria de fertilizantes?

## Módulo Principal

| Módulo | Pasta | Objetivo | Comando principal |
| --- | --- | --- | --- |
| **Sólidos ML** | `Modulo_Solidos_ML/` | Pipeline completo de machine learning + orçamento (essencial, fidelidade, inteligente). | `python Modulo_Solidos_ML/main.py orcamento --modo inteligente` |

### Componentes de Suporte

| Componente | Pasta | Função |
| --- | --- | --- |
| **Core** | `core/` | Config centralizada (`fin_params.py`), logging, utilitários de pipeline |
| **Utilitários** | `fin_utils.py` | Métricas de preço, filtros de conta/fonte |
| **Integração ML** | `integration_ml.py` | Orquestração ML + análise ABC + criticidade por TAG |

> As pastas `Modulo_PA/`, `Modulo_PT/`, `Modulo_PisCofins/` e `finscope_gui.py` existem no repositório como código legado/auxiliar, mas **não fazem parte do escopo do TCC**.

## Funcionalidades principais

- **Pipeline de previsão** (`Modulo_Solidos_ML/pipeline.py`): aplica filtros de negócio, gera séries mensais, calcula ADI/CV² e escolhe o melhor modelo por item (Croston Safe, Croston Modificado, SES ou ARIMA). O seletor inteligente (RandomForest) pode ser ativado/desativado via YAML.
- **Orçamento com margens dinâmicas** (`Modulo_Solidos_ML/budget_projection.py`): integra preços corrigidos por IPCA, converte previsões em quantidade/projeção anual e aplica margens baseadas no *Quality Score* ou em margens herdadas.
- **Configuração central** (`config/params.yaml`): define filtros, thresholds, modelos habilitados, margens por faixa de qualidade, correção IPCA e flags como `ml.quality_score.habilitado`.
- **Exportação rastreável**: gera `ML_CONSUMO_SOLIDOS.xlsx`, `predicoes.csv`, `avaliacao.csv`, `relatorio.json` e o Excel final `ORCAMENTO_2026_SOLIDOS_ML.xlsx`, todos com metadados que explicam a decisão do modelo.
- **Core compartilhado**: pasta core/ expõe in_params.py (leitura do config/params.yaml) para todos os módulos.

## Estrutura de diretórios

```
FinScope/
├─ config/                # params.yaml e variações de configuração
├─ Modulo_Solidos_ML/     # Pipeline ML + orçamento (FOCO DO TCC)
├─ core/                  # Config, logging, utilitários de pipeline
├─ docs/                  # Wiki (MkDocs) + specs do TCC
├─ TCC/                   # Documento do TCC (.docx)
├─ fin_utils.py           # Funções utilitárias compartilhadas
├─ integration_ml.py      # Orquestração ML + ABC + criticidade
├─ requirements.txt       # Dependências de execução
└─ README.md
```

## Instalação e dependências

### Ambiente Python (venv)

```bash
python -m venv .venv
.\.venv\Scripts\activate    # Windows
# ou
source .venv/bin/activate   # Linux/macOS
```

Requisitos mínimos:

| Pacote | Uso principal |
| --- | --- |
| `pandas` | Manipulação de dados tabulares. |
| `numpy` | Cálculos numéricos e estatísticos. |
| `scikit-learn` | Seletor inteligente e métricas auxiliares. |
| `statsmodels` | ARIMA automático. |
| `openpyxl` | Leitura/escrita de arquivos Excel (`.xlsx`). |
| `PyYAML` | Leitura de `config/params.yaml`. |
| `typer` *(opcional)* | Utilitário de CLI usado em módulos auxiliares. |

Instale tudo com:

```bash
pip install -r requirements.txt
```

> Para gerar a documentação local, instale também `mkdocs` e `mkdocs-material`.

## Executando os módulos

### Sólidos ML (pipeline + orçamento)

```bash
# Apenas previsões (gera CSV/Excel intermediário)
python Modulo_Solidos_ML/main.py ml --horizon 6

# Orçamento – modo essencial (margens herdadas ou padrão)
python Modulo_Solidos_ML/main.py orcamento --modo essencial

# Orçamento – modo inteligente (padrão recomendado)
python Modulo_Solidos_ML/main.py orcamento --modo inteligente
```

Entradas esperadas na raiz do projeto:

- `data/BASE_HISTORICA.xlsx` – histórico bruto ou filtrado de consumo.
- `data/TAB_AUX.xlsx` – índices de IPCA (aba `TABS_AUX`) e metadados.

Saídas principais: `Modulo_Solidos_ML/orcamento_2026/` com os CSVs, Excel e JSON de resumo.

Consulte `Modulo_Solidos_ML/README.md` para parâmetros adicionais.

## Configuração (params.yaml)

- **Filtros de negócio**: `filtros.*` controla contas contábeis, range de centro de custo, ano mínimo e exclusão da palavra "PARADA".
- **Modelos de previsão**: `ml.modelos_habilitados` liga/desliga SES, Croston Safe, Croston Modificado e ARIMA; `ml.seletor_inteligente.habilitado` ativa o RandomForest.
- **Validação & qualidade**: `ml.validacao.*` define limite de gap, penalização, meses inativos, razão de revisão; `ml.quality_score.habilitado` determina se a coluna `QUALITY_SCORE` deve ser calculada.
- **Orçamento**: `orcamento.margens_por_quality`, `orcamento.margem_seguranca_padrao` e `orcamento.margem_maxima` controlam margens aplicadas; `precos.*` define correção por IPCA e ajustes adicionais.

## Documentação

A wiki oficial está em `docs/` e cobre:

- Arquitetura completa do pipeline.
- Referência das funções e modelos.
- Modos de orçamento e estrutura das saídas.
- Troubleshooting, glossário e checklists.

Para visualizar localmente:

```bash
pip install mkdocs mkdocs-material
mkdocs serve         # abre em http://127.0.0.1:8000
```

## Boas práticas

1. **Sempre ajuste a configuração via YAML** – evita divergência entre ambientes e facilita versionamento.
2. **Monitore alertas** (`REVIEW_ALERT`, `GAP_ALERT`, `BAIXA_CONFIABILIDADE`) ao revisar o orçamento.
3. **Atualize IPCA periodicamente** para garantir que `PRECO_2026` reflita inflação corrente.
4. **Versione as saídas** (Excel, CSV, JSON) em artefatos do pipeline ou repositórios internos.
5. **Consulte a wiki** ao evoluir regras de negócio ou ao integrar o FinScope com sistemas corporativos.

Com o ambiente pronto, execute os módulos necessários, acompanhe os logs `[OK]` emitidos pelo pipeline e utilize as planilhas geradas para tomadas de decisão financeiras com rastreabilidade total.

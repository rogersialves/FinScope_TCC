# Módulo — Sólidos (ML) — Previsão de Demanda e Orçamento 2026

## Objetivos
1. **Pipeline ML**: Implementar previsão de taxa de consumo usando técnicas de Machine Learning para demanda intermitente
2. **Orçamento 2026**: Projetar gastos anuais baseados em previsões ML + evolução de preços + margens de segurança
3. **Integração Completa**: Combinar padrões de demanda, análise ABC, criticidade por TAG e estratégias de estoque

## Entrada de Dados
- **Principal**: [BASE_HISTORICA.xlsx](../BASE_HISTORICA.xlsx) (aba CONSOLIDADO)
- **Auxiliar**: [TAB_AUX.xlsx](../TAB_AUX.xlsx) (para IPCA e catálogo de TAGs)
- **Filtros aplicados (parametrizaveis via `config/params.yaml`)**:
  - `filtros.aplicar_filtro_conta_contabil` mantem apenas contas "MATERIAIS" e "COMBUSTIVEIS E LUBRIFICANTES".
  - `filtros.aplicar_filtro_centro_custo` e `filtros.centro_custo_range` controlam o range de Centro de Custo (padrao 6000-6675).
  - `filtros.aplicar_filtro_ano_minimo` + `filtros.ano_minimo_dados` limitam o historico; desative para aproveitar series completas.
  - `filtros.remover_itens_palavra_parada` define a remocao de itens com palavra-chave (padrao "PARADA").
- **Projecao de precos**: `precos.ajuste_projecao_padrao_pct` determina o acrescimo percentual na projecao base (default 3,5%).

## Como Executar (ATUALIZADO)

### Comando Completo (Recomendado)
Para reexecutar ML + Orçamento 2026 com todas as correções:
```powershell
# Limpar resultados antigos para forçar reexecução
Remove-Item "Modulo_Solidos_ML\saida_ml\ml_results" -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item "Modulo_Solidos_ML\saida_ml\*.csv" -Force -ErrorAction SilentlyContinue

# Executar pipeline completo
python Modulo_Solidos_ML/main.py orcamento --margem 0.20
```

### Modos de Orçamento
- **Modo Fidelidade**: Usa a taxa prevista pelo ML diretamente (com amortecimento por qualidade)
  ```bash
  python Modulo_Solidos_ML/main.py orcamento --modo fidelidade
  ```
- **Modo Essencial**: Aplicação conservadora com poda de itens "Muito Raro" e amortecimento adicional por classe de uso
  ```bash
  python Modulo_Solidos_ML/main.py orcamento --modo essencial --margem 0.20
  ```
- **Modo inteligente (Novo)**: Seleção inteligente de modelo + ajustes dinâmicos de parâmetros (Croston Modificado, ARIMA opcional, ML para seleção)
  ```bash
  # Recomendado: instalar dependências adicionais
  pip install statsmodels scikit-learn

  # Executar orçamento com Modo inteligente
  python Modulo_Solidos_ML/main.py orcamento --modo inteligente --margem 0.20
  ```

### 1. Apenas Previsão ML
```bash
python Modulo_Solidos_ML/main.py ml --horizon 12
```
python Modulo_Solidos_ML/main.py orcamento --modo fidelidade
### 2. Orçamento Completo 2026 (ML + Preços + Estratégias)
```bash
python Modulo_Solidos_ML/main.py orcamento --margem 0.15
```

### 3. Pipeline ML Direto
```bash
python Modulo_Solidos_ML/pipeline.py -i BASE_HISTORICA.xlsx -o saida_ml --horizon 3
```

### 4. Orçamento Direto
```bash
python Modulo_Solidos_ML/budget_projection.py --margem 0.20
```

### 5. Integração Completa (Teste)
```bash
python integration_ml.py
python Modulo_Analise/test_integration_ml.py
```

## Arquivos e Funcionalidades

### Core Pipeline ML
- **[`pipeline.py`](pipeline.py)**: Pipeline principal de Machine Learning
  - `load_data()`: Carrega e filtra dados (CONTA_CONTABIL, CC 6000-6675, remove "PARADA")
  - `validate_and_clean()`: Valida datas, remove negativos, consolida duplicatas
  - `build_monthly_series()`: Constrói calendário mensal completo por item
  - `compute_indicators()`: Calcula métricas (MESES_COM_CONSUMO, TAXA_MESES_COM_CONSUMO, gaps)
  - `compute_adi_cv2()`: Calcula ADI (Average Demand Interval) e CV² (Coefficient of Variation²)
  - `classify_demand_patterns()`: Classifica em Smooth/Erratic/Intermittent/Lumpy
  - `select_and_forecast()`: Seleciona modelo (SES vs Croston) por menor MASE e gera previsões
  - `evaluate_and_score()`: Avalia qualidade (MASE, RMSE, MAE) e flags de confiabilidade
  - `export_outputs()`: Exporta Excel + CSV + JSON com resultados

### Orçamento e Integração
- **[`budget_projection.py`](budget_projection.py)**: Projeção orçamentária 2026
  - `run_budget_projection()`: Pipeline completo ML → Preços → Orçamento
  - `calcular_projecao_orcamentaria()`: Combina taxa ML + preços 2026 + margens de segurança
  - `gerar_resumo_orcamentario()`: Métricas executivas e distribuição ABC
  - Margens dinâmicas baseadas em qualidade da previsão e padrão de demanda
  - Modo Proposto: `_forecast_proposto` com seleção ML (RandomForest), Croston Modificado e ARIMA opcional

- **[`integration_ml.py`](../integration_ml.py)**: Integração avançada
  - `enrich_ml_predictions_with_prices()`: ML + Preços + Estratégias de estoque
  - `classify_items_by_value_and_criticality()`: Análise ABC + Criticidade por TAG
  - `calculate_abc_analysis()`: Classificação Pareto 80-20 por valor
  - `map_criticality_by_tag()`: Criticidade (Critical/Normal/Low) baseada em padrões de TAG

### Interface e Testes
- **[`main.py`](main.py)**: CLI principal com dois modos
  - `ml`: Apenas pipeline de previsão
  - `orcamento`: Pipeline completo com orçamento 2026
- **[`Modulo_Analise/test_integration_ml.py`](../Modulo_Analise/test_integration_ml.py)**: Teste de integração com dados simulados

## Saídas Geradas

### 1. Pipeline ML (`saida_ml/`)
- **ML_CONSUMO_SOLIDOS.xlsx** com 5 abas:
  - `MENSAL_SERIES`: Séries temporais mensais (QTD_MENSAL, VALOR_MENSAL)
  - `INDICADORES`: Métricas por item (MESES_COM_CONSUMO, TAXA_MESES_COM_CONSUMO, gaps)
  - `CLASSIFICACAO`: Padrões de demanda (ADI, CV², DEMAND_PATTERN)
  - `PREVISOES`: Modelos usados, taxas previstas, séries JSON
  - `AVALIACAO`: Qualidade das previsões (MASE, RMSE, MAE, QUALITY_SCORE)
- **predicoes.csv**: Previsões por item (modelo, taxa, meta-parâmetros)
- **avaliacao.csv**: Métricas de erro e flags de confiabilidade
- **relatorio.json**: Resumo executivo (total processado, distribuição por padrão, qualidade média)

### 2. Orçamento 2026 (`orcamento_2026/`)
- **ORCAMENTO_2026_SOLIDOS_ML.xlsx** com 5 abas:
  - `ORCAMENTO_2026`: Lista completa (quantidade projetada, preços 2026, gastos com margem)
  - `RESUMO_ABC`: Totalização por classe ABC (A=80%, B=95%, C=resto)
  - `TOP_20_ITENS`: 20 itens de maior impacto financeiro
  - `ORCAMENTO_MENSAL`: Distribuição mensal probabilística do orçamento por item (inclui `CLASSE_USO`)
  - `CONSOLIDADO_ORIGINAL`: Dados históricos processados (para auditoria)
- **resumo_orcamento_2026.json**: Métricas executivas
  - Orçamento total vs. base
  - Distribuição ABC (itens, valores, percentuais)
  - Qualidade média das previsões

### 3. Integração Completa
- **ML_INTEGRADO_PRECOS_ESTRATEGIAS.xlsx** (via `integration_ml.py`):
  - `INTEGRACAO_COMPLETA`: ML + Preços + ABC + Criticidade + Estratégias
  - `CLASSIFICACAO_ABC_CRIT`: Análise ABC e criticidade por item
  - `RESUMO_POR_ESTRATEGIA`: Agregação por estratégia de estoque
- **teste_integracao_ml.xlsx** (via `Modulo_Analise/test_integration_ml.py`):
  - Teste com dados simulados (valores/TAGs) aplicado aos 4.316 itens reais

## Metodologia e Algoritmos

### Classificação de Padrões de Demanda
Baseada nas métricas **ADI** e **CV²** para caracterizar intermitência e variabilidade:

| Padrão | ADI | CV² | Características | Estratégia ML |
|--------|-----|-----|-----------------|---------------|
| **Smooth** | < 1.32 | < 0.49 | Frequente + Estável | SES (Suavização Exponencial) |
| **Erratic** | < 1.32 | ≥ 0.49 | Frequente + Volátil | SES + Margem Alta |
| **Intermittent** | ≥ 1.32 | < 0.49 | Raro + Previsível | Croston |
| **Lumpy** | ≥ 1.32 | ≥ 0.49 | Raro + Volátil | Croston + Margem Alta |

### Modelos de Previsão
1. **SES (Simple Exponential Smoothing)**:
   - Grade de alphas: 0.05, 0.1, 0.2, 0.3
   - Seleção por validação temporal (MASE)
   - Ideal para demanda frequente

2. **Croston Seguro (2025)** (CORRIGIDO):
   - Separa tamanho da demanda do intervalo entre demandas
   - Taxa = demanda_média / intervalo_médio
   - **Tratamento especial para casos extremos**:
     - 1 ocorrência: distribui ao longo de 2× período observado
     - 2-3 ocorrências: usa média simples com fator conservador (+50% intervalo)
     - 4+ ocorrências: aplica Croston padrão
   - Ideal para demanda intermitente

3. **Croston Modificado (Novo)**:
  - Ajuste de tendência linear leve nos eventos de demanda
  - Dois alphas: demanda (α) e intervalo (β)
  - Mantém conservadorismo para poucos eventos

4. **ARIMA (Opcional no Modo Proposto)**:
  - Seleção automática simples de ordem (p,d,q) por AIC
  - Aplicado quando há tendência detectável e a série possui comprimento mínimo (≥ 18 períodos)
  - Requer `statsmodels`
  - Fallback automático para Croston Modificado ou SES quando as condições não forem atendidas

5. **Seleção por Machine Learning (Novo)**:
  - Classificador RandomForest para escolher entre SES, Croston Seguro, Croston Modificado e ARIMA
  - Features: ADI, CV², força de tendência, sazonalidade simples e número de ocorrências
  - Treino inicial com dados simulados (bootstrap); pode ser substituído por histórico real
    - Robustez: features são saneadas (substituição de NaN/Inf e limitação por faixas) para evitar erros de entrada

### 3. **Seleção Baseada em Padrão** (CORRIGIDO):
   - **Intermittent/Lumpy** (ADI ≥ 1.32): Força uso do Croston
   - **Smooth/Erratic** (ADI < 1.32): Força uso do SES
   - **Não** compara por MASE, aplica modelo apropriado diretamente

### Métricas de Qualidade
- **MASE**: Mean Absolute Scaled Error (escala por naive sazonal)
- **RMSE**: Root Mean Square Error
- **MAE**: Mean Absolute Error
- **QUALITY_SCORE**: 90% (MASE≤0.5) → 75% (≤1.0) → 60% (≤1.5) → 50% (≤2.0) → 40%

### Cálculo da Quantidade Projetada 2026 (QTD_PROJETADA_COM_MARGEM)

A **QTD_PROJETADA_COM_MARGEM** é a quantidade anual estimada para 2026, calculada em duas etapas principais:

1. **BUDGET_RATE (Taxa Mensal Ajustada)**:
   - Baseada no **PREDICTED_CONSUMPTION_RATE** (taxa mensal prevista pelo modelo ML)
   - Aplicação de fatores de amortecimento por qualidade e classe de uso
   - Duas políticas: **Essencial** (conservadora) ou **Fidelidade** (usa taxa ML pura)

2. **QTD_PROJETADA_COM_MARGEM**:
   - Fórmula: `QTD_PROJETADA_COM_MARGEM = ceil(BUDGET_RATE × 12)`
   - Arredondamento para cima (`ceil`) garante pelo menos 1 unidade inteira
   - Itens classificados como "Muito Raro" recebem QTD_PROJETADA_COM_MARGEM = 0 por padrão

#### Exemplo Prático: COD_ITEM 35820
- **PREDICTED_CONSUMPTION_RATE**: 48,07933313 (unidades/mês, prevista pelo ML)
- **Modo Orçamento**: Essencial (padrão conservador)
- **Classe de Uso**: Rotineiro (alta frequência de uso)
- **QUALITY_SCORE**: 60 (entre 60-80, aplica amortecimento de 70%)
- **Cálculo BUDGET_RATE**:
  - `BUDGET_RATE = 48,07933313 × 0,70 = 33,65553319`
- **Cálculo QTD_PROJETADA_COM_MARGEM**:
  - `QTD_PROJETADA_COM_MARGEM = ceil(33,65553319 × 12) = ceil(403,8663983) = 404`
- **Resultado**: 404 unidades projetadas para 2026 (arredondado para cima)

#### Fatores de Amortecimento por Qualidade
- **QUALITY_SCORE < 40**: Amortecimento de 30% (alta incerteza)
- **QUALITY_SCORE 40-60**: Amortecimento de 50%
- **QUALITY_SCORE 60-80**: Amortecimento de 70%
- **QUALITY_SCORE > 80**: Amortecimento de 100% (confiança alta)
- **BAIXA_CONFIABILIDADE = True**: Amortecimento adicional de 20%

#### Políticas por Classe de Uso (Modo Essencial)
- **Rotineiro**: Usa taxa ML amortecida
- **Intermitente**: Taxa ML × 0,80 (conservador)
- **Ocasional**: min(taxa ML, max(ocorrências_12m/12, 1/12))
- **Raro**: min(taxa ML, ocorrências_12m/12)
- **Muito Raro**: QTD_PROJETADA_COM_MARGEM = 0 (sem projeção)

#### Políticas no Modo Proposto (Novo)
- Ajuste da taxa por ML (`_budget_rate_proposto`) com amortecimento por qualidade
- Itens "Muito Raro" mantêm QTD_PROJETADA_COM_MARGEM = 0 por padrão
- Margem de segurança adaptativa por qualidade + incerteza do padrão (máx 50%)

### Projeção Orçamentária
1. **Quantidade 2026**: `PREDICTED_CONSUMPTION_RATE × 12 meses`
2. **Preço 2026**: Via `fin_utils.compute_price_metrics` (último preço + IPCA + 3.5%)
3. **Margem de Segurança** (dinâmica):
   - Qualidade < 50%: +20%
   - Qualidade < 70%: +10%
   - Baixa confiabilidade: +15%
   - Padrão Lumpy: +25%
   - Padrão Erratic: +15%
   - Máximo: 50%

### Estratégias por Categoria
Matriz **ABC × Criticidade × Padrão**:
- **A + Critical + Lumpy**: "LSTM + Fornecedor Express"
- **A + Critical + Erratic**: "Ensemble Models + Alto Estoque"
- **B + Normal + Intermittent**: "Croston + Ponto Pedido"
- **C + Low + Smooth**: "Reposição Simples"

## Resultados Obtidos

### Pipeline ML (4.316 itens processados)
```json
{
  "total_items_processed": 4316,
  "distribution_by_pattern": {
    "Erratic": 2073,      // 48% - Frequente mas volátil
    "Intermittent": 1660, // 38.5% - Raro mas previsível  
    "Lumpy": 437,         // 10.1% - Raro e volátil
    "Smooth": 146         // 3.4% - Frequente e estável
  },
  "quality_summary": {
    "mean_quality": 74.18 // 74.2% de qualidade média
  }
}
```

### Orçamento 2026 (exemplo)
- **Orçamento Total**: R$ 45.2M (com margens de segurança)
- **Orçamento Base**: R$ 39.3M (previsões puras)
- **Margem de Segurança**: R$ 5.9M (15% médio)
- **Distribuição ABC**:
  - Classe A: 660 itens (15%) → R$ 36.2M (80%)
  - Classe B: 1.305 itens (30%) → R$ 6.8M (15%)
  - Classe C: 2.351 itens (55%) → R$ 2.2M (5%)

### Estratégias de Estoque (amostra)
- **Croston + Estoque Zero**: 738 itens (itens C intermitentes)
- **SES + Estoque Baixo**: 673 itens (itens B/C erráticos)
- **Ensemble Models + Alto Estoque**: 117 itens (itens A críticos erráticos)
- **LSTM + Fornecedor Express**: 26 itens (itens A críticos lumpy)

## Integração com Ecossistema FinScope

### Compatibilidade
- **Modulo_Solidos**: Compara TIPO_PADRAO manual vs padrões ML
- **Modulo_PA**: Valida classificações entre módulos
- **fin_utils**: Usa `compute_price_metrics` para preços 2026
- **TAB_AUX.xlsx**: Integra IPCA e catálogo de TAGs

### Dados Compartilhados
- **COD_ITEM**: Chave comum entre todos os módulos
- **DESC_ITEM**: Normalizado (remove espaços duplos, primeiro desc por item)
- **BASE_HISTORICA.xlsx**: Fonte única com filtros padronizados
- **Filtros**: CONTA_CONTABIL, CC 6000-6675, remoção "PARADA"

## Correções Implementadas (2025)

### Problemas Identificados e Soluções
1. **SES aplicado incorretamente a itens intermitentes**:
   - **Problema**: Item com 1 ocorrência (702 unidades) gerava previsão constante de 702/mês
   - **Solução**: Seleção forçada por padrão de demanda (ADI ≥ 1.32 → Croston)

2. **Croston inseguro para casos extremos**:
   - **Problema**: Poucos dados históricos geravam taxas irreais
   - **Solução**: Croston adaptativo com fatores conservadores

3. **Preços unitários incorretos**:
   - **Problema**: Confusão entre VALOR (total) e PRECO_UNITARIO
   - **Solução**: Cálculo explícito PRECO_UNITARIO = VALOR/QUANTIDADE

4. **Filtros de importação incompletos**:
   - **Problema**: Dados de "PARADA" inflando orçamento
   - **Solução**: Filtro adicional remove registros com "PARADA" na CONTA_CONTABIL

### Resultado Esperado
- **Antes**: Item 13905 → 702 unidades/mês × 12 = 8.424 unidades/ano
- **Depois**: Item 13905 → ~11.7 unidades/mês × 12 = ~140 unidades/ano (realístico)

### Validação de Qualidade
Para verificar se as correções foram aplicadas:
1. Verificar que itens com ADI ≥ 1.32 usam modelo "Croston"
2. Verificar que previsões de itens com 1-2 ocorrências são conservadoras
3. Verificar que PRECO_UNITARIO varia por item (não constante)
4. Verificar que aba CONSOLIDADO_ORIGINAL está presente no Excel final

### Algoritmos Avançados
- **Croston-SBA/TSB**: Variantes com bias adjustment
- **LSTM com MC Dropout**: Para itens Lumpy críticos (se TensorFlow disponível)
- **Ensemble Methods**: Combinação de modelos por peso de qualidade
- **Bootstrap**: Intervalos de confiança para quantificar incerteza

### Relatórios e Dashboards
- **HTML Report**: Dashboard interativo com distribuição e itens críticos
- **Power BI Integration**: Conectores para dashboards executivos
- **Alertas**: Detecção de mudança de padrão (Smooth → Erratic)
- **ROI Analysis**: Comparação estratégias vs custos de estoque

### Funcionalidades Operacionais
- **Atualização Incremental**: Pipeline para dados novos sem reprocessar histórico
- **API REST**: Endpoints para consulta de previsões por item
- **Scheduling**: Execução automática mensal/trimestral
- **Auditoria**: Log de mudanças de classificação e qualidade ao longo do tempo

## Documentação Técnica

### Especificação Original
- **[Pipeline_Machine_learning_TAXA_CONSUMO.md](Pipeline_Machine_learning_TAXA_CONSUMO.md)**: Especificação detalhada do pipeline ML
- **[README_INTEGRACAO_ML.md](../README_INTEGRACAO_ML.md)**: Guia de uso da integração completa

### Performance
- **Tempo de Execução**: ~2-3 min para 4.316 itens (pipeline completo)
- **Memória**: ~500MB peak para dados históricos completos
- **Precisão**: 74.2% qualidade média (superior a métodos tradicionais)

### Dependências
```python
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
pathlib  # stdlib
argparse  # stdlib
scikit-learn>=1.3.0  # Modo Proposto (seleção por ML)
statsmodels>=0.13.0  # Modo Proposto (ARIMA opcional)
```

## Contato e Suporte
Para dúvidas técnicas ou sugestões de melhorias, consulte:
- **Issues**: Problemas e bugs reportados
- **Logs**: Arquivo de execução para debugging
- **Testes**: `Modulo_Analise/test_integration_ml.py` para validação

<!-- ===================== -->
<!-- DETALHAMENTO DAS COLUNAS DO ORÇAMENTO EM HTML -->
<!-- ===================== -->

<h2 id="estrutura-saida">Estrutura de Saída - Detalhamento das Colunas</h2>
<p><b>Arquivo:</b> <code>ORCAMENTO_2026_SOLIDOS_ML.xlsx</code> (Aba: ORCAMENTO_2026)</p>
<p>Esta seção documenta todas as colunas geradas no arquivo final de orçamento, explicando sua origem, cálculo e finalidade.</p>

<h3>1. Identificação do Item</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Origem</th></tr></thead>
<tbody>
<tr><td><code>COD_ITEM</code></td><td>String</td><td>Código único do item no sistema</td><td>13905</td><td>BASE_HISTORICA.xlsx</td></tr>
<tr><td><code>DESC_ITEM</code></td><td>String</td><td>Descrição normalizada do item</td><td>AREIA FINA</td><td>BASE_HISTORICA.xlsx</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Identificação única para rastreabilidade entre sistemas e módulos.</p>

<h3>2. Classificação e Análise</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>CLASSE_ABC</code></td><td>String</td><td>Classificação Pareto por valor orçado</td><td>A, B, C</td><td>Ordenação descendente por <code>GASTO_COM_MARGEM_2026</code>: A=80% acumulado, B=15%, C=5%</td></tr>
<tr><td><code>CLASSE_USO</code></td><td>String</td><td>Classificação de frequência de uso</td><td>Rotineiro, Intermitente, Ocasional, Raro, Muito Raro</td><td>Baseado em <code>USE_RATE</code> e <code>OCCURRENCES_12M</code></td></tr>
</tbody></table>
<p><b>Finalidade:</b> Priorização orçamentária (ABC) e estratégia de estoque (Classe de Uso).</p>
<pre><code class="language-python"># Algoritmo de Classificação ABC
budget = budget.sort_values('GASTO_COM_MARGEM_2026', ascending=False)
total_valor = budget['GASTO_COM_MARGEM_2026'].sum()
budget['VALOR_ACUMULADO_PCT'] = budget['GASTO_COM_MARGEM_2026'].cumsum() / total_valor
budget['CLASSE_ABC'] = np.where(
    budget['VALOR_ACUMULADO_PCT'] <= 0.80, 'A',
    np.where(budget['VALOR_ACUMULADO_PCT'] <= 0.95, 'B', 'C')
)
</code></pre>
<pre><code class="language-python"># Algoritmo de Classe de Uso
def classificar_uso(use_rate, occ_12m):
    if use_rate >= 0.75 or occ_12m >= 10:
        return 'Rotineiro'
    elif use_rate >= 0.50 or occ_12m >= 6:
        return 'Intermitente'
    elif use_rate >= 0.25 or occ_12m >= 3:
        return 'Ocasional'
    elif occ_12m >= 1:
        return 'Raro'
    else:
        return 'Muito Raro'
</code></pre>

<h3>3. Previsão de Consumo (Machine Learning)</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>PREDICTED_CONSUMPTION_RATE</code></td><td>Float</td><td>Taxa mensal de consumo prevista pelo modelo ML</td><td>48.08</td><td>Saída do modelo ML (SES/Croston/ARIMA)</td></tr>
<tr><td><code>QTD_PROJETADA_BASE_2026</code></td><td>Float</td><td>Quantidade anual antes da margem</td><td>368.18</td><td>PREDICTED_CONSUMPTION_RATE × 12</td></tr>
<tr><td><code>QTD_PROJETADA_COM_MARGEM</code></td><td>Float</td><td>Quantidade anual com margem aplicada</td><td>404</td><td>QTD_PROJETADA_BASE_2026 × (1 + MARGEM_SEGURANCA_PCT)</td></tr>
<tr><td><code>MODEL_USED</code></td><td>String</td><td>Modelo de ML utilizado</td><td>SES, Croston_Safe, Croston_Modificado, ARIMA</td><td>Seleção automática baseada em ADI/CV² ou RandomForest</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Previsão quantitativa para dimensionamento de estoque e orçamento.</p>
<pre><code class="language-python"># Cálculo Detalhado de QTD_PROJETADA_COM_MARGEM
qtd_base = predicted_rate * 12
qtd_projetada = qtd_base * (1 + margem_seguranca_pct)
</code></pre>

<h3>4. Preços e Valores</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>PRECO_ULTIMO</code></td><td>Float</td><td>Último preço unitário registrado</td><td>125.75</td><td>Último registro de VALOR/QUANTIDADE no histórico</td></tr>
<tr><td><code>PRECO_2026</code></td><td>Float</td><td>Preço projetado para 2026 (R$)</td><td>136.69</td><td>PRECO_ULTIMO × fator_IPCA × 1.035</td></tr>
<tr><td><code>GASTO_BASE_2026</code></td><td>Float</td><td>Gasto sem margem de seguranca (R$)</td><td>55,223.76</td><td>QTD_PROJETADA_BASE_2026 × PRECO_2026</td></tr>
<tr><td><code>MARGEM_SEGURANCA_PCT</code></td><td>Float</td><td>Margem de segurança aplicada (0-1)</td><td>0.15</td><td>Dinâmica por quality score + padrão de demanda</td></tr>
<tr><td><code>GASTO_COM_MARGEM_2026</code></td><td>Float</td><td>Gasto final orcado (R$)</td><td>63,507.32</td><td>QTD_PROJETADA_COM_MARGEM × PRECO_2026</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Projeção financeira realista com ajuste inflacionário e buffer de segurança.</p>
<pre><code class="language-python"># Cálculo de PRECO_2026
preco_2026 = preco_ultimo * fator_ipca * 1.035
</code></pre>

<h3>5. Qualidade da Previsão</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>QUALITY_SCORE</code></td><td>Float</td><td>Score de qualidade da previsão (0-100)</td><td>78.5</td><td>Mapeamento de MASE: ≤0.5→90, ≤1.0→75, ≤1.5→60, ≤2.0→50, >2.0→40</td></tr>
<tr><td><code>BAIXA_CONFIABILIDADE</code></td><td>Boolean</td><td>Flag de dados históricos insuficientes</td><td>True/False</td><td>True se histórico &lt; 12 meses OU ocorrências ≤ 2</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Indicadores de confiança para decisão de compra e revisão manual.</p>
<pre><code class="language-python"># Algoritmo de QUALITY_SCORE
def calcular_quality_score(mase):
    if mase <= 0.5:
        return 90
    elif mase <= 1.0:
        return 75
    elif mase <= 1.5:
        return 60
    elif mase <= 2.0:
        return 50
    else:
        return 40
</code></pre>

<h3>6. Métricas de Demanda (Classificação ADI/CV²)</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>ADI</code></td><td>Float</td><td>Average Demand Interval - Intervalo médio entre demandas</td><td>2.45</td><td>(último_mês - primeiro_mês + 1) / meses_com_consumo</td></tr>
<tr><td><code>CV2</code></td><td>Float</td><td>Coefficient of Variation² - Volatilidade da demanda</td><td>0.82</td><td>(desvio_padrão / média)² apenas meses com consumo &gt; 0</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Classificação de padrão de demanda (Smooth/Erratic/Intermittent/Lumpy) para seleção de modelo ML.</p>
<pre><code class="language-python"># Cálculo de ADI
def calcular_adi(serie_mensal):
    y = serie_mensal['QTD_MENSAL'].values
    pos_com_consumo = np.where(y > 0)[0]
    if len(pos_com_consumo) == 0:
        return float('inf')
    elif len(pos_com_consumo) == 1:
        return float(len(y))
    else:
        primeiro = pos_com_consumo[0]
        ultimo = pos_com_consumo[-1]
        periodo_ativo = ultimo - primeiro + 1
        num_ocorrencias = len(pos_com_consumo)
        adi = periodo_ativo / num_ocorrencias
        return round(adi, 2)
</code></pre>
<pre><code class="language-python"># Cálculo de CV2
def calcular_cv2(serie_mensal):
    y = serie_mensal['QTD_MENSAL'].values
    y_nonzero = y[y > 0]
    if len(y_nonzero) <= 1:
        return float('inf')
    media = np.mean(y_nonzero)
    desvio = np.std(y_nonzero, ddof=0)
    if media == 0:
        return float('inf')
    cv2 = (desvio / media) ** 2
    return round(cv2, 2)
</code></pre>

<h3>7. Features do Seletor Inteligente (Modo Inteligente)</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>TREND_STRENGTH</code></td><td>Float</td><td>Força da tendência linear</td><td>0.35</td><td>|coef_angular| da regressão linear em série temporal</td></tr>
<tr><td><code>SEASONALITY_SIMPLE</code></td><td>Integer</td><td>Indicador binário de sazonalidade</td><td>0 ou 1</td><td>1 se desvio_padrão &gt; 0.5 × média, senão 0</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Features adicionais para seleção automática de modelo via RandomForest (modo inteligente).</p>
<pre><code class="language-python"># Cálculo de TREND_STRENGTH
def calcular_trend_strength(serie_mensal):
    y = serie_mensal['QTD_MENSAL'].values
    if len(y) < 4:
        return 0.0
    try:
        x = np.arange(len(y))
        coef = np.polyfit(x, y, 1)
        trend_strength = abs(coef[0])
        return round(trend_strength, 2)
    except:
        return 0.0
</code></pre>
<pre><code class="language-python"># Cálculo de SEASONALITY_SIMPLE
def detectar_sazonalidade_simples(serie_mensal):
    y = serie_mensal['QTD_MENSAL'].values
    if len(y) < 6:
        return 0
    media = np.mean(y)
    desvio = np.std(y)
    if media > 0 and desvio > 0.5 * media:
        return 1
    else:
        return 0
</code></pre>

<h3>8. Métricas de Ocorrência</h3>
<table>
<thead><tr><th>Coluna</th><th>Tipo</th><th>Descrição</th><th>Exemplo</th><th>Cálculo/Origem</th></tr></thead>
<tbody>
<tr><td><code>OCCURRENCES_TOTAL</code></td><td>Integer</td><td>Total de meses com consumo &gt; 0</td><td>15</td><td>count(QTD_MENSAL &gt; 0) no histórico completo</td></tr>
<tr><td><code>OCCURRENCES_12M</code></td><td>Integer</td><td>Meses com consumo nos últimos 12 meses</td><td>8</td><td>count(QTD_MENSAL &gt; 0) nos 12 períodos mais recentes</td></tr>
<tr><td><code>USE_RATE</code></td><td>Float</td><td>Taxa de utilização (0-1)</td><td>0.68</td><td>OCCURRENCES_TOTAL / MESES_DESDE_PRIMEIRO</td></tr>
</tbody></table>
<p><b>Finalidade:</b> Avaliar frequência de uso e tendências recentes para classificação e validação.</p>
<pre><code class="language-python"># Cálculo de OCCURRENCES_TOTAL
def calcular_occurrences_total(serie_mensal):
    y = serie_mensal['QTD_MENSAL'].values
    occurrences = (y > 0).sum()
    return int(occurrences)
</code></pre>
<pre><code class="language-python"># Cálculo de OCCURRENCES_12M
def calcular_occurrences_12m(serie_mensal):
    serie_ordenada = serie_mensal.sort_values('ANO_MES', ascending=False)
    ultimos_12 = serie_ordenada.head(12)
    occurrences_12m = (ultimos_12['QTD_MENSAL'] > 0).sum()
    return int(occurrences_12m)
</code></pre>
<pre><code class="language-python"># Cálculo de USE_RATE
def calcular_use_rate(serie_mensal):
    primeiro_mes = serie_mensal[serie_mensal['QTD_MENSAL'] > 0]['ANO_MES'].min()
    ultimo_mes = serie_mensal['ANO_MES'].max()
    meses_decorridos = calcular_diferenca_meses(primeiro_mes, ultimo_mes) + 1
    occurrences = (serie_mensal['QTD_MENSAL'] > 0).sum()
    if meses_decorridos == 0:
        return 0.0
    use_rate = occurrences / meses_decorridos
    return round(use_rate, 2)
</code></pre>

<!-- ===================== -->
<!-- FIM DO DETALHAMENTO HTML -->
<!-- ===================== -->

## Parâmetros do Sistema (sem alteração de lógica)
Agora é possível ajustar filtros e defaults via arquivo `config/params.yaml`. Se o arquivo não existir, o sistema usa os valores padrão (mesmo comportamento atual).

- Caminhos padrão:
  - `sistema.paths.historico`: BASE_HISTORICA.xlsx
  - `sistema.paths.tab_aux`: TAB_AUX.xlsx
  - `sistema.paths.out_orcamento`: Modulo_Solidos_ML/orcamento_2026
- Filtros:
  - CONTA_CONTABIL permitidas: `filtros.conta_contabil_permitidas` (ex.: MATERIAIS; COMBUSTIVEIS E LUBRIFICANTES)
  - Centro de Custo: `filtros.centro_custo_range` (default 6000–6675)
  - Remoção de itens contendo "PARADA": `filtros.remover_itens_palavra_parada: true`
- ML:
  - `ml.horizon_meses_default`: 3
  - ADI/CV² thresholds: 1.32 / 0.49
- Orçamento:
  - `orcamento.ano_orcamento`: 2026
  - `orcamento.margem_seguranca_padrao`: 0.15
  - `orcamento.modo_padrao`: essencial | fidelidade | inteligente

### Exemplo de params.yaml
```yaml
sistema:
  paths:
    historico: "BASE_HISTORICA.xlsx"
filtros:
  centro_custo_range: [6000, 6675]
  remover_itens_palavra_parada: true
ml:
  horizon_meses_default: 3
orcamento:
  margem_seguranca_padrao: 0.15
```

### Variável de ambiente
Defina `FINSCOPE_CONFIG` para apontar um YAML customizado:
```bash
set FINSCOPE_CONFIG=C:\GitHub\RPA\FinScope\config\params.yaml
```

## Como Executar
- Modo Inteligente:
```bash
python Modulo_Solidos_ML/main.py orcamento --modo inteligente
```
- Modo Fidelidade:
```bash
python Modulo_Solidos_ML/main.py orcamento --modo fidelidade
```
- Modo Essencial:
```bash
python Modulo_Solidos_ML/main.py orcamento --modo essencial --margem 0.20
```
- Apenas ML:
```bash
python Modulo_Solidos_ML/main.py ml --horizon 12
```

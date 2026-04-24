# Pipeline de Automação para Projeção da Taxa de Consumo (Demanda Intermitente)

Este documento descreve, em formato operacional e detalhado, o pipeline que seu agente de automação deve executar para projetar a TAXA DE CONSUMO utilizando o Modelo Híbrido recomendado. Inclui instruções passo a passo, formatos de entrada/saída, regras de seleção de modelos, parametrização e métricas de validação. O pipeline está preparado para trabalhar com o arquivo BASE_HISTORICA.xlsx com as colunas detectadas: COD_ITEM, DESC_ITEM, QUANTIDADE, VALOR, DATA_ENTREGA.

## 1) Especificação de Entrada

Formato esperado:

- Arquivo: histórico da Fábrica de Sólidos a partir de [BASE_HISTORICA.xlsx](../BASE_HISTORICA.xlsx) (CC 6000–6675)
- Planilha padrão: primeira planilha
- Colunas obrigatórias:
    - COD_ITEM (int/str)
    - DESC_ITEM (str)
    - QUANTIDADE (float/int, valores ≥ 0)
    - VALOR (float, valores ≥ 0)
    - DATA_ENTREGA (datetime)

Validações:

- DATA_ENTREGA deve conter valores válidos entre 2010-01-01 e data atual.
- QUANTIDADE e VALOR não podem ser NaN; substituir NaN por 0 e sinalizar em relatório.
- Remover linhas com QUANTIDADE < 0 ou VALOR < 0.
- Duplicatas idênticas (COD_ITEM, DATA_ENTREGA, DESC_ITEM, QUANTIDADE, VALOR) devem ser consolidadas por soma de QUANTIDADE e VALOR.

Obs. Constatou-se que há 22 meses únicos no arquivo atual (2024-01 a 2025-10), com 19.471 registros e ~2.178 itens únicos. Isso é suficiente para uma primeira versão do pipeline (validação temporal mínima: 18 meses).

## 2) Preparação e Agregação Temporal

Objetivo: criar séries mensais de consumo por item.

Passos:

1. Converter DATA_ENTREGA para período mensal: ANO_MES = to_period('M').
2. Agregar por (COD_ITEM, ANO_MES):
    - QTD_MENSAL = soma(QUANTIDADE)
    - VALOR_MENSAL = soma(VALOR)
3. Preencher meses ausentes para cada item no intervalo [min(DATA_ENTREGA do item), max(DATA_ENTREGA do item)]:
    - Calendário mensal completo por item.
    - Onde não houver registro, QTD_MENSAL = 0, VALOR_MENSAL = 0.
4. Calcular colunas derivadas por item:
    - PRIMEIRA_DATA = primeira DATA_ENTREGA do item
    - ULTIMA_DATA = última DATA_ENTREGA do item
    - MESES_ANALISADOS_DESDE_1A_OCORRENCIA = total de meses do calendário desde PRIMEIRA_DATA
    - MESES_COM_CONSUMO = contagem de meses com QTD_MENSAL > 0
    - TAXA_MESES_COM_CONSUMO = MESES_COM_CONSUMO / MESES_ANALISADOS_DESDE_1A_OCORRENCIA
    - MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA = meses sem consumo após o último mês com QTD_MENSAL > 0

Saída desta etapa:

- Tabela mensal por item com QTD_MENSAL, VALOR_MENSAL.
- Tabela de indicadores por item com as colunas acima.


## 3) Métricas de Classificação por Intermitência

Definições:

- ADI (Average Demand Interval):
    - ADI = MESES_ANALISADOS_DESDE_1A_OCORRENCIA / MESES_COM_CONSUMO (se MESES_COM_CONSUMO = 0, definir ADI = ∞ e classificar como Lumpy por padrão)
- CV² (Coefficient of Variation squared) da série de QTD_MENSAL > 0:
    - Calcular média e desvio padrão apenas nos meses com QTD_MENSAL > 0; se o item tiver ≤ 1 mês com consumo, definir CV² = ∞.

Regras de classificação:

- Smooth: ADI < 1.32 e CV² < 0.49
- Erratic: ADI < 1.32 e CV² ≥ 0.49
- Intermittent: ADI ≥ 1.32 e CV² < 0.49
- Lumpy: ADI ≥ 1.32 e CV² ≥ 0.49

Observações operacionais:

- Se a série tiver forte sazonalidade visual (ex.: picos trimestrais) mesmo com ADI < 1.32, habilitar um flag SAZONALIDADE_SUSPEITA para posterior checagem com STL.
- Se MESES_ANALISADOS_DESDE_1A_OCORRENCIA < 12, restringir a modelos simples (SES/Croston) e marcar baixa confiabilidade.


## 4) Seleção e Operação dos Modelos

Estratégia por classe:

- Smooth e Erratic:
    - Modelo: SES (suavização exponencial simples) para taxa de consumo mensal. Ajuste sobre QTD_MENSAL.
    - Hiperparâmetros: alpha ∈ [0.05, 0.3], selecionar por grid-search temporal.
    - Saída: previsão pontual para próximos N meses (N configurável, padrão 3-6). Projeção da TAXA_MESES_COM_CONSUMO prevista pode ser obtida como fração de meses com previsão > 0 em janelas móveis.
- Intermittent:
    - Modelo: Croston modificado (se disponível, usar implementação validada, ex.: sktime Croston).
    - Operação:
        - Separar tamanho da demanda nos meses com QTD_MENSAL > 0.
        - Estimar intervalo médio entre demandas (em meses).
        - Previsão da taxa de consumo mensal: taxa = previsão_demanda / previsão_intervalo.
    - Hiperparâmetros: alpha_demand e alpha_interval ∈ [0.05, 0.3]; selecionar por validação deslizante.
    - Ajustes:
        - Se ocorrer overshooting de zeros previstos (taxa muito baixa), aplicar Croston-SBA ou Croston-TSB (para probabilidade de ocorrência).
- Lumpy:
    - Modelo: LSTM univariado na série de QTD_MENSAL, com transformação log1p para estabilizar amplitude.
    - Janela: sequence_length ∈  (se < 12 meses úteis, cair para Croston/TSB).[^1][^2]
    - Arquitetura:
        - LSTM(50, return_sequences=True) → Dropout(0.2) → LSTM(50) → Dropout(0.2) → Dense(25) → Dense(1, ReLU).
    - Treino:
        - Split temporal: últimas k janelas para validação (k proporcional a 20% da série).
        - Early stopping por validação (pacience 10).
        - Normalização min-max com fit apenas no treino.
    - Saída:
        - Previsão mensal de QTD_MENSAL.
        - Converter para taxa de consumo mensal via indicador de ocorrência (>0) quando necessário.

Tratamento de zeros e rarefação:

- Se a série tiver longos blocos de zeros (MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA alto), preferir Croston-TSB para modelar probabilidade de ocorrência por mês.
- Se a série for quase binária (0/1), considerar modelo Bernoulli (probabilidade de ocorrência) combinado a um modelo de magnitude para meses com consumo.


## 5) Ensemble e Seleção Dinâmica

Objetivo: combinar previsões para robustez.

Estratégias:

- Weighted Average (padrão):
    - Peso_i = 1 / (MASE_i + eps), normalizado entre modelos válidos para o item.
- Stacking (opcional):
    - Meta-learner linear treinado com features: previsões dos modelos base, ADI, CV², taxa histórica de consumo, meses sem ocorrência recentes.
- Seleção dinâmica:
    - Escolher o modelo de menor MASE nas últimas janelas de validação rolling.
    - Regras:
        - Intermittent: preferir Croston/TSB se MASE ≤ MASE_LSTM − delta.
        - Lumpy: preferir LSTM quando houver ≥ 18 pontos mensais não-nulos distribuídos no histórico.


## 6) Validação, Métricas e Confiabilidade

Métricas por item:

- MASE (principal para séries intermitentes)
- RMSE e MAE (complementares)
- Bias (média(pred − real))
- Coverage de IC (percentual de reais dentro do intervalo)

Cálculo de Intervalos de Confiança:

- SES/Croston:
    - Bootstrapping residual por janelas temporais (1.000 reamostragens) para IC 80% e 95%.
- LSTM:
    - Dropout em inferência (MC Dropout) com 100 amostras; IC pelos percentis.

Score de Qualidade por item:

- Quality_Score = 1 / (1 + MASE), truncado em.[^3]
- Flags:
    - BAIXA_CONFIABILIDADE se MESES_ANALISADOS_DESDE_1A_OCORRENCIA < 12 ou MESES_COM_CONSUMO ≤ 2.
    - DRIFT_POSSIVEL se Bias > limiar e tendência recente divergente.


## 7) Saídas e Artefatos Gerados

Saída por item (CSV/Parquet):

- COD_ITEM
- DEMAND_PATTERN
- ADI
- CV2
- MODEL_USED (ou ENSEMBLE)
- PREDICTED_CONSUMPTION_RATE (média mensal prevista)
- CONFIDENCE_INTERVAL_LOWER (80%)
- CONFIDENCE_INTERVAL_UPPER (80%)
- MASE
- BIAS
- QUALITY_SCORE
- MESES_COM_CONSUMO
- MESES_ANALISADOS_DESDE_1A_OCORRENCIA
- MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA
- PRIMEIRA_DATA
- ULTIMA_DATA

Relatório gerencial (JSON/HTML):

- total_items_processed
- distribution_by_pattern (Smooth/Erratic/Intermittent/Lumpy)
- model_performance_summary por classe
- itens_requiring_attention (QUALITY_SCORE < threshold)
- top_items_by_risk (alto valor e baixa confiabilidade)
- parâmetros efetivos usados (alpha, sequence_length, etc.)

Logs e monitoramento:

- Logar versões de modelos, seeds e hiperparâmetros
- Log de desempenho por execução (tempo por etapa, itens por segundo)
- Alertas em caso de falhas de validação


## 8) Orquestração do Pipeline

Ordem de execução:

1. load_data()
2. validate_and_clean()
3. build_monthly_series()
4. compute_indicators()
5. classify_demand_patterns()
6. select_and_fit_models()
7. forecast_per_item()
8. ensemble_and_select()
9. evaluate_and_score()
10. export_outputs()
11. generate_reports()
12. archive_artifacts()

Parâmetros globais:

- horizonte_previsao_meses: 3 (padrão, configurável 1-12)
- min_meses_para_modelo_avancado: 18
- min_meses_com_consumo_para_lstm: 6 não-zero
- alpha_grid: [0.05, 0.1, 0.2, 0.3]
- sequence_length_grid:[^2][^4][^1]
- validation_split_temporal: 0.2
- quality_threshold: 0.7

Erros e exceções:

- Se série vazia após limpeza: classificar como Lumpy, forçar Croston-TSB com alta incerteza.
- Se não houver variação (série constante): usar Naive + IC por ruído histórico mínimo.
- Se LSTM falhar por dados insuficientes: fallback para Croston/TSB.


## 9) Considerações Específicas para o Seu Arquivo

- O arquivo atual possui 22 meses de histórico (2024-01 a 2025-10). Isso permite:
    - SES/Croston com validação rolling.
    - LSTM apenas para itens com pelo menos 18 observações significativas; caso contrário, usar Croston/TSB.
- Itens com movimentação em todos os 22 meses terão TAXA_MESES_COM_CONSUMO próxima de 1; ainda assim, a magnitude da QTD_MENSAL deve ser prevista (SES/LSTM).
- Para itens muito raros (MESES_COM_CONSUMO ≤ 2), priorize Croston-TSB e aumente o intervalo de confiança.


## 10) Interface de Configuração (YAML)

Exemplo:

```yaml
data:
  input_path: "./BASE_HISTORICA.xlsx"
  time_granularity: "M"
  min_months_for_model: 12

models:
  ses:
    alpha_grid: [0.05, 0.1, 0.2, 0.3]
  croston:
    alpha_demand_grid: [0.05, 0.1, 0.2, 0.3]
    alpha_interval_grid: [0.05, 0.1, 0.2, 0.3]
    variant: "standard"  # options: standard, sba, tsb
  lstm:
    sequence_length: 12
    units: 50
    dropout: 0.2
    epochs: 100
    batch_size: 32
    use_mc_dropout: true
ensemble:
  method: "weighted_average"
  validation_metric: "MASE"
validation:
  split_ratio: 0.2
  rolling_windows: 3
output:
  export_format: ["csv", "json"]
  confidence_level: 0.8
  quality_threshold: 0.7
```


## 11) Checklists Operacionais

Checklist de dados:

- [ ] Colunas obrigatórias presentes
- [ ] Datas válidas e ordenáveis
- [ ] Agregação mensal correta
- [ ] Calendário mensal completo por item
- [ ] Cálculo de métricas derivadas (ADI, CV², taxas)

Checklist de modelagem:

- [ ] Classificação por padrão aplicada
- [ ] Seleção de modelo conforme regra
- [ ] Hiperparâmetros otimizados por validação temporal
- [ ] Ensemble/seleção dinâmica aplicada
- [ ] IC gerados

Checklist de qualidade:

- [ ] MASE calculado por item
- [ ] Score de qualidade atribuído
- [ ] Itens com baixa confiabilidade listados
- [ ] Logs e parâmetros salvos para auditoria

Essas instruções fornecem o detalhamento operacional que o agente de automação precisa para construir e executar o pipeline ponta a ponta, lidando minuciosamente com a intermitência e os períodos sem ocorrência para projetar a TAXA DE CONSUMO com robustez.




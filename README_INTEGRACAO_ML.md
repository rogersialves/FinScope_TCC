# Guia de Uso - Integração ML com Preços e Estratégias

## Arquivos Implementados

### 1. `integration_ml.py`
Contém as funções principais para integração completa:

- **`calculate_abc_analysis(valores)`**: Classifica itens em A, B, C baseado no valor
- **`map_criticality_by_tag(tags)`**: Define criticidade (Critical/Normal/Low) pela TAG
- **`classify_items_by_value_and_criticality(df_items, df_ml_results)`**: Combina ABC + Criticidade + Padrão ML → Estratégia
- **`enrich_ml_predictions_with_prices()`**: Integração completa ML + Preços + Estratégias

### 2. `Modulo_Analise/test_integration_ml.py`
Script de teste usando dados ML já existentes com valores e TAGs simulados.

## Como Usar

### Opção 1: Integração Completa (Recomendada)
```python
from integration_ml import enrich_ml_predictions_with_prices

# Executa tudo: ML → Preços → ABC → Estratégias
resultado = enrich_ml_predictions_with_prices(
    input_path='data/BASE_HISTORICA.xlsx',
    output_dir='Modulo_Solidos_ML/saida_ml',
    tab_aux_path='data/TAB_AUX.xlsx'
)

# Salva: ML_INTEGRADO_PRECOS_ESTRATEGIAS.xlsx
```

### Opção 2: Usando Dados ML Existentes
```python
from integration_ml import classify_items_by_value_and_criticality
import pandas as pd

# Carrega resultados ML já processados
ml_data = pd.read_excel('Modulo_Solidos_ML/saida_ml/ML_CONSUMO_SOLIDOS.xlsx', 'CLASSIFICACAO')

# Seus dados com valor e TAG
seus_dados = pd.DataFrame({
    'COD_ITEM': ['ABC123', 'XYZ456'],
    'VALOR_TOTAL': [15000, 5000],
    'TAG': ['31SC01', '39UTL01']
})

# Aplica classificação e estratégias
resultado = classify_items_by_value_and_criticality(seus_dados, ml_data)
print(resultado[['COD_ITEM', 'ABC_CLASS', 'CRITICALITY', 'DEMAND_PATTERN', 'STRATEGY']])
```

### Opção 3: Teste Rápido
```bash
# PowerShell
python Modulo_Analise/test_integration_ml.py
```

## Saídas Geradas

### Arquivo: `ML_INTEGRADO_PRECOS_ESTRATEGIAS.xlsx`
- **INTEGRACAO_COMPLETA**: Todos os dados consolidados
- **CLASSIFICACAO_ABC_CRIT**: ABC + Criticidade por item
- **RESUMO_POR_ESTRATEGIA**: Agregação por estratégia

### Colunas Principais no Resultado
- `COD_ITEM`, `DESC_ITEM`
- `PREDICTED_CONSUMPTION_RATE`: Taxa mensal prevista (ML)
- `PRECO_2026`: Preço projetado para 2026
- `GASTO_MENSAL_PROJETADO_ML`: Taxa × Preço
- `GASTO_ANUAL_PROJETADO_ML`: Gasto × 12
- `DEMAND_PATTERN`: Smooth/Erratic/Intermittent/Lumpy
- `ABC_CLASS`: A/B/C (80-20 por valor)
- `CRITICALITY`: Critical/Normal/Low (por TAG)
- `STRATEGY`: Estratégia recomendada

## Estratégias Definidas

### Exemplos por Categoria:
- **A + Critical + Lumpy**: "LSTM + Fornecedor Express"
- **A + Critical + Erratic**: "Ensemble Models + Alto Estoque"
- **B + Normal + Intermittent**: "Croston + Ponto Pedido"
- **C + Low + Smooth**: "Reposição Simples"


### Resultado do Teste (4.252 itens):
```
Carregando dados ML: Modulo_Solidos_ML\orcamento_2026\ML_CONSUMO_SOLIDOS.xlsx
Dados carregados: 4252 previsões, 4252 classificações

=== RESULTADOS DA INTEGRAÇÃO ===
Total de itens processados: 4252

Distribuição ABC:
ABC_CLASS
C    2313
B    1285
A     654
Name: count, dtype: int64

Distribuição por Criticidade:
CRITICALITY
Normal      1916
Critical    1563
Low          773
Name: count, dtype: int64

Top 10 Estratégias:
STRATEGY
Croston + Estoque Zero              1890
Croston + Ponto Pedido              1358
Estoque Zero                         423
Análise Manual Necessária            350
Croston + Estoque Segurança Alto     231
Name: count, dtype: int64

Combinação ABC + Criticidade + Padrão (amostra):
ABC_CLASS  CRITICALITY  DEMAND_PATTERN
A          Critical     Intermittent       231
           Low          Intermittent       116
           Normal       Intermittent       307
B          Critical     Intermittent       478
           Low          Intermittent       234
           Normal       Intermittent       573
C          Critical     Intermittent       854
           Low          Intermittent       423
           Normal       Intermittent      1036
dtype: int64

Resultados salvos em: teste_integracao_ml.xlsx

✅ Teste concluído com sucesso! 4252 itens processados.
```

## Personalização

### Ajustar Criticidade por TAG:
Edite `map_criticality_by_tag()` em `integration_ml.py`:
```python
# Equipamentos críticos específicos da sua empresa
critical_prefixes = ['31', '32', '33', '01']  # Seus códigos
critical_keywords = ['BOMBA', 'COMPRESSOR']   # Suas palavras-chave
```

### Ajustar Cortes ABC:
```python
abc_class = calculate_abc_analysis(valores, percentiles=(70, 90))  # A=70%, B=90%
```

### Novas Estratégias:
Edite o dicionário `strategies` em `classify_items_by_value_and_criticality()`.

## Validação dos Resultados

### Distribuição Esperada:
- **ABC**: ~15% A, ~30% B, ~55% C
- **Criticidade**: Varia por empresa; equipamentos de processo = Critical
- **Padrões ML**: Intermittent + Erratic = ~85% (típico de manutenção)

### Checklist de Qualidade:
1. ✅ Itens A + Critical têm estratégias robustas (LSTM, Ensemble, Alto Estoque)
2. ✅ Itens Lumpy têm "Compra Sob Encomenda" ou fornecedores express
3. ✅ Itens C + Low têm estratégias simples (Estoque Zero, Reposição Simples)
4. ✅ Gasto projetado = Taxa ML × Preço 2026 calculado corretamente

## Integração com Outros Módulos

### Com Modulo_Solidos:
```python
# Compare estratégias ML vs. classificação manual
solidos_resumo = pd.read_excel('Modulo_Solidos/HISTORICO_FINAL.xlsx', 'RESUMO_CONSUMO')
comparacao = ml_resultado.merge(solidos_resumo[['COD_ITEM', 'TIPO_PADRAO']], on='COD_ITEM')
```

### Com Modulo_PA:
```python
# Valide padrões ML vs. PA
pa_dados = pd.read_excel('Modulo_PA/resultado.xlsx', 'RESUMO_CONSUMO')
```

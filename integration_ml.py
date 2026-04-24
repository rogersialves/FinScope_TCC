"""
Integração entre Modulo_Solidos_ML e outros módulos do FinScope.
Combina previsões ML com análises de preço, criticidade e valor.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Tuple, Optional

from Modulo_Solidos_ML.pipeline import run_pipeline, Config
from fin_utils import compute_price_metrics, filter_por_fonte, filter_por_conta_contabil, remover_por_grupo_budget


def calculate_abc_analysis(valores: pd.Series, percentiles: Tuple[float, float] = (80, 95)) -> pd.Series:
    """
    Classifica itens em categorias ABC baseado no valor acumulado.
    
    Args:
        valores: Série com valores totais por item
        percentiles: Cortes para A (80%) e B (95%), restante vira C
    
    Returns:
        Série com classificação 'A', 'B' ou 'C'
    """
    # Remove valores nulos e ordena decrescente
    vals = valores.fillna(0).sort_values(ascending=False)
    cumsum = vals.cumsum()
    total = vals.sum()
    
    if total == 0:
        return pd.Series(['C'] * len(valores), index=valores.index)
    
    cumsum_pct = (cumsum / total) * 100
    
    # Classificação
    abc_class = pd.Series(['C'] * len(vals), index=vals.index)
    abc_class[cumsum_pct <= percentiles[0]] = 'A'
    abc_class[(cumsum_pct > percentiles[0]) & (cumsum_pct <= percentiles[1])] = 'B'
    
    # Retorna na ordem original
    return abc_class.reindex(valores.index, fill_value='C')


def map_criticality_by_tag(tags: pd.Series) -> pd.Series:
    """
    Mapeia criticidade baseado na TAG do item.
    
    Args:
        tags: Série com TAGs dos itens
    
    Returns:
        Série com criticidade 'Critical', 'Normal' ou 'Low'
    """
    def _classify_tag(tag):
        if pd.isna(tag) or not isinstance(tag, str):
            return 'Normal'
        
        tag = str(tag).upper().strip()
        
        # Equipamentos críticos (bombas, compressores, turbinas)
        critical_prefixes = ['31', '32', '33', '01']  # Adaptável conforme padrões da empresa
        critical_keywords = ['BOMBA', 'COMPRESSOR', 'TURBINA', 'MOTOR', 'VALVULA']
        
        # Verifica prefixo crítico
        for prefix in critical_prefixes:
            if tag.startswith(prefix):
                return 'Critical'
        
        # Verifica palavras-chave críticas (se TAG contiver descrição)
        for keyword in critical_keywords:
            if keyword in tag:
                return 'Critical'
        
        # Utilidades e auxiliares
        utility_prefixes = ['39', '49', '99']
        for prefix in utility_prefixes:
            if tag.startswith(prefix):
                return 'Low'
        
        return 'Normal'
    
    return tags.apply(_classify_tag)


def classify_items_by_value_and_criticality(df_items: pd.DataFrame, df_ml_results: pd.DataFrame) -> pd.DataFrame:
    """
    Combina padrões ML com valor/criticidade do item para definir estratégias.
    
    Args:
        df_items: DataFrame com COD_ITEM, VALOR_TOTAL, TAG
        df_ml_results: DataFrame com COD_ITEM, DEMAND_PATTERN da classificação ML
    
    Returns:
        DataFrame enriquecido com ABC_CLASS, CRITICALITY, STRATEGY
    """
    # Análise ABC
    if 'VALOR_TOTAL' in df_items.columns:
        df_items['ABC_CLASS'] = calculate_abc_analysis(df_items['VALOR_TOTAL'])
    else:
        df_items['ABC_CLASS'] = 'C'
    
    # Criticidade por TAG
    if 'TAG' in df_items.columns:
        df_items['CRITICALITY'] = map_criticality_by_tag(df_items['TAG'])
    else:
        df_items['CRITICALITY'] = 'Normal'
    
    # Merge com resultados ML
    df_enriched = df_items.merge(
        df_ml_results[['COD_ITEM', 'DEMAND_PATTERN']], 
        on='COD_ITEM', 
        how='left'
    )
    df_enriched['DEMAND_PATTERN'] = df_enriched['DEMAND_PATTERN'].fillna('Intermittent')
    
    # Estratégias diferenciadas por quadrante
    strategies = {
        ('A', 'Critical', 'Lumpy'): 'LSTM + Fornecedor Express',
        ('A', 'Critical', 'Erratic'): 'Ensemble Models + Alto Estoque',
        ('A', 'Critical', 'Intermittent'): 'Croston + Estoque Segurança Alto',
        ('A', 'Critical', 'Smooth'): 'SES + Reposição Automática',
        
        ('A', 'Normal', 'Lumpy'): 'Croston + Fornecedor Confiável',
        ('A', 'Normal', 'Erratic'): 'SES + Estoque Médio',
        ('A', 'Normal', 'Intermittent'): 'Croston + Ponto Pedido',
        ('A', 'Normal', 'Smooth'): 'SES + Reposição Fixa',
        
        ('B', 'Critical', 'Lumpy'): 'Croston + Backup Fornecedor',
        ('B', 'Critical', 'Erratic'): 'SES + Estoque Médio',
        ('B', 'Critical', 'Intermittent'): 'Croston + Ponto Pedido',
        ('B', 'Critical', 'Smooth'): 'SES + Reposição Fixa',
        
        ('B', 'Normal', 'Lumpy'): 'Croston + Estoque Mínimo',
        ('B', 'Normal', 'Erratic'): 'SES + Estoque Baixo',
        ('B', 'Normal', 'Intermittent'): 'Croston + Ponto Pedido',
        ('B', 'Normal', 'Smooth'): 'SES + Reposição Fixa',
        
        # Categoria C: estratégias mais simples
        ('C', 'Critical', 'Lumpy'): 'Compra Sob Encomenda',
        ('C', 'Critical', 'Erratic'): 'SES + Estoque Baixo',
        ('C', 'Critical', 'Intermittent'): 'Croston + Estoque Zero',
        ('C', 'Critical', 'Smooth'): 'Reposição Fixa',
        
        ('C', 'Normal', 'Lumpy'): 'Compra Sob Encomenda',
        ('C', 'Normal', 'Erratic'): 'SES + Estoque Zero',
        ('C', 'Normal', 'Intermittent'): 'Croston + Estoque Zero',
        ('C', 'Normal', 'Smooth'): 'Reposição Fixa',
        
        ('C', 'Low', 'Lumpy'): 'Compra Sob Encomenda',
        ('C', 'Low', 'Erratic'): 'Estoque Zero',
        ('C', 'Low', 'Intermittent'): 'Estoque Zero',
        ('C', 'Low', 'Smooth'): 'Reposição Simples',
    }
    
    def _get_strategy(row):
        key = (row['ABC_CLASS'], row['CRITICALITY'], row['DEMAND_PATTERN'])
        return strategies.get(key, 'Análise Manual Necessária')
    
    df_enriched['STRATEGY'] = df_enriched.apply(_get_strategy, axis=1)
    
    return df_enriched


def enrich_ml_predictions_with_prices(
    input_path: str = 'data/BASE_HISTORICA.xlsx', 
    output_dir: str = 'Modulo_Solidos_ML/saida_ml',
    tab_aux_path: Optional[str] = 'data/TAB_AUX.xlsx'
) -> pd.DataFrame:
    """
    Integra previsões ML com métricas de preço para gerar projeções de gasto.
    
    Args:
        input_path: Caminho para data/BASE_HISTORICA.xlsx
        output_dir: Diretório de saída do ML
        tab_aux_path: Caminho para TAB_AUX.xlsx (para IPCA)
    
    Returns:
        DataFrame com previsões enriched: COD_ITEM, PREDICTED_CONSUMPTION_RATE, 
        PRECO_2026, GASTO_PROJETADO_ML, STRATEGY, etc.
    """
    # 1. Executa pipeline ML
    cfg = Config(horizonte_previsao_meses=12)  # 1 ano para análise anual
    ml_output_path = run_pipeline(input_path, output_dir, cfg)
    
    # 2. Carrega resultados ML
    ml_excel = pd.ExcelFile(ml_output_path)
    previsoes = pd.read_excel(ml_excel, sheet_name='PREVISOES')
    classificacao = pd.read_excel(ml_excel, sheet_name='CLASSIFICACAO')
    indicadores = pd.read_excel(ml_excel, sheet_name='INDICADORES')
    
    # 3. Carrega dados originais para cálculo de preços
    historico_df = pd.read_excel(input_path, sheet_name=0)  # primeira aba
    
    # Aplicar mesmos filtros do ML
    historico_df, _ = filter_por_fonte(historico_df)
    historico_df, _ = filter_por_conta_contabil(historico_df)
    historico_df, _ = remover_por_grupo_budget(historico_df, col_override='GRUPO_BUDGET')
    
    # Constrói séries mensais para preços
    data_entrega = pd.to_datetime(historico_df['DATA_ENTREGA'], errors='coerce')
    historico_df = historico_df.assign(DATA_ENTREGA=data_entrega)
    historico_df = historico_df.dropna(subset=['DATA_ENTREGA'])
    historico_df['ANO_MES'] = data_entrega.loc[historico_df.index].dt.to_period('M')
    
    # Agregações mensais
    mensal_qtd = (historico_df.groupby(['COD_ITEM', 'ANO_MES'])['QUANTIDADE']
                  .sum().rename('QTD_MENSAL').reset_index())
    mensal_valor = (historico_df.groupby(['COD_ITEM', 'ANO_MES'])['VALOR']
                    .sum().rename('VALOR_MENSAL').reset_index())
    
    # 4. Calcula métricas de preço
    tab_aux_full_path = Path(input_path).parent / tab_aux_path if tab_aux_path else None
    price_data = compute_price_metrics(
        mensal_qtd, mensal_valor, 
        tab_aux_full_path, 
        ano_base=2025
    )
    
    # 5. Combina tudo
    merged = previsoes.merge(price_data, on='COD_ITEM', how='left')
    merged = merged.merge(classificacao[['COD_ITEM', 'DEMAND_PATTERN']], on='COD_ITEM', how='left')
    
    # 6. Calcula gasto projetado
    merged['PRECO_2026'] = merged['PRECO_2026'].fillna(0)
    merged['PREDICTED_CONSUMPTION_RATE'] = merged['PREDICTED_CONSUMPTION_RATE'].fillna(0)
    
    # Gasto mensal projetado = taxa mensal × preço 2026
    merged['GASTO_MENSAL_PROJETADO_ML'] = (
        merged['PREDICTED_CONSUMPTION_RATE'] * merged['PRECO_2026']
    )
    
    # Gasto anual projetado
    merged['GASTO_ANUAL_PROJETADO_ML'] = merged['GASTO_MENSAL_PROJETADO_ML'] * 12
    
    # 7. Análise ABC e estratégias
    # Para ABC, usa valor histórico total
    valor_historico = (historico_df.groupby('COD_ITEM')['VALOR']
                       .sum().rename('VALOR_TOTAL').reset_index())
    
    # Para TAG, pega do histórico (assumindo que existe)
    tag_map = None
    if 'TAG' in historico_df.columns:
        tag_map = (historico_df.groupby('COD_ITEM')['TAG']
                   .first().rename('TAG').reset_index())
    
    # Combina dados para classificação
    df_for_classification = merged[['COD_ITEM', 'DEMAND_PATTERN']].copy()
    df_for_classification = df_for_classification.merge(valor_historico, on='COD_ITEM', how='left')
    if tag_map is not None:
        df_for_classification = df_for_classification.merge(tag_map, on='COD_ITEM', how='left')
    
    # Aplica classificação por valor e criticidade
    df_classified = classify_items_by_value_and_criticality(
        df_for_classification, 
        classificacao
    )
    
    # 8. Merge final
    final_result = merged.merge(
        df_classified[['COD_ITEM', 'ABC_CLASS', 'CRITICALITY', 'STRATEGY']], 
        on='COD_ITEM', 
        how='left'
    )
    
    # 9. Salva resultado integrado
    output_path = Path(output_dir) / 'ML_INTEGRADO_PRECOS_ESTRATEGIAS.xlsx'
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        final_result.to_excel(writer, sheet_name='INTEGRACAO_COMPLETA', index=False)
        df_classified.to_excel(writer, sheet_name='CLASSIFICACAO_ABC_CRIT', index=False)
        
        # Resumo por estratégia
        resumo_estrategia = (final_result.groupby('STRATEGY')
                            .agg({
                                'COD_ITEM': 'count',
                                'GASTO_ANUAL_PROJETADO_ML': 'sum'
                            })
                            .rename(columns={'COD_ITEM': 'QTD_ITENS'})
                            .reset_index())
        resumo_estrategia.to_excel(writer, sheet_name='RESUMO_POR_ESTRATEGIA', index=False)
    
    print(f"Integração completa salva em: {output_path}")
    return final_result


if __name__ == '__main__':
    # Exemplo de uso
    resultado = enrich_ml_predictions_with_prices()
    print(f"Processados {len(resultado)} itens com integração ML + Preços + Estratégias")

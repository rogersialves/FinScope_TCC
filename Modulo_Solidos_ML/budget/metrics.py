"""
Métricas de uso e classificação de itens.

Este módulo contém funções para calcular indicadores de consumo,
métricas de uso e classificação de padrões de demanda.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from typing import Any

from core.logger import get_logger

_logger = get_logger(__name__)


def _months_between(p_ini: pd.Period, p_fim: pd.Period) -> int:
    """Calcula número de meses entre dois períodos."""
    return (p_fim.year - p_ini.year) * 12 + (p_fim.month - p_ini.month)


def _to_period_month(series: pd.Series) -> pd.Series:
    """Converte série para PeriodIndex mensal."""
    try:
        return pd.PeriodIndex(series, freq='M').to_series().reset_index(drop=True)
    except (TypeError, ValueError):
        return pd.PeriodIndex(pd.to_datetime(series, errors='coerce'), freq='M').to_series().reset_index(drop=True)


def build_indicadores(mensal_qtd: pd.DataFrame, df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula indicadores de consumo otimizados.
    
    Args:
        mensal_qtd: DataFrame com série mensal de quantidades
        df_raw: DataFrame com dados brutos originais
        
    Returns:
        DataFrame com indicadores por COD_ITEM
    """
    m = mensal_qtd.copy()
    cols = [
        'COD_ITEM', 'MESES_ANALISADOS_DESDE_1A_OCORRENCIA', 'MESES_COM_CONSUMO',
        'TAXA_MESES_COM_CONSUMO', 'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA',
        'PRIMEIRA_DATA', 'ULTIMA_DATA', 'DESC_ITEM'
    ]
    
    if m.empty:
        return pd.DataFrame(columns=cols)
    
    ultimo_global = m['ANO_MES'].max()

    meses_com = (m[m['QTD_MENSAL'] > 0]
                   .groupby('COD_ITEM')['ANO_MES']
                   .nunique()
                   .rename('MESES_COM_CONSUMO'))

    first_month = (m[m['QTD_MENSAL'] > 0].groupby('COD_ITEM')['ANO_MES'].min().rename('PRIMEIRO_MES_CONS'))
    last_month = (m[m['QTD_MENSAL'] > 0].groupby('COD_ITEM')['ANO_MES'].max().rename('ULTIMO_MES_CONS'))

    r = df_raw.copy()
    r['DATA_ENTREGA'] = pd.to_datetime(r['DATA_ENTREGA'].astype(str), errors='coerce')
    real_first = (r[r['QUANTIDADE'] > 0].groupby('COD_ITEM')['DATA_ENTREGA'].min().rename('PRIMEIRA_DATA'))
    real_last = (r[r['QUANTIDADE'] > 0].groupby('COD_ITEM')['DATA_ENTREGA'].max().rename('ULTIMA_DATA'))
    desc = (r.groupby('COD_ITEM')['DESC_ITEM'].agg(
        lambda s: s.dropna().iloc[0] if not s.dropna().empty else None
    ))

    base = pd.DataFrame({'COD_ITEM': m['COD_ITEM'].unique()}).set_index('COD_ITEM')
    base = base.join([meses_com, first_month, last_month, real_first, real_last, desc])

    base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'] = base.apply(
        lambda x: (_months_between(x['PRIMEIRO_MES_CONS'], ultimo_global) + 1) 
        if pd.notna(x['PRIMEIRO_MES_CONS']) else 0,
        axis=1
    )

    base['MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA'] = base.apply(
        lambda x: _months_between(x['ULTIMO_MES_CONS'], ultimo_global) 
        if pd.notna(x['ULTIMO_MES_CONS']) else np.nan,
        axis=1
    )

    base['TAXA_MESES_COM_CONSUMO'] = np.where(
        base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'] > 0,
        base['MESES_COM_CONSUMO'] / base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'],
        np.nan
    )

    base = base.reset_index()
    return base[cols]


def build_usage_metrics(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas de uso e classifica itens.
    
    Args:
        mensal_qtd: DataFrame com série mensal de quantidades
        
    Returns:
        DataFrame com métricas e CLASSE_USO por item
    """
    usage_cols = [
        'COD_ITEM', 'OCCURRENCES_TOTAL', 'OCCURRENCES_12M', 'MESES_DESDE_PRIMEIRO',
        'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA', 'USE_RATE', 'CLASSE_USO',
        'INTERVALO_MEDIO_MESES', 'QTD_MEDIA_OCORRENCIA', 'QTD_ULTIMA_OCORRENCIA',
        'ULTIMO_MES_ORDINAL', 'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO'
    ]
    
    if mensal_qtd is None or mensal_qtd.empty:
        _logger.warning("mensal_qtd está vazio, retornando DataFrame vazio")
        return pd.DataFrame(columns=usage_cols)

    df = mensal_qtd.copy()
    df['COD_ITEM'] = df['COD_ITEM'].astype(str)
    
    if 'ANO_MES' not in df.columns:
        _logger.warning("mensal_qtd não possui coluna ANO_MES")
        return pd.DataFrame(columns=usage_cols)

    try:
        df['ANO_MES'] = pd.PeriodIndex(df['ANO_MES'], freq='M')
    except (TypeError, ValueError):
        df['ANO_MES'] = pd.PeriodIndex(pd.to_datetime(df['ANO_MES'], errors='coerce'), freq='M')

    df = df.sort_values(['COD_ITEM', 'ANO_MES'])
    ultimo = df['ANO_MES'].max()
    
    inicio_por_item = df[df['QTD_MENSAL'] > 0].groupby('COD_ITEM')['ANO_MES'].min()
    fim_por_item = df[df['QTD_MENSAL'] > 0].groupby('COD_ITEM')['ANO_MES'].max()
    
    occ_total = (df.assign(HAS_USE=(df['QTD_MENSAL'] > 0).astype(int))
                   .groupby('COD_ITEM')['HAS_USE']
                   .sum()
                   .rename('OCCURRENCES_TOTAL'))
    
    ult_12 = ultimo - 11
    occ_12m = (df[df['ANO_MES'] >= ult_12]
               .assign(HAS_USE=(df['QTD_MENSAL'] > 0).astype(int))
               .groupby('COD_ITEM')['HAS_USE']
               .sum()
               .rename('OCCURRENCES_12M'))
    
    base = pd.DataFrame({'COD_ITEM': df['COD_ITEM'].unique()}).set_index('COD_ITEM')
    base = base.join(occ_total).join(occ_12m)
    base['PRIMEIRO_MES'] = inicio_por_item
    base['ULTIMO_MES_POS'] = fim_por_item
    
    base['MESES_DESDE_PRIMEIRO'] = base.apply(
        lambda r: (_months_between(r['PRIMEIRO_MES'], ultimo) + 1) 
        if pd.notna(r['PRIMEIRO_MES']) else 0,
        axis=1
    )
    
    base['USE_RATE'] = np.where(
        base['MESES_DESDE_PRIMEIRO'] > 0,
        base['OCCURRENCES_TOTAL'] / base['MESES_DESDE_PRIMEIRO'],
        0.0
    )
    
    def _classify(rate: float, occ12: int) -> str:
        if occ12 <= 2 or rate <= 0.10:
            return 'Muito Raro'
        elif rate <= 0.25:
            return 'Raro'
        elif rate <= 0.50:
            return 'Ocasional'
        elif rate < 0.80:
            return 'Intermitente'
        return 'Rotineiro'
    
    base['MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA'] = base.apply(
        lambda r: _months_between(r['ULTIMO_MES_POS'], ultimo) if pd.notna(r['ULTIMO_MES_POS']) else np.inf,
        axis=1
    )
    
    base['CLASSE_USO'] = base.apply(lambda r: _classify(r['USE_RATE'], r['OCCURRENCES_12M']), axis=1)

    # Métricas de intervalo e quantidade
    positivos = df[df['QTD_MENSAL'] > 0].copy()
    if not positivos.empty:
        positivos['ORDINAL'] = positivos['ANO_MES'].astype(int)

        def _avg_interval(series: pd.Series) -> float:
            if len(series) < 2:
                return np.nan
            vals = np.sort(series.to_numpy())
            diffs = np.diff(vals)
            diffs = diffs[diffs > 0]
            return float(np.mean(diffs)) if len(diffs) else np.nan

        intervalo_medio = positivos.groupby('COD_ITEM')['ORDINAL'].apply(_avg_interval).rename('INTERVALO_MEDIO_MESES')
        qtd_media = positivos.groupby('COD_ITEM')['QTD_MENSAL'].mean().rename('QTD_MEDIA_OCORRENCIA')
        
        ultimo_evento = (positivos.sort_values('ANO_MES')
                                   .groupby('COD_ITEM')
                                   .tail(1)
                                   .set_index('COD_ITEM')[['QTD_MENSAL', 'ANO_MES', 'ORDINAL']])
        ultimo_evento = ultimo_evento.rename(columns={
            'QTD_MENSAL': 'QTD_ULTIMA_OCORRENCIA',
            'ORDINAL': 'ULTIMO_MES_ORDINAL'
        })
        # Converter Period para Timestamp antes de acessar .dt.year/.dt.month
        _ts = ultimo_evento['ANO_MES'].dt.to_timestamp()  # type: ignore[union-attr]
        ultimo_evento['ULTIMO_ANO_CONSUMO'] = _ts.dt.year.astype('Int64')  # type: ignore[union-attr]
        ultimo_evento['ULTIMO_MES_CONSUMO'] = _ts.dt.month.astype('Int64')  # type: ignore[union-attr]
        ultimo_evento = ultimo_evento[['QTD_ULTIMA_OCORRENCIA', 'ULTIMO_MES_ORDINAL',
                                       'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO']]
        
        base = base.join(intervalo_medio).join(qtd_media).join(ultimo_evento)
    
    base = base.reset_index()
    _logger.info("build_usage_metrics gerou %d itens", len(base))
    
    return base[usage_cols]


def build_monthly_distribution(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula distribuição mensal histórica por item.
    
    Args:
        mensal_qtd: DataFrame com série mensal de quantidades
        
    Returns:
        DataFrame com pesos proporcionais por mês
    """
    cols = ['COD_ITEM', 'MES', 'PESO', 'QTD_TOTAL', 'OCORRENCIAS', 'MES_MAIS_RECENTE']
    
    if mensal_qtd is None or mensal_qtd.empty:
        return pd.DataFrame(columns=cols)

    df = mensal_qtd.copy()
    if 'ANO_MES' not in df.columns:
        return pd.DataFrame(columns=cols)

    df['ANO_MES'] = _to_period_month(df['ANO_MES'])
    # Converter Period para Timestamp para acessar .dt.month
    df['MES'] = df['ANO_MES'].dt.to_timestamp().dt.month  # type: ignore[union-attr]
    ultimo_periodo = df['ANO_MES'].max()
    
    positivos = df[df['QTD_MENSAL'] > 0].copy()
    if positivos.empty:
        return pd.DataFrame(columns=cols)

    positivos['DIST_MESES'] = positivos['ANO_MES'].apply(lambda p: _months_between(p, ultimo_periodo))
    positivos['RECENCY_FACTOR'] = np.exp(-positivos['DIST_MESES'] / 12.0)
    positivos['PESO_BRUTO'] = positivos['QTD_MENSAL'].astype(float) * positivos['RECENCY_FACTOR']
    positivos['COD_ITEM'] = positivos['COD_ITEM'].astype(str)

    agrupado = (positivos.groupby(['COD_ITEM', 'MES'])
                          .agg(OCORRENCIAS=('QTD_MENSAL', 'count'),
                               QTD_TOTAL=('QTD_MENSAL', 'sum'),
                               PESO_BRUTO=('PESO_BRUTO', 'sum'))
                          .reset_index())
    
    totais = (agrupado.groupby('COD_ITEM')
                        .agg(TOTAL_QTD_ITEM=('QTD_TOTAL', 'sum'),
                             TOTAL_OCORRENCIAS_ITEM=('OCORRENCIAS', 'sum'),
                             TOTAL_PESO_BRUTO=('PESO_BRUTO', 'sum'))
                        .reset_index())
    
    distrib = agrupado.merge(totais, on='COD_ITEM', how='left')
    distrib['PESO'] = np.where(
        distrib['TOTAL_PESO_BRUTO'] > 0,
        distrib['PESO_BRUTO'] / distrib['TOTAL_PESO_BRUTO'],
        0.0
    )
    
    # Converter Period para Timestamp para acessar .dt.month
    ultimo_mes = (positivos.groupby('COD_ITEM')['ANO_MES']
                            .max()
                            .apply(lambda p: p.to_timestamp().month)
                            .rename('MES_MAIS_RECENTE')
                            .reset_index())
    
    distrib = distrib.merge(ultimo_mes, on='COD_ITEM', how='left')
    return distrib[cols]


def compute_adi_cv2_local(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula ADI (Average Demand Interval) e CV² (Coefficient of Variation squared).
    
    Args:
        mensal_qtd: DataFrame com série mensal de quantidades
        
    Returns:
        DataFrame com ADI e CV2 por item
    """
    if mensal_qtd is None or mensal_qtd.empty:
        return pd.DataFrame(columns=['COD_ITEM', 'ADI', 'CV2'])
    
    df = mensal_qtd.copy()
    df['COD_ITEM'] = df['COD_ITEM'].astype(str)
    
    positivos = df[df['QTD_MENSAL'] > 0].copy()
    if positivos.empty:
        return pd.DataFrame(columns=['COD_ITEM', 'ADI', 'CV2'])
    
    try:
        positivos['ANO_MES'] = pd.PeriodIndex(positivos['ANO_MES'], freq='M')
    except (TypeError, ValueError):
        positivos['ANO_MES'] = pd.PeriodIndex(pd.to_datetime(positivos['ANO_MES'], errors='coerce'), freq='M')
    
    positivos['ORDINAL'] = positivos['ANO_MES'].astype(int)
    positivos = positivos.sort_values(['COD_ITEM', 'ORDINAL'])
    
    def _calc_adi(g: pd.DataFrame) -> float:
        if len(g) < 2:
            return np.nan
        ords = g['ORDINAL'].to_numpy()
        diffs = np.diff(ords)
        return float(np.mean(diffs)) if len(diffs) else np.nan
    
    def _calc_cv2(g: pd.DataFrame) -> float:
        vals = g['QTD_MENSAL'].to_numpy()
        if len(vals) < 2:
            return np.nan
        mean = np.mean(vals)
        if mean <= 0:
            return np.nan
        return float((np.std(vals, ddof=1) / mean) ** 2)
    
    adi = positivos.groupby('COD_ITEM')[['ORDINAL', 'QTD_MENSAL']].apply(_calc_adi).rename('ADI')
    cv2 = positivos.groupby('COD_ITEM')[['QTD_MENSAL']].apply(_calc_cv2).rename('CV2')
    
    result = pd.DataFrame({'COD_ITEM': positivos['COD_ITEM'].unique()}).set_index('COD_ITEM')
    result = result.join(adi).join(cv2).reset_index()
    
    return result


def build_selector_features(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói features para seleção de modelo ML.
    
    Args:
        mensal_qtd: DataFrame com série mensal de quantidades
        
    Returns:
        DataFrame com features por item (TREND_STRENGTH, SEASONALITY_STRENGTH, etc.)
    """
    if mensal_qtd is None or mensal_qtd.empty:
        return pd.DataFrame(columns=['COD_ITEM', 'TREND_STRENGTH', 'SEASONALITY_STRENGTH'])
    
    df = mensal_qtd.copy()
    df['COD_ITEM'] = df['COD_ITEM'].astype(str)
    
    try:
        df['ANO_MES'] = pd.PeriodIndex(df['ANO_MES'], freq='M')
    except (TypeError, ValueError):
        df['ANO_MES'] = pd.PeriodIndex(pd.to_datetime(df['ANO_MES'], errors='coerce'), freq='M')
    
    df['ORDINAL'] = df['ANO_MES'].astype(int)
    df = df.sort_values(['COD_ITEM', 'ORDINAL'])
    
    def _trend_strength(g: pd.DataFrame) -> float:
        if len(g) < 3:
            return 0.0
        x = g['ORDINAL'].to_numpy()
        y = g['QTD_MENSAL'].to_numpy()
        x_norm = x - x.mean()
        y_norm = y - y.mean()
        
        denom = np.sum(x_norm ** 2)
        if denom == 0:
            return 0.0
        
        slope = np.sum(x_norm * y_norm) / denom
        return abs(slope)
    
    def _seasonality_strength(g: pd.DataFrame) -> float:
        if len(g) < 12:
            return 0.0
        g = g.copy()
        # Converter Period para Timestamp para acessar .dt.month
        g['MES'] = g['ANO_MES'].dt.to_timestamp().dt.month  # type: ignore[union-attr]
        monthly_mean = g.groupby('MES')['QTD_MENSAL'].mean()
        overall_mean = g['QTD_MENSAL'].mean()
        
        if overall_mean <= 0:
            return 0.0
        
        cv_monthly = monthly_mean.std() / overall_mean if overall_mean > 0 else 0
        return min(cv_monthly, 1.0)
    
    trend = df.groupby('COD_ITEM')[['ORDINAL', 'QTD_MENSAL']].apply(_trend_strength).rename('TREND_STRENGTH')
    season = df.groupby('COD_ITEM')[['ANO_MES', 'QTD_MENSAL']].apply(_seasonality_strength).rename('SEASONALITY_STRENGTH')
    
    result = pd.DataFrame({'COD_ITEM': df['COD_ITEM'].unique()}).set_index('COD_ITEM')
    result = result.join(trend).join(season).reset_index()
    
    return result


def calculate_monthly_metrics(df: pd.DataFrame, date_col: str = "DATA") -> pd.DataFrame:
    """Calcula métricas mensais a partir do DataFrame."""
    if df.empty:
        return df
    
    # Garantir que a coluna de data é datetime
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    
    # Extrair ano e mês usando .dt accessor (não .year/.month diretamente)
    df["ANO"] = df[date_col].dt.year  # type: ignore[union-attr]
    df["MES"] = df[date_col].dt.month  # type: ignore[union-attr]
    
    return df


def _extract_year_month(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Extrai ano e mês de uma série de datas."""
    dt_series = pd.to_datetime(series, errors="coerce")
    return dt_series.dt.year, dt_series.dt.month


def aggregate_by_period(
    df: pd.DataFrame,
    group_cols: list[str],
    value_col: str,
    agg_func: str = "sum"
) -> pd.DataFrame:
    """Agrega dados por período."""
    if df.empty:
        return df
    
    grouped = df.groupby(group_cols, as_index=False)
    
    # Usar agg em vez de apply com include_groups
    if agg_func == "sum":
        result = grouped.agg({value_col: "sum"})
    elif agg_func == "mean":
        result = grouped.agg({value_col: "mean"})
    elif agg_func == "count":
        result = grouped.agg({value_col: "count"})
    else:
        result = grouped.agg({value_col: agg_func})
    
    return result


def calculate_period_totals(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_cols: list[str] | None = None
) -> pd.DataFrame:
    """Calcula totais por período."""
    if df.empty:
        return df
    
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df["_PERIOD"] = df[date_col].dt.to_period("M")  # type: ignore[union-attr]
    
    if group_cols:
        all_cols = group_cols + ["_PERIOD"]
    else:
        all_cols = ["_PERIOD"]
    
    # Usar agg em vez de apply
    result = df.groupby(all_cols, as_index=False).agg({value_col: "sum"})
    
    return result

import json
import calendar
import math
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Optional, Any
import warnings
warnings.filterwarnings('ignore')

# Ajusta sys.path durante execução direta
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Import do logger centralizado
from core.logger import get_logger

logger = get_logger(__name__)

# Normaliza encoding das saídas para evitar caracteres corrompidos em terminais não UTF-8
try:
    if hasattr(sys.stdout, "reconfigure"):
        getattr(sys.stdout, "reconfigure")(encoding="utf-8", errors="replace")
except Exception as e:
    logger.debug(f"Não foi possível reconfigurar stdout: {e}")

from core.fin_params import get_param
from Modulo_Solidos_ML.data_import import load_and_preprocess, monthly_qtd_val, monthly_price
from Modulo_Solidos_ML.fix_excel_abas import append_ml_tabs_to_excel

# Ordem amigável para classes de uso (para relatórios/ordenção opcional)
ORDER_MAP: Dict[str, int] = {
    'Rotineiro': 1,
    'Intermitente': 2,
    'Ocasional': 3,
    'Raro': 4,
    'Muito Raro': 5,
}

ORDER_MAP_INDUSTRIAL: Dict[str, int] = {
    'ROTINEIRO': 1,
    'INTERMITENTE': 2,
    'OCASIONAL': 3,
    'RARO': 4,
    'MUITO RARO': 5,
    'SEM_CLASSIFICACAO': 99
}

QTD_BASE_COL = 'QTD_PROJETADA_BASE_2026'
QTD_FINAL_COL = 'QTD_PROJETADA_COM_MARGEM'
QTD_FINAL_LEGACY_COL = 'QTD_PROJETADA_2026'

@dataclass
class BudgetConfig:
    ano_orcamento: int = 2026
    margem_seguranca_pct: float = 0.15
    ajuste_inflacao_anual: float = 0.035
    horizonte_meses: int = 12
    modo_orcamento: str = 'essencial'  # 'essencial' | 'fidelidade' | 'inteligente'

# --- Utilidades para IPCA (mantidas) ---
def _load_ipca_fatores(aux_path: Path) -> pd.DataFrame | None:
    try:
        def _usecols(name: str) -> bool:
            n = str(name).strip().upper()
            return n == 'ANO' or 'IPCA' in n
        df = pd.read_excel(aux_path, sheet_name='TABS_AUX', usecols=_usecols)
        col_ano = next((c for c in df.columns if str(c).strip().upper() == 'ANO'), None)
        col_ipc = next((c for c in df.columns if 'IPCA' in str(c).upper()), None)
        if not col_ano or not col_ipc:
            return None
        out = df[[col_ano, col_ipc]].dropna().copy()
        out.columns = ['ANO', 'IPCA_PCT']
        out['ANO'] = out['ANO'].astype(int)
        out['IPCA_FATOR'] = 1.0 + (pd.to_numeric(out['IPCA_PCT'], errors='coerce') / 100.0)
        return out[['ANO','IPCA_FATOR']]
    except FileNotFoundError:
        logger.debug(f"Arquivo IPCA não encontrado: {aux_path}")
        return None
    except Exception as e:
        logger.warning(f"Erro ao carregar fatores IPCA: {e}")
        return None

def _fator_acumulado_ipca(ipca: pd.DataFrame | None, ano_origem: int, ano_dest: int) -> float:
    if ipca is None or pd.isna(ano_origem) or pd.isna(ano_dest):
        return 1.0
    if ano_dest <= ano_origem:
        return 1.0
    rng = ipca[(ipca['ANO'] > int(ano_origem)) & (ipca['ANO'] <= int(ano_dest))]
    if rng.empty:
        return 1.0
    return float(np.prod(rng['IPCA_FATOR'].astype(float).to_numpy()))

# ===== NOVO: Métricas de uso e classificação =====
def _months_between(p_ini: pd.Period, p_fim: pd.Period) -> int:
    return (p_fim.year - p_ini.year) * 12 + (p_fim.month - p_ini.month)

def build_indicadores(mensal_qtd: pd.DataFrame, df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    INDICADORES otimizados - calcula até último mês global,
    mas sem expansão desnecessária que causava OOM.
    """
    m = mensal_qtd.copy()
    if m.empty:
        return pd.DataFrame(columns=[
            'COD_ITEM','MESES_ANALISADOS_DESDE_1A_OCORRENCIA','MESES_COM_CONSUMO',
            'TAXA_MESES_COM_CONSUMO','MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA',
            'PRIMEIRA_DATA','ULTIMA_DATA','DESC_ITEM'
        ])
    
    # OTIMIZAÇÃO: usar último mês de dados reais globais
    ultimo_global = m['ANO_MES'].max()

    # contagem eficiente sem expansão
    meses_com = (m[m['QTD_MENSAL'] > 0]
                   .groupby('COD_ITEM')['ANO_MES']
                   .nunique()
                   .rename('MESES_COM_CONSUMO'))

    # primeiro/último mês com consumo
    first_month = (m[m['QTD_MENSAL']>0].groupby('COD_ITEM')['ANO_MES'].min().rename('PRIMEIRO_MES_CONS'))
    last_month  = (m[m['QTD_MENSAL']>0].groupby('COD_ITEM')['ANO_MES'].max().rename('ULTIMO_MES_CONS'))

    # datas reais (primeira/última ocorrência) - sem alteração
    r = df_raw.copy()
    r['DATA_ENTREGA'] = pd.to_datetime(r['DATA_ENTREGA'].astype(str), errors='coerce')
    real_first = (r[r['QUANTIDADE']>0].groupby('COD_ITEM')['DATA_ENTREGA'].min().rename('PRIMEIRA_DATA'))
    real_last  = (r[r['QUANTIDADE']>0].groupby('COD_ITEM')['DATA_ENTREGA'].max().rename('ULTIMA_DATA'))
    desc = (r.groupby('COD_ITEM')['DESC_ITEM'].agg(lambda s: s.dropna().iloc[0] if not s.dropna().empty else None))

    base = pd.DataFrame({'COD_ITEM': m['COD_ITEM'].unique()}).set_index('COD_ITEM')
    base = base.join([meses_com, first_month, last_month, real_first, real_last, desc])

    # cálculo otimizado
    base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'] = base.apply(
        lambda x: (_months_between(x['PRIMEIRO_MES_CONS'], ultimo_global) + 1) if pd.notna(x['PRIMEIRO_MES_CONS']) else 0,
        axis=1
    )

    base['MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA'] = base.apply(
        lambda x: _months_between(x['ULTIMO_MES_CONS'], ultimo_global) if pd.notna(x['ULTIMO_MES_CONS']) else np.nan,
        axis=1
    )

    base['TAXA_MESES_COM_CONSUMO'] = np.where(
        base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA']>0,
        base['MESES_COM_CONSUMO'] / base['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'],
        np.nan
    )

    base = base.reset_index()
    cols = [
        'COD_ITEM','MESES_ANALISADOS_DESDE_1A_OCORRENCIA','MESES_COM_CONSUMO','TAXA_MESES_COM_CONSUMO',
        'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA','PRIMEIRA_DATA','ULTIMA_DATA','DESC_ITEM'
    ]
    return base[cols]

def export_ml_consumo(mensal_qtd: pd.DataFrame, mensal_val: pd.DataFrame, indicadores: pd.DataFrame, out_dir: Path) -> Path:
    """Exporta com verificações de tamanho para evitar OOM."""
    out = Path(out_dir) / 'ML_CONSUMO_SOLIDOS.xlsx'
    
    # Verifica tamanho antes do merge
    logger.info(f"Preparando ML_CONSUMO_SOLIDOS.xlsx: {len(mensal_qtd)} linhas QTD, {len(mensal_val)} linhas VAL")
    
    if len(mensal_qtd) > 500_000 or len(mensal_val) > 500_000:
        logger.warning("Dataset muito grande, exportando tabelas separadas")
        with pd.ExcelWriter(out, engine='openpyxl') as xw:
            mensal_qtd.to_excel(xw, sheet_name='MENSAL_QTD', index=False)
            mensal_val.to_excel(xw, sheet_name='MENSAL_VAL', index=False) 
            indicadores.to_excel(xw, sheet_name='INDICADORES', index=False)
    else:
        # Merge seguro
        ms = (mensal_qtd.merge(mensal_val, on=['COD_ITEM','ANO_MES'], how='inner')  # inner join para reduzir
                        .assign(HAS_CONS=lambda d: d['QTD_MENSAL']>0))
        logger.debug(f"Merge resultou em {len(ms)} linhas")
        
        with pd.ExcelWriter(out, engine='openpyxl') as xw:
            ms.to_excel(xw, sheet_name='MENSAL_SERIES', index=False)
            indicadores.to_excel(xw, sheet_name='INDICADORES', index=False)
    
    return out

# ===== Métricas de uso por classe (reintroduzidas para integração) =====
def build_usage_metrics(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """Calcula ocorrências e classe de uso por item com base na série mensal."""
    usage_cols = [
        'COD_ITEM','OCCURRENCES_TOTAL','OCCURRENCES_12M','MESES_DESDE_PRIMEIRO',
        'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA','USE_RATE','CLASSE_USO',
        'INTERVALO_MEDIO_MESES','QTD_MEDIA_OCORRENCIA','QTD_ULTIMA_OCORRENCIA',
        'ULTIMO_MES_ORDINAL','ULTIMO_ANO_CONSUMO','ULTIMO_MES_CONSUMO'
    ]
    if mensal_qtd is None or mensal_qtd.empty:
        logger.warning("mensal_qtd está vazio, retornando DataFrame vazio de métricas de uso")
        return pd.DataFrame(columns=usage_cols)

    df = mensal_qtd.copy()
    
    # **CRÍTICO**: Normaliza COD_ITEM para string ANTES de qualquer operação
    df['COD_ITEM'] = df['COD_ITEM'].astype(str)
    if 'ANO_MES' not in df.columns:
        logger.warning("mensal_qtd não possui coluna ANO_MES")
        return pd.DataFrame(columns=usage_cols)

    ano_mes_series = df['ANO_MES']
    try:
        df['ANO_MES'] = pd.PeriodIndex(ano_mes_series, freq='M')
    except (TypeError, ValueError) as e:
        logger.debug(f"Convertendo ANO_MES via datetime: {e}")
        df['ANO_MES'] = pd.PeriodIndex(pd.to_datetime(ano_mes_series, errors='coerce'), freq='M')
    
    if df.empty:
        logger.warning("Após cópia, mensal_qtd está vazio")
        return pd.DataFrame(columns=usage_cols)

    df = df.sort_values(['COD_ITEM', 'ANO_MES'])
    ultimo = df['ANO_MES'].max()
    inicio_por_item = (df[df['QTD_MENSAL'] > 0]
                       .groupby('COD_ITEM')['ANO_MES']
                       .min())
    fim_por_item = (df[df['QTD_MENSAL'] > 0]
                    .groupby('COD_ITEM')['ANO_MES']
                    .max())
    # Ocorrências totais
    occ_total = (df.assign(HAS_USE=(df['QTD_MENSAL'] > 0).astype(int))
                   .groupby('COD_ITEM')['HAS_USE']
                   .sum()
                   .rename('OCCURRENCES_TOTAL'))
    # Ocorrências nos últimos 12 meses do último global
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
        lambda r: (_months_between(r['PRIMEIRO_MES'], ultimo) + 1) if pd.notna(r['PRIMEIRO_MES']) else 0, axis=1
    )
    base['USE_RATE'] = np.where(base['MESES_DESDE_PRIMEIRO'] > 0,
                                base['OCCURRENCES_TOTAL'] / base['MESES_DESDE_PRIMEIRO'],
                                0.0)
    # Classificação por taxa/ocorrência
    def _classify(rate, occ12):
        if occ12 <= 2 or rate <= 0.10:
            return 'Muito Raro'
        elif rate <= 0.25:
            return 'Raro'
        elif rate <= 0.50:
            return 'Ocasional'
        elif rate < 0.80:
            return 'Intermitente'
        else:
            return 'Rotineiro'
    base['MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA'] = base.apply(
        lambda r: (_months_between(r['ULTIMO_MES_POS'], ultimo) if pd.notna(r['ULTIMO_MES_POS']) else np.inf),
        axis=1
    )
    base['CLASSE_USO'] = base.apply(lambda r: _classify(r['USE_RATE'], r['OCCURRENCES_12M']), axis=1)

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
                                   .set_index('COD_ITEM')[['QTD_MENSAL','ANO_MES','ORDINAL']])
        ultimo_evento = ultimo_evento.rename(columns={
            'QTD_MENSAL': 'QTD_ULTIMA_OCORRENCIA',
            'ORDINAL': 'ULTIMO_MES_ORDINAL'
        })
        ultimo_evento['ULTIMO_ANO_CONSUMO'] = pd.to_datetime(ultimo_evento['ANO_MES'].astype(str)).dt.year.astype('Int64')  # type: ignore[union-attr]
        ultimo_evento['ULTIMO_MES_CONSUMO'] = pd.to_datetime(ultimo_evento['ANO_MES'].astype(str)).dt.month.astype('Int64')  # type: ignore[union-attr]
        ultimo_evento = ultimo_evento[['QTD_ULTIMA_OCORRENCIA', 'ULTIMO_MES_ORDINAL',
                                       'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO']]
    else:
        intervalo_medio = pd.Series(dtype=float, name='INTERVALO_MEDIO_MESES')
        qtd_media = pd.Series(dtype=float, name='QTD_MEDIA_OCORRENCIA')
        ultimo_evento = pd.DataFrame(columns=['QTD_ULTIMA_OCORRENCIA', 'ULTIMO_MES_ORDINAL',
                                              'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO'])
        ultimo_evento.index.name = 'COD_ITEM'

    base = (base.join(intervalo_medio)
                 .join(qtd_media)
                 .join(ultimo_evento))
    base = base.reset_index()
    
    # Log de debug
    logger.info(f"build_usage_metrics gerou {len(base)} itens com CLASSE_USO")
    if len(base) > 0:
        logger.debug(f"Exemplo: {base[['COD_ITEM','CLASSE_USO']].head(3).to_dict('records')}")
    
    return base[usage_cols]


def build_monthly_distribution(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula a distribuição mensal histórica por item.
    Retorna pesos proporcionais ao volume consumido (fallback por ocorrência).
    """
    if mensal_qtd is None or mensal_qtd.empty:
        return pd.DataFrame(columns=['COD_ITEM', 'MES', 'PESO', 'QTD_TOTAL', 'OCORRENCIAS', 'MES_MAIS_RECENTE'])

    df = mensal_qtd.copy()
    if 'ANO_MES' not in df.columns:
        return pd.DataFrame(columns=['COD_ITEM', 'MES', 'PESO', 'QTD_TOTAL', 'OCORRENCIAS', 'MES_MAIS_RECENTE'])

    df['ANO_MES'] = _to_period_month(df['ANO_MES'])
    df['MES'] = pd.to_datetime(df['ANO_MES'].astype(str)).dt.month  # type: ignore[union-attr]
    ultimo_periodo = df['ANO_MES'].max()
    positivos = df[df['QTD_MENSAL'] > 0].copy()
    if positivos.empty:
        return pd.DataFrame(columns=['COD_ITEM', 'MES', 'PESO', 'QTD_TOTAL', 'OCORRENCIAS', 'MES_MAIS_RECENTE'])

    positivos['DIST_MESES'] = positivos['ANO_MES'].apply(lambda p: _months_between(p, ultimo_periodo))
    positivos['RECENCY_FACTOR'] = np.exp(-positivos['DIST_MESES'] / 12.0)
    positivos['PESO_BRUTO'] = positivos['QTD_MENSAL'].astype(float) * positivos['RECENCY_FACTOR']

    # **CORREÇÃO**: Garante que COD_ITEM seja string antes do groupby
    positivos['COD_ITEM'] = positivos['COD_ITEM'].apply(
        lambda x: str(int(x)) if isinstance(x, (np.integer, int, np.floating, float)) else str(x)
    )

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
        np.nan
    )
    mask_zero = distrib['PESO'].isna() | (distrib['PESO'] <= 0)
    if mask_zero.any():
        distrib.loc[mask_zero, 'PESO'] = np.where(
            distrib.loc[mask_zero, 'TOTAL_OCORRENCIAS_ITEM'] > 0,
            distrib.loc[mask_zero, 'OCORRENCIAS'] / distrib.loc[mask_zero, 'TOTAL_OCORRENCIAS_ITEM'],
            0.0
        )
    distrib['PESO'] = distrib['PESO'].fillna(0.0)
    ultimo_mes = (positivos.groupby('COD_ITEM')['ANO_MES']
                            .max()
                            .apply(lambda x: pd.to_datetime(str(x)).month)  # type: ignore[union-attr]
                            .rename('MES_MAIS_RECENTE')
                            .reset_index())
    distrib = distrib.merge(ultimo_mes, on='COD_ITEM', how='left')
    return distrib[['COD_ITEM', 'MES', 'PESO', 'QTD_TOTAL', 'OCORRENCIAS', 'MES_MAIS_RECENTE']]

# ===== NOVO: ADI/CV² e features do seletor (tendência/sazonalidade) =====
def _compute_adi_cv2_local(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """Recalcula ADI e CV² por item a partir de mensal_qtd (evita depender do pipeline)."""
    rows = []
    for cod, sub in mensal_qtd.groupby('COD_ITEM'):
        s = sub.sort_values('ANO_MES')
        y = s['QTD_MENSAL'].values.astype(float)
        pos = y > 0  # type: ignore[operator]
        mc = int(pos.sum())
        if mc == 0:
            adi = float('inf')
            cv2 = float('inf')
        elif mc == 1:
            adi = float(len(y))  # período total observado
            cv2 = float('inf')
        else:
            idx_pos = np.where(pos)[0]
            p_ini, p_fim = idx_pos[0], idx_pos[-1]
            periodo_ativo = (p_fim - p_ini + 1)
            adi = float(periodo_ativo / mc) if mc > 0 else float('inf')
            nz = np.asarray(y[pos])
            if len(nz) <= 1:
                cv2 = float('inf')
            else:
                mu = float(np.mean(nz))  # type: ignore[arg-type]
                sd = float(np.std(nz, ddof=0))  # type: ignore[arg-type]
                cv2 = float((sd / mu) ** 2) if mu > 0 else float('inf')
        rows.append({'COD_ITEM': cod, 'ADI': adi, 'CV2': cv2})
    return pd.DataFrame(rows)

def _build_selector_features(mensal_qtd: pd.DataFrame) -> pd.DataFrame:
    """Calcula força de tendência e sazonalidade simples por item (features do seletor)."""
    feats = []
    for cod, sub in mensal_qtd.groupby('COD_ITEM'):
        s = sub.sort_values('ANO_MES')
        y = s['QTD_MENSAL'].values.astype(float)
        n = len(y)
        if n > 3:
            try:
                slope = float(np.polyfit(np.arange(n), np.asarray(y), 1)[0])  # type: ignore[arg-type]
            except (TypeError, ValueError, np.linalg.LinAlgError) as e:
                logger.debug(f"polyfit falhou para COD_ITEM={cod}: {e}")
                slope = 0.0
        else:
            slope = 0.0
        trend_strength = abs(slope)
        m = float(np.mean(np.asarray(y))) if n > 0 else 0.0  # type: ignore[arg-type]
        sd = float(np.std(np.asarray(y))) if n > 0 else 0.0  # type: ignore[arg-type]
        seasonality_simple = 1 if (m > 0 and sd > m * 0.5) else 0
        feats.append({
            'COD_ITEM': cod,
            'TREND_STRENGTH': trend_strength,
            'SEASONALITY_SIMPLE': seasonality_simple
        })
    return pd.DataFrame(feats)

# ===== Helpers locais para ML outputs e histórico =====
def _find_ml_output_dir(base: Path) -> Path:
    """
    Resolve o diretório que contém predicoes.csv e avaliacao.csv.
    Prioriza:
      1) base
      2) base/ml_results
      3) base/saida_ml
      4) Modulo_Solidos_ML/orcamento_2026
      5) Modulo_Solidos_ML (fallback)
    """
    base = Path(base)
    if base.is_file():
        base = base.parent

    candidates = [
        base,
        base / 'ml_results',
        base / 'saida_ml',
        Path(__file__).resolve().parent / 'orcamento_2026',
        Path(__file__).resolve().parent
    ]
    for c in candidates:
        if (c / 'predicoes.csv').exists() or (c / 'avaliacao.csv').exists():
            return c
    return base

def _load_ml_outputs_csvs(dir_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Carrega CSVs de predições e avaliação se existirem. Retorna DataFrames vazios caso contrário.
    """
    dir_path = Path(dir_path)
    pred_path = dir_path / 'predicoes.csv'
    aval_path = dir_path / 'avaliacao.csv'
    pred_df = pd.read_csv(pred_path) if pred_path.exists() else pd.DataFrame()
    aval_df = pd.read_csv(aval_path) if aval_path.exists() else pd.DataFrame()
    return pred_df, aval_df

def _to_period_month(series: pd.Series) -> pd.Series:
    """
    Converte série para Period[M] de forma segura (sem usar apply).
    Aceita strings ou datetime; mantém NaT.
    """
    dt = pd.to_datetime(series, errors='coerce')
    # Series.dt.to_period lida com NaT corretamente
    return dt.dt.to_period('M')

# ===== Helpers de preço =====
def _ultimo_preco(mensal_preco: pd.DataFrame) -> pd.DataFrame:
    """Extrai último preço conhecido por item, com ano de referência."""
    if mensal_preco is None or mensal_preco.empty:
        return pd.DataFrame(columns=['COD_ITEM','PRECO_ULTIMO','ANO_ULTIMO'])
    df = mensal_preco.copy()
    # Normaliza coluna período
    if 'ANO_MES' in df.columns:
        df['ANO_MES'] = _to_period_month(df['ANO_MES'])
        df['ANO_REF'] = pd.to_datetime(df['ANO_MES'].astype(str)).dt.year  # type: ignore[union-attr]
    elif 'ANO' in df.columns:
        df['ANO_REF'] = pd.to_numeric(df['ANO'], errors='coerce').astype('Int64')
    else:
        df['ANO_REF'] = pd.NA
    # Coluna de preço candidata
    cand = [c for c in ['PRECO_UNIT','PRECO_MEDIO','PRECO_MEDIO_MENSAL','PRECO'] if c in df.columns]
    if not cand:
        return pd.DataFrame(columns=['COD_ITEM','PRECO_ULTIMO','ANO_ULTIMO'])
    col_p = cand[0]
    df = df.sort_values(['COD_ITEM', 'ANO_REF']).dropna(subset=['COD_ITEM'])
    # último registro por item
    last = df.groupby('COD_ITEM').tail(1)[['COD_ITEM', col_p, 'ANO_REF']].rename(
        columns={col_p: 'PRECO_ULTIMO', 'ANO_REF': 'ANO_ULTIMO'}
    )
    # Tipos
    last['PRECO_ULTIMO'] = pd.to_numeric(last['PRECO_ULTIMO'], errors='coerce')
    return last

# ===== Integração ML + preços =====
def integrar_ml_com_precos(ml_results_path: Path, mensal_qtd: pd.DataFrame, 
                           mensal_preco: pd.DataFrame, aux_path: Path) -> pd.DataFrame:
    """Integra resultados ML com métricas de preço CORRIGIDAS"""
    # Localiza diretório real dos CSVs (raiz ou ml_results)
    dir_with_csvs = _find_ml_output_dir(ml_results_path)
    logger.info(f"Carregando previsões ML de: {dir_with_csvs}")
    ml_preds, ml_eval = _load_ml_outputs_csvs(dir_with_csvs)

    logger.info(f"ML: {len(ml_preds)} previsões, {len(ml_eval)} avaliações")
    
    # --- CALCULA PREÇO ÚLTIMO e PREÇO 2026 CORRIGIDOS ---
    # Para cada item, pega o último preço registrado e ajusta pelo IPCA
    
    # Ano mais recente do dataset
    ano_max = int(str(mensal_qtd['ANO_MES'].max()).split('-')[0])
    logger.debug(f"Ano mais recente no dataset: {ano_max}")
    
    # Carrega IPCA se disponível
    ipca = _load_ipca_fatores(aux_path) if aux_path and Path(aux_path).exists() else None
    
    # Para cada item, calcula o último preço e ajusta
    precos_ajustados = []
    
    for cod in ml_preds['COD_ITEM'].unique():
        # Último registro de preço para este item
        item_precos = mensal_preco[mensal_preco['COD_ITEM'] == cod].copy()
        
        if item_precos.empty:
            # Item sem histórico de preço - usar preço padrão
            precos_ajustados.append({
                'COD_ITEM': cod,
                'PRECO_ULTIMO': 1.0,
                'PRECO_2025': 1.0,
                'PRECO_2026': 1.035,
                'ANO_ULTIMO_PRECO': ano_max
            })
            continue
        
        # Ordena por período e pega o último
        item_precos = item_precos.sort_values('ANO_MES')
        ultimo_registro = item_precos.iloc[-1]
        
        preco_ultimo = ultimo_registro['PRECO_MEDIO_MENSAL']
        ano_ultimo = int(str(ultimo_registro['ANO_MES']).split('-')[0])
        
        # Ajuste do preço pelo IPCA
        if ipca is not None and ano_ultimo < ano_max:
            # Ajusta do ano do último preço até o ano máximo dos dados
            fator_ate_ano_max = _fator_acumulado_ipca(ipca, ano_ultimo, ano_max)
            preco_ajustado_ano_max = preco_ultimo * fator_ate_ano_max
        else:
            preco_ajustado_ano_max = preco_ultimo
        
        # Projeta para 2025 e 2026
        if ipca is not None:
            fator_2025 = _fator_acumulado_ipca(ipca, ano_max, 2025)
            fator_2026 = _fator_acumulado_ipca(ipca, ano_max, 2026)
            preco_2025 = preco_ajustado_ano_max * fator_2025
            preco_2026 = preco_ajustado_ano_max * fator_2026
        else:
            # Sem IPCA: usa inflação estimada de 3.5% ao ano
            anos_para_2025 = max(0, 2025 - ano_max)
            anos_para_2026 = max(0, 2026 - ano_max)
            preco_2025 = preco_ajustado_ano_max * (1.035 ** anos_para_2025)
            preco_2026 = preco_ajustado_ano_max * (1.035 ** anos_para_2026)
        
        precos_ajustados.append({
            'COD_ITEM': cod,
            'PRECO_ULTIMO': preco_ultimo,
            'PRECO_AJUSTADO_ATE_ANO_MAX': preco_ajustado_ano_max,
            'PRECO_2025': preco_2025,
            'PRECO_2026': preco_2026,
            'ANO_ULTIMO_PRECO': ano_ultimo
        })
    
    price_metrics = pd.DataFrame(precos_ajustados)
    logger.info(f"Métricas de preço calculadas: {len(price_metrics)} itens")
    logger.debug(f"Exemplo de preços: {price_metrics[['COD_ITEM', 'PRECO_ULTIMO', 'PRECO_2026']].head().to_dict('records')}")
    
    # Merge: ML + Preços + Avaliação
    budget_base = (ml_preds.merge(price_metrics, on='COD_ITEM', how='left')
                            .merge(ml_eval[['COD_ITEM', 'QUALITY_SCORE', 'BAIXA_CONFIABILIDADE']], on='COD_ITEM', how='left'))
    
    # **CRÍTICO**: Normaliza COD_ITEM para string em TODOS os DataFrames
    for df_name, df in [('budget_base', budget_base), ('mensal_qtd', mensal_qtd)]:
        if df is not None and not df.empty and 'COD_ITEM' in df.columns:
            df['COD_ITEM'] = df['COD_ITEM'].astype(str)
            logger.debug(f"{df_name}: COD_ITEM normalizado para string ({len(df)} linhas)")
    
    # Gera métricas de uso
    logger.info("Gerando métricas de uso...")
    uso = build_usage_metrics(mensal_qtd)
    logger.debug(f"Métricas de uso: {len(uso)} itens")
    
    if not uso.empty:
        uso['COD_ITEM'] = uso['COD_ITEM'].astype(str)
        logger.debug(f"Colunas em 'uso': {list(uso.columns)}")
        logger.debug(f"CLASSE_USO presente: {'CLASSE_USO' in uso.columns}")
    
    # Gera ADI/CV²
    adi_cv2 = _compute_adi_cv2_local(mensal_qtd)
    if not adi_cv2.empty:
        adi_cv2['COD_ITEM'] = adi_cv2['COD_ITEM'].astype(str)
    
    # Gera features do seletor
    feats = _build_selector_features(mensal_qtd)
    if not feats.empty:
        feats['COD_ITEM'] = feats['COD_ITEM'].astype(str)
    
    # **ORDEM DOS MERGES É CRÍTICA**
    logger.debug("Realizando merges...")
    logger.debug(f"   budget_base antes: {budget_base.columns.tolist()}")
    
    # 1. Merge com métricas de uso (CLASSE_USO)
    budget_base = budget_base.merge(uso, on='COD_ITEM', how='left', suffixes=('', '_uso'))
    logger.debug(f"Após merge com 'uso': CLASSE_USO presente = {'CLASSE_USO' in budget_base.columns}")
    logger.debug(f"Itens com CLASSE_USO não-nula: {budget_base['CLASSE_USO'].notna().sum()}/{len(budget_base)}")
    
    # 2. Merge com ADI/CV²
    budget_base = budget_base.merge(adi_cv2, on='COD_ITEM', how='left', suffixes=('', '_adi'))
    
    # 3. Merge com features do seletor
    budget_base = budget_base.merge(feats, on='COD_ITEM', how='left', suffixes=('', '_feat'))
    
    # **VALIDAÇÃO FINAL**
    if 'CLASSE_USO' not in budget_base.columns:
        logger.error("CLASSE_USO não está no DataFrame final!")
        logger.error(f"Colunas presentes: {budget_base.columns.tolist()}")
    else:
        logger.info(f"CLASSE_USO presente no DataFrame final")
        logger.debug(f"Distribuição: {budget_base['CLASSE_USO'].value_counts().to_dict()}")
    
    # Ordenação amigável (se desejar utilizar fin_utils.ORDER_MAP)
    try:
        budget_base['CLASSE_USO_ORD'] = budget_base['CLASSE_USO'].map(ORDER_MAP)
    except KeyError as e:
        logger.debug(f"Mapeamento CLASSE_USO_ORD falhou: {e}")
        budget_base['CLASSE_USO_ORD'] = None
    
    logger.info(f"Base orçamentária: {len(budget_base)} itens após merge")
    
    # Estatísticas para validação
    logger.info(f"Estatísticas de preços: PRECO_ULTIMO Média=R$ {budget_base['PRECO_ULTIMO'].mean():.2f}, PRECO_2026 Média=R$ {budget_base['PRECO_2026'].mean():.2f}")
    
    # **CORREÇÃO**: Preencher CLASSE_USO para itens sem histórico
    if 'CLASSE_USO' in budget_base.columns:
        mask_sem_classe = budget_base['CLASSE_USO'].isna()
        n_sem_classe = mask_sem_classe.sum()
        
        if n_sem_classe > 0:
            logger.warning(f"{n_sem_classe} itens sem CLASSE_USO - aplicando fallback...")
            
            def _fallback_classe_uso(row):
                """Classifica itens sem histórico baseado em MODEL_USED e taxa projetada"""
                rate = row.get('PREDICTED_CONSUMPTION_RATE', 0)

                if _is_model_inativo(row.get('MODEL_USED', '')) and not (_ignorar_inativo_critico() and _is_critico(row)):
                    return 'Muito Raro'
                if rate == 0:
                    return 'Muito Raro'
                
                # Classificação por taxa de consumo projetada
                if rate >= 10:
                    return 'Rotineiro'
                elif rate >= 3:
                    return 'Intermitente'
                elif rate >= 1:
                    return 'Ocasional'
                elif rate >= 0.3:
                    return 'Raro'
                else:
                    return 'Muito Raro'
            
            budget_base.loc[mask_sem_classe, 'CLASSE_USO'] = budget_base[mask_sem_classe].apply(
                _fallback_classe_uso, axis=1
            )
            
            logger.info(f"[OK] Fallback aplicado. Nova distribuição:")
            logger.info(f"   {budget_base['CLASSE_USO'].value_counts().to_dict()}")
            
            # Debug: exporta itens que receberam fallback
            itens_fallback = budget_base[mask_sem_classe][['COD_ITEM', 'DESC_ITEM', 'MODEL_USED', 
                                                             'PREDICTED_CONSUMPTION_RATE', 'CLASSE_USO']]
            debug_path = Path('Modulo_Solidos_ML') / 'orcamento_2026' / 'DEBUG_FALLBACK_CLASSE_USO.csv'
            itens_fallback.to_csv(debug_path, index=False, encoding='utf-8-sig')
            logger.debug(f"📝 Debug: {debug_path}")
        mask_sem_classe = budget_base['CLASSE_USO'].isna()
        
        if mask_sem_classe.any():
            logger.warning(f"⚠️ ALERTA: {mask_sem_classe.sum()} itens sem CLASSE_USO, aplicando fallback...")
            
            # Fallback baseado no modelo usado
            def _fallback_classe_uso(row):
                is_inactive = _is_model_inativo(row.get('MODEL_USED', ''))
                if is_inactive and not (_ignorar_inativo_critico() and _is_critico(row)):
                    return 'Muito Raro'
                elif row['PREDICTED_CONSUMPTION_RATE'] == 0:
                    return 'Muito Raro'
                elif row['PREDICTED_CONSUMPTION_RATE'] >= 10:
                    return 'Rotineiro'
                elif row['PREDICTED_CONSUMPTION_RATE'] >= 3:
                    return 'Intermitente'
                elif row['PREDICTED_CONSUMPTION_RATE'] >= 1:
                    return 'Ocasional'
                elif row['PREDICTED_CONSUMPTION_RATE'] >= 0.3:
                    return 'Raro'
                else:
                    return 'Muito Raro'
            
            budget_base.loc[mask_sem_classe, 'CLASSE_USO'] = \
                budget_base[mask_sem_classe].apply(_fallback_classe_uso, axis=1)
            
            logger.info(f"   Fallback aplicado: {mask_sem_classe.sum()} itens classificados")
            logger.info(f"   Nova distribuição: {budget_base['CLASSE_USO'].value_counts().to_dict()}")
    
    return budget_base


def _ajustar_correias_transportadoras(base: pd.DataFrame, cfg: BudgetConfig) -> pd.DataFrame:
    """
    Ajusta a projeção das correias transportadoras considerando metragem fixa e intervalo médio.
    """
    if base is None or base.empty or 'DESC_ITEM' not in base.columns:
        return base

    correia_cols = [
        'CORREIA_TROCAS_2026',
        'CORREIA_QTD_EVENTO_REFERENCIA',
        'CORREIA_INTERVALO_APLICADO_MESES',
        'CORREIA_PROXIMA_TROCA',
    ]
    for col in correia_cols:
        if col not in base.columns:
            base[col] = np.nan
    if QTD_BASE_COL not in base.columns:
        base[QTD_BASE_COL] = np.nan
    if QTD_FINAL_COL not in base.columns:
        if QTD_FINAL_LEGACY_COL in base.columns:
            base = base.rename(columns={QTD_FINAL_LEGACY_COL: QTD_FINAL_COL})
        else:
            base[QTD_FINAL_COL] = np.nan
    if 'GASTO_BASE_2026' not in base.columns:
        base['GASTO_BASE_2026'] = np.nan

    desc_series = base['DESC_ITEM'].astype(str)
    mask_correia = desc_series.str.contains('CORREIA TRANSPORTADORA', case=False, na=False)
    if not mask_correia.any():
        return base

    required_cols = [
        'INTERVALO_MEDIO_MESES',
        'ULTIMO_ANO_CONSUMO',
        'ULTIMO_MES_CONSUMO',
        'QTD_ULTIMA_OCORRENCIA',
        'QTD_MEDIA_OCORRENCIA',
    ]
    missing = [c for c in required_cols if c not in base.columns]
    if missing:
        logger.warning(f"⚠️ Regra de correias não aplicada: colunas ausentes {missing}")
        return base

    ano_orc = getattr(cfg, 'ano_orcamento', 2026)
    start_ord = ano_orc * 12
    end_ord = start_ord + 11

    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    status_inativo_cfg = _inactive_status()
    criticos_cfg = _critical_terms()
    desc_series = base['DESC_ITEM'].astype(str)
    mask_critico = pd.Series(False, index=base.index)
    if criticos_cfg:
        desc_upper = desc_series.str.upper()
        for termo in criticos_cfg:
            mask_critico = mask_critico | desc_upper.str.contains(termo, na=False, regex=False)
    ignorar_inativo = _ignorar_inativo_critico()

    ajustados_cod: list[Any] = []
    for idx in base.index[mask_correia]:
        row = base.loc[idx]

        model_used_val = row.get('MODEL_USED', '')
        is_inativo = status_inativo_cfg and str(model_used_val).strip().upper() == status_inativo_cfg
        if is_inativo and not (ignorar_inativo and mask_critico.loc[idx]):
            continue

        intervalo = _safe_float(row.get('INTERVALO_MEDIO_MESES'))
        if intervalo is None or not math.isfinite(intervalo) or intervalo <= 0:
            continue
        intervalo_int = max(1, int(round(intervalo)))

        ano_ultimo = row.get('ULTIMO_ANO_CONSUMO')
        mes_ultimo = row.get('ULTIMO_MES_CONSUMO')
        if pd.isna(ano_ultimo) or pd.isna(mes_ultimo):
            continue
        try:
            ano_ultimo = int(ano_ultimo)
            mes_ultimo = int(mes_ultimo)
        except (TypeError, ValueError):
            continue
        if ano_ultimo <= 0:
            continue
        mes_ultimo = max(1, min(12, mes_ultimo))

        last_ord = ano_ultimo * 12 + (mes_ultimo - 1)
        current = last_ord + intervalo_int
        trocas = 0
        prox_ord = None
        guard = 0
        while current <= end_ord and guard < 60:
            if current >= start_ord:
                trocas += 1
                if prox_ord is None:
                    prox_ord = current
            current += intervalo_int
            guard += 1
        if trocas == 0:
            continue

        qtd_evento = _safe_float(row.get('QTD_ULTIMA_OCORRENCIA'))
        if qtd_evento is None or qtd_evento <= 0:
            qtd_evento = _safe_float(row.get('QTD_MEDIA_OCORRENCIA'))
        if qtd_evento is None or qtd_evento <= 0:
            continue

        qtd_total = qtd_evento * trocas
        preco = _safe_float(row.get('PRECO_2026')) or 0.0
        gasto_base = qtd_total * preco

        base.at[idx, QTD_BASE_COL] = qtd_total
        base.at[idx, QTD_FINAL_COL] = qtd_total
        base.at[idx, 'GASTO_BASE_2026'] = gasto_base
        base.at[idx, 'CORREIA_TROCAS_2026'] = trocas
        base.at[idx, 'CORREIA_QTD_EVENTO_REFERENCIA'] = qtd_evento
        base.at[idx, 'CORREIA_INTERVALO_APLICADO_MESES'] = intervalo_int

        if prox_ord is not None:
            prox_ano = prox_ord // 12
            prox_mes = (prox_ord % 12) + 1
            base.at[idx, 'CORREIA_PROXIMA_TROCA'] = f"{int(prox_ano):04d}-{int(prox_mes):02d}"

        ajustados_cod.append(base.at[idx, 'COD_ITEM'])

    if ajustados_cod:
        logger.info(f"[OK] Itens de correia ajustados: {len(ajustados_cod)} (ex.: {ajustados_cod[:3]})")
    else:
        logger.warning("⚠️ Nenhum item de correia atendeu aos critérios de intervalo médio.")

    return base


@lru_cache(maxsize=1)
def _critical_terms() -> tuple[str, ...]:
    """Termos configurados para análise crítica (uppercase)."""
    termos_raw = get_param('orcamento.analise_critica.descricoes_palavras', [])
    if not isinstance(termos_raw, list):
        return tuple()
    cleaned = []
    for t in termos_raw:
        t_str = str(t).strip().upper()
        if t_str:
            cleaned.append(t_str)
    return tuple(cleaned)


@lru_cache(maxsize=1)
def _inactive_status() -> str:
    """Retorna o status configurado para itens inativos."""
    val = get_param('modelo_inativo.status', 'INACTIVE')
    return str(val).strip().upper()


@lru_cache(maxsize=1)
def _ignorar_inativo_critico() -> bool:
    """Flag de configuração para ignorar inativos em itens críticos."""
    return bool(get_param('orcamento.analise_critica.ignorar_model_inactive', True))


def _is_model_inativo(model_used: Any) -> bool:
    """Indica se o valor informado corresponde ao status de item inativo."""
    status = _inactive_status()
    if not status:
        return False
    return str(model_used).strip().upper() == status


def _is_critico(row: Any) -> bool:
    """Retorna True se a descrição do item contém algum dos termos críticos."""
    termos = _critical_terms()
    if not termos:
        return False
    desc_val = ''
    if hasattr(row, 'get'):
        try:
            desc_val = row.get('DESC_ITEM', '')
        except Exception:
            desc_val = ''
    elif isinstance(row, dict):
        desc_val = row.get('DESC_ITEM', '')
    desc_upper = str(desc_val).upper()
    if not desc_upper.strip():
        return False
    return any(term in desc_upper for term in termos)


def _build_analise_critica_df(budget: pd.DataFrame) -> pd.DataFrame:
    """
    Monta DataFrame com itens críticos com base em palavras configuradas.
    """
    if budget is None or budget.empty:
        return pd.DataFrame()

    descricoes_cfg = _critical_terms()
    if not descricoes_cfg:
        return pd.DataFrame()

    desc_series = budget.get('DESC_ITEM')
    if desc_series is None:
        return pd.DataFrame()

    desc_str = desc_series.fillna('').astype(str)
    desc_upper = desc_str.str.upper()
    match_labels = pd.Series(pd.NA, index=budget.index, dtype='object')
    mask_final = pd.Series(False, index=budget.index)

    for termo in descricoes_cfg:
        termo_mask = desc_upper.str.contains(termo, na=False, regex=False)
        new_hits = termo_mask & match_labels.isna()
        match_labels.loc[new_hits] = termo
        mask_final = mask_final | termo_mask

    if not mask_final.any():
        return pd.DataFrame()

    criticos = budget.loc[mask_final].copy()
    criticos['GRUPO_CRITICO'] = match_labels.loc[mask_final].fillna('OUTROS')

    status_inativo_cfg = _inactive_status()
    ignorar_inativo = _ignorar_inativo_critico()
    if status_inativo_cfg and ignorar_inativo and 'MODEL_USED' in criticos.columns:
        criticos['MODEL_USED'] = criticos['MODEL_USED'].astype(str)
        criticos.loc[criticos['MODEL_USED'].str.upper() == status_inativo_cfg, 'MODEL_USED'] = 'IGNORED_INACTIVE'

    colunas = [
        'GRUPO_CRITICO', 'COD_ITEM', 'DESC_ITEM', 'UM', 'CLASSE_ABC', 'CLASSE_USO',
        'CLASSE_USO_INDUSTRIAL', 'MODEL_USED', 'QUALITY_SCORE',
        'PREDICTED_CONSUMPTION_RATE', QTD_FINAL_COL, 'PRECO_2026',
        'GASTO_COM_MARGEM_2026', 'MARGEM_SEGURANCA_PCT',
        'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA',
        'INTERVALO_MEDIO_MESES', 'QTD_MEDIA_OCORRENCIA', 'QTD_ULTIMA_OCORRENCIA',
        'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO',
        'CORREIA_TROCAS_2026', 'CORREIA_QTD_EVENTO_REFERENCIA',
        'CORREIA_INTERVALO_APLICADO_MESES', 'CORREIA_PROXIMA_TROCA'
    ]

    colunas_presentes = [c for c in colunas if c == 'GRUPO_CRITICO' or c in criticos.columns]
    criticos = criticos[colunas_presentes]

    if 'GASTO_COM_MARGEM_2026' in criticos.columns:
        criticos = criticos.sort_values(
            ['GRUPO_CRITICO', 'GASTO_COM_MARGEM_2026'],
            ascending=[True, False],
            kind='stable'
        )
    else:
        criticos = criticos.sort_values('GRUPO_CRITICO', kind='stable')

    return criticos.reset_index(drop=True)


def _normalize_cod_item(value: Any) -> str | None:
    """Normaliza COD_ITEM para string sem casas decimais."""
    if value is None:
        return None
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        if math.isnan(value):
            return None
        return str(int(round(value)))
    text = str(value).strip()
    if not text:
        return None
    try:
        if float(text).is_integer():
            return str(int(float(text)))
    except ValueError:
        pass
    return text


def _normalize_tipo_despesa(value: Any, fallback: str = "0000") -> tuple[str, str]:
    """
    Retorna par (td_display, td_norm) para NUM_TIPO_DESPESA.
    td_display mantém a representação original (sem zero-fill),
    td_norm possui 4 caracteres (zero à esquerda).
    """
    if value is None:
        return fallback.lstrip("0") or "0", fallback
    if isinstance(value, (np.floating, float)):
        if math.isnan(value):
            return fallback.lstrip("0") or "0", fallback
    td_str = str(value).strip()
    if not td_str:
        return fallback.lstrip("0") or "0", fallback
    td_digits = td_str
    if "." in td_digits:
        try:
            td_digits = str(int(float(td_digits)))
        except Exception:
            td_digits = td_digits.replace(".", "")
    if td_digits.isdigit():
        td_norm = td_digits.zfill(4)
        td_display = td_digits.lstrip("0") or "0"
        return td_display, td_norm
    # fallback: usa primeiros 4 caracteres, preenchendo se necessário
    td_norm = (td_digits[:4]).rjust(4, "0")
    return td_digits, td_norm


def _normalize_centro_custo(value: Any, fallback: str = "0000") -> str:
    """Normaliza centro de custo para string sem casas decimais."""
    if value is None:
        return fallback
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        if math.isnan(value):
            return fallback
        return str(int(round(value)))
    text = str(value).strip()
    if not text:
        return fallback
    if text.isdigit():
        return text
    try:
        if float(text).is_integer():
            return str(int(float(text)))
    except Exception:
        pass
    return text


def _build_item_expense_center_maps(
    df_consolidado: pd.DataFrame | None,
) -> tuple[dict[str, Any], dict[str, dict[str, float]]]:
    """
    Constrói mapas:
      - item_expense_map: COD_ITEM -> NUM_TIPO_DESPESA mais recorrente (por frequência, depois valor)
      - item_cc_share_map: COD_ITEM -> {CENTRO_CUSTO: participação}
    """
    item_expense_map: dict[str, Any] = {}
    item_cc_share_map: dict[str, dict[str, float]] = {}

    if df_consolidado is None or df_consolidado.empty:
        return item_expense_map, item_cc_share_map

    required = {'COD_ITEM', 'NUM_TIPO_DESPESA', 'CENTRO_CUSTO'}
    if not required.issubset(df_consolidado.columns):
        return item_expense_map, item_cc_share_map

    valor_col = next(
        (c for c in ['VALOR', 'VALOR_TOTAL', 'VALOR_LIQUIDO', 'VALOR_GASTO'] if c in df_consolidado.columns),
        None,
    )

    data = df_consolidado[list(required | ({valor_col} if valor_col else set()))].copy()
    data['COD_ITEM_KEY'] = data['COD_ITEM'].apply(_normalize_cod_item)
    data = data[data['COD_ITEM_KEY'].notna()]

    # Mapa de despesas prioritárias
    if 'NUM_TIPO_DESPESA' in data.columns:
        expense_group = data.groupby(['COD_ITEM_KEY', 'NUM_TIPO_DESPESA'], dropna=False)
        expense_summary = expense_group.size().rename('OCORRENCIAS').reset_index()
        if valor_col:
            valores = expense_group[valor_col].sum().rename('VALOR_TOTAL').reset_index()
            expense_summary = expense_summary.merge(
                valores,
                on=['COD_ITEM_KEY', 'NUM_TIPO_DESPESA'],
                how='left',
            )
        for item_key, subset in expense_summary.groupby('COD_ITEM_KEY', sort=False):
            subset_sorted = subset.sort_values(
                ['OCORRENCIAS', 'VALOR_TOTAL'] if 'VALOR_TOTAL' in subset.columns else ['OCORRENCIAS'],
                ascending=[False, False] if 'VALOR_TOTAL' in subset.columns else [False],
                kind='stable',
            )
            first = subset_sorted.iloc[0]
            item_expense_map[str(item_key)] = first['NUM_TIPO_DESPESA']

    # Participações por centro de custo
    center_group = data.groupby(['COD_ITEM_KEY', 'CENTRO_CUSTO'], dropna=False)
    share_summary = center_group.size().rename('OCORRENCIAS').reset_index()
    if valor_col:
        valores_cc = center_group[valor_col].sum().rename('VALOR_TOTAL').reset_index()
        share_summary = share_summary.merge(
            valores_cc,
            on=['COD_ITEM_KEY', 'CENTRO_CUSTO'],
            how='left',
        )

    for item_key, subset in share_summary.groupby('COD_ITEM_KEY', sort=False):
        subset = subset.copy()
        subset.loc[:, 'CENTRO_CUSTO'] = subset['CENTRO_CUSTO'].apply(_normalize_centro_custo)
        subset = subset[subset['CENTRO_CUSTO'].notna()]
        if subset.empty:
            continue
        if valor_col and subset['VALOR_TOTAL'].notna().any():
            total_val = subset['VALOR_TOTAL'].sum()
            if total_val > 0:
                subset.loc[:, 'SHARE'] = subset['VALOR_TOTAL'] / total_val
            else:
                total_occ = subset['OCORRENCIAS'].sum()
                subset.loc[:, 'SHARE'] = subset['OCORRENCIAS'] / total_occ if total_occ > 0 else 0.0
        else:
            total_occ = subset['OCORRENCIAS'].sum()
            subset.loc[:, 'SHARE'] = subset['OCORRENCIAS'] / total_occ if total_occ > 0 else 0.0

        subset.loc[:, 'SHARE'] = subset['SHARE'].fillna(0.0)
        total_share = float(subset['SHARE'].sum())
        if total_share <= 0:
            continue
        subset.loc[:, 'SHARE'] = subset['SHARE'] / total_share
        item_cc_share_map[str(item_key)] = dict(zip(subset['CENTRO_CUSTO'], subset['SHARE']))

    return item_expense_map, item_cc_share_map

# ===== Cálculo do orçamento =====
def calcular_projecao_orcamentaria(dados: pd.DataFrame, cfg: BudgetConfig) -> pd.DataFrame:
    """
    Calcula a projecao orcamentaria com base nos dados integrados do ML e precos.
    Inclui o modo 'inteligente'.
    """
    base = dados.copy()
    
    # Adiciona coluna ANO_ORCAMENTO
    base['ANO_ORCAMENTO'] = cfg.ano_orcamento
    
    # Fallback para modo 'essencial' se configuracao invalida
    modo = cfg.modo_orcamento if cfg.modo_orcamento in ['essencial', 'fidelidade', 'inteligente'] else 'essencial'

    margens_cfg = get_param('orcamento.margens_por_quality', [])
    margem_maxima_cfg = float(get_param('orcamento.margem_maxima', 0.5))
    margens_validas = []
    for item in margens_cfg if isinstance(margens_cfg, list) else []:
        if isinstance(item, dict) and 'min_quality' in item and 'margem' in item:
            try:
                margens_validas.append({
                    'min_quality': float(item['min_quality']),
                    'margem': float(item['margem'])
                })
            except (TypeError, ValueError):
                continue
    margens_validas.sort(key=lambda x: x['min_quality'], reverse=True)
    
    use_quality_score = bool(get_param('ml.quality_score.habilitado', True))
    default_margin = float(cfg.margem_seguranca_pct)
    margin_series = pd.Series(default_margin, index=base.index, dtype=float)

    margem_herdada = None
    if 'MARGEM_SEGURO' in base.columns:
        margem_herdada = pd.to_numeric(base['MARGEM_SEGURO'], errors='coerce') / 100.0

    def _margin_from_quality(score: float) -> float:
        if not margens_validas:
            return default_margin
        if pd.isna(score):
            return default_margin
        for item in margens_validas:
            if score >= item['min_quality']:
                return item['margem']
        return default_margin

    if use_quality_score and margens_validas and 'QUALITY_SCORE' in base.columns:
        margin_from_quality = base['QUALITY_SCORE'].apply(_margin_from_quality)
        margin_series = margin_from_quality.fillna(margin_series)

    if margem_herdada is not None:
        margin_series = margin_series.fillna(margem_herdada)

    margin_series = margin_series.fillna(default_margin).clip(lower=0.0, upper=margem_maxima_cfg)
    base['MARGEM_SEGURANCA_PCT'] = margin_series.astype(float)
    
    has_orcamento_proj = 'ORCAMENTO_PROJETADO' in base.columns
    has_gasto_2025 = 'GASTO_COM_MARGEM_2025' in base.columns
    has_predicted = 'PREDICTED_CONSUMPTION_RATE' in base.columns
    has_preco_2026 = 'PRECO_2026' in base.columns

    preco_2026_series = (
        pd.to_numeric(base['PRECO_2026'], errors='coerce')
        if has_preco_2026
        else pd.Series(np.nan, index=base.index, dtype=float)
    )

    if QTD_FINAL_LEGACY_COL in base.columns and QTD_FINAL_COL not in base.columns:
        base = base.rename(columns={QTD_FINAL_LEGACY_COL: QTD_FINAL_COL})
    qtd_base = pd.Series(np.nan, index=base.index, dtype=float)
    if QTD_FINAL_COL in base.columns:
        qtd_base = pd.to_numeric(base[QTD_FINAL_COL], errors='coerce')

    if has_predicted:
        forecast_qtd = pd.to_numeric(base['PREDICTED_CONSUMPTION_RATE'], errors='coerce') * 12.0
        if modo == 'inteligente':
            qtd_base = forecast_qtd
        else:
            qtd_base = qtd_base.fillna(forecast_qtd)

    if has_orcamento_proj and has_preco_2026:
        orcamento_proj = pd.to_numeric(base['ORCAMENTO_PROJETADO'], errors='coerce')
        mask_calc = qtd_base.isna() & preco_2026_series.notna() & (preco_2026_series > 0)
        qtd_base.loc[mask_calc] = orcamento_proj.loc[mask_calc] / preco_2026_series.loc[mask_calc]

    if has_gasto_2025 and has_preco_2026:
        gasto_prev = pd.to_numeric(base['GASTO_COM_MARGEM_2025'], errors='coerce')
        mask_calc_prev = qtd_base.isna() & preco_2026_series.notna() & (preco_2026_series > 0)
        qtd_base.loc[mask_calc_prev] = gasto_prev.loc[mask_calc_prev] / preco_2026_series.loc[mask_calc_prev]

    qtd_base = qtd_base.fillna(0.0)
    base[QTD_BASE_COL] = qtd_base
    base[QTD_FINAL_COL] = qtd_base.copy()

    gasto_base = pd.Series(np.nan, index=base.index, dtype=float)
    if 'GASTO_BASE_2026' in base.columns:
        gasto_base = pd.to_numeric(base['GASTO_BASE_2026'], errors='coerce')

    if modo == 'essencial' and has_orcamento_proj:
        gasto_base = pd.to_numeric(base['ORCAMENTO_PROJETADO'], errors='coerce')
    else:
        if has_preco_2026:
            gasto_base = gasto_base.fillna(qtd_base * preco_2026_series)
            mask_preco = preco_2026_series.notna()
            gasto_base.loc[mask_preco] = qtd_base.loc[mask_preco] * preco_2026_series.loc[mask_preco]
        if has_orcamento_proj:
            gasto_base = gasto_base.fillna(pd.to_numeric(base['ORCAMENTO_PROJETADO'], errors='coerce'))
        if modo == 'fidelidade' and has_gasto_2025:
            gasto_base = gasto_base.fillna(pd.to_numeric(base['GASTO_COM_MARGEM_2025'], errors='coerce'))

    gasto_base = gasto_base.fillna(0.0)
    base['GASTO_BASE_2026'] = gasto_base

    base = _ajustar_correias_transportadoras(base, cfg)
    base[QTD_BASE_COL] = pd.to_numeric(base[QTD_BASE_COL], errors='coerce').fillna(0.0)
    base[QTD_FINAL_COL] = base[QTD_BASE_COL]

    fator_margem = 1.0 + base['MARGEM_SEGURANCA_PCT'].fillna(0.0)
    base[QTD_FINAL_COL] = base[QTD_BASE_COL] * fator_margem

    if 'UM' in base.columns:
        int_units = {'PC', 'CJ', 'CX', 'CT', 'UN'}
        um_series = base['UM'].astype(str).str.strip().str.upper()
        mask_int_um = um_series.isin(int_units)
        if mask_int_um.any():
            qtd_proj = pd.to_numeric(
                base.loc[mask_int_um, QTD_FINAL_COL], errors='coerce'
            ).fillna(0.0)
            rounded = qtd_proj.copy()
            mask_maior_um = qtd_proj > 1.0
            rounded.loc[mask_maior_um] = np.floor(qtd_proj.loc[mask_maior_um])
            rounded.loc[~mask_maior_um] = np.ceil(qtd_proj.loc[~mask_maior_um])
            base.loc[mask_int_um, QTD_FINAL_COL] = rounded

            fator_local = fator_margem.loc[mask_int_um].replace(0.0, 1.0)
            fator_np = fator_local.to_numpy(dtype=float)
            rounded_np = rounded.to_numpy(dtype=float)
            base.loc[mask_int_um, QTD_BASE_COL] = np.divide(
                rounded_np,
                fator_np,
                out=np.zeros_like(rounded_np, dtype=float),
                where=fator_np != 0
            )

    fator_margem = 1.0 + base['MARGEM_SEGURANCA_PCT'].fillna(0.0)
    if has_preco_2026:
        custo_base_calc = base[QTD_BASE_COL] * preco_2026_series.fillna(0.0)
        custo_final_calc = base[QTD_FINAL_COL] * preco_2026_series.fillna(0.0)
    else:
        custo_base_calc = pd.Series(np.nan, index=base.index, dtype=float)
        custo_final_calc = pd.Series(np.nan, index=base.index, dtype=float)

    if modo == 'essencial' and has_orcamento_proj:
        custo_base_calc = pd.to_numeric(base['ORCAMENTO_PROJETADO'], errors='coerce').fillna(custo_base_calc)
    elif has_orcamento_proj:
        custo_base_calc = custo_base_calc.fillna(pd.to_numeric(base['ORCAMENTO_PROJETADO'], errors='coerce'))

    custo_final_calc = custo_final_calc.combine_first(custo_base_calc * fator_margem)

    base['GASTO_BASE_2026'] = custo_base_calc.fillna(0.0)
    base['GASTO_COM_MARGEM_2026'] = custo_final_calc.fillna(0.0)

    if 'GASTO_COM_MARGEM_2026' in base.columns:
        base = base.sort_values('GASTO_COM_MARGEM_2026', ascending=False)
        base['VALOR_ACUMULADO'] = base['GASTO_COM_MARGEM_2026'].cumsum()
        total_valor = base['GASTO_COM_MARGEM_2026'].sum()
        base['PCT_ACUMULADO'] = (base['VALOR_ACUMULADO'] / total_valor * 100) if total_valor > 0 else 0
        
        base['CLASSE_ABC'] = np.where(
            base['PCT_ACUMULADO'] <= 80, 'A',
            np.where(base['PCT_ACUMULADO'] <= 95, 'B', 'C')
        )
    
    classe_uso_col = 'CLASSE_USO' if 'CLASSE_USO' in base.columns else ('DEMAND_PATTERN' if 'DEMAND_PATTERN' in base.columns else None)
    if classe_uso_col is not None:
        classe_uso_series = base[classe_uso_col].fillna('').astype(str)
        industrial = pd.Series('SEM_CLASSIFICACAO', index=base.index, dtype='object')
        industrial.loc[classe_uso_series == 'Smooth'] = 'ROTINEIRO'
        industrial.loc[classe_uso_series == 'Intermittent'] = 'INTERMITENTE'
        industrial.loc[classe_uso_series == 'Erratic'] = 'OCASIONAL'

        lumpy_mask = classe_uso_series.str.upper() == 'LUMPY'
        industrial.loc[lumpy_mask] = 'MUITO RARO'

        occ_total_raw = base.get('OCCURRENCES_TOTAL')
        occ_total = pd.to_numeric(occ_total_raw, errors='coerce') if occ_total_raw is not None else pd.Series(0, index=base.index)
        occ_total = occ_total.fillna(0)

        occ_12m_raw = base.get('OCCURRENCES_12M')
        occ_12m = pd.to_numeric(occ_12m_raw, errors='coerce') if occ_12m_raw is not None else pd.Series(0, index=base.index)
        occ_12m = occ_12m.fillna(0)

        meses_sem_uso_raw = base.get('MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA')
        meses_sem_uso = pd.to_numeric(meses_sem_uso_raw, errors='coerce') if meses_sem_uso_raw is not None else pd.Series(np.nan, index=base.index)
        if meses_sem_uso.isna().all() and 'META_MESES_SEM_USO' in base.columns:
            meses_sem_uso = pd.to_numeric(base['META_MESES_SEM_USO'], errors='coerce')
        meses_sem_uso = meses_sem_uso.fillna(np.inf)

        lumpy_cfg = get_param('orcamento.uso_industrial.lumpy', {})
        min_occ_total = int(lumpy_cfg.get('min_occ_total_para_raro', 4) or 0)
        min_occ_12m = int(lumpy_cfg.get('min_occ_12m_para_raro', 1) or 0)
        max_meses_sem_uso = lumpy_cfg.get('max_meses_sem_uso_para_raro', 12)
        try:
            max_meses_sem_uso = float(max_meses_sem_uso)
        except (TypeError, ValueError):
            max_meses_sem_uso = 12.0

        raro_mask = lumpy_mask & (
            (occ_total >= min_occ_total) &
            (occ_12m >= min_occ_12m) &
            (meses_sem_uso <= max_meses_sem_uso)
        )
        industrial.loc[raro_mask] = 'RARO'

        base['CLASSE_USO_INDUSTRIAL'] = industrial
        base['CLASSE_USO_INDUSTRIAL_ORD'] = base['CLASSE_USO_INDUSTRIAL'].map(ORDER_MAP_INDUSTRIAL).fillna(99).astype(int)
    
    base = base.drop(columns=['MARGEM_SEGURO'], errors='ignore')
    
    return base


def _build_monthly_price_map(df_consolidado: pd.DataFrame | None) -> Dict[str, Dict[int, float]]:
    """Gera fatores sazonais de preço por mês (1..12) por item.
    A média dos 12 fatores por item é normalizada para 1.0, permitindo
    derivar PRECO_Mm = PRECO_2026 * fator_mês.
    """
    if df_consolidado is None or df_consolidado.empty:
        return {}
    try:
        mp = monthly_price(df_consolidado)
        if mp is None or mp.empty:
            return {}
        mp = mp.copy()
        if 'ANO_MES' not in mp.columns or 'PRECO_MEDIO_MENSAL' not in mp.columns:
            return {}
        # mês do ano
        mp['MES'] = mp['ANO_MES'].astype(str).str[-2:].astype(int)
        # preço médio por mês do ano
        by_m = mp.groupby(['COD_ITEM', 'MES'])['PRECO_MEDIO_MENSAL'].mean().reset_index()
        out: Dict[str, Dict[int, float]] = {}
        for cod, grp in by_m.groupby('COD_ITEM'):
            cod_key = str(cod)
            series = grp.set_index('MES')['PRECO_MEDIO_MENSAL'].astype(float)
            avg = float(series.mean()) if not series.empty else 0.0
            if avg <= 0:
                continue
            # fator = preço_m / média
            fatores = (series / avg).to_dict()
            # normalização adicional para ter soma ~12
            soma = sum(fatores.get(m, 0.0) for m in range(1, 13))
            if soma > 0:
                fatores = {m: fatores.get(m, 0.0) * (12.0 / soma) for m in range(1, 13)}
            out[cod_key] = {m: float(fatores.get(m, 1.0)) for m in range(1, 13)}
        return out
    except Exception:
        return {}


def _gerar_orcamento_mensal(budget: pd.DataFrame,
                            monthly_weights: pd.DataFrame | None,
                            monthly_price_map: Dict[str, Dict[int, float]] | None,
                            cfg: BudgetConfig | None,
                            df_consolidado: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Projeta o orçamento mensal no formato wide (uma linha por item),
    com colunas de QTD_M01..QTD_M12 e GASTO_M01..GASTO_M12.
    A distribuição mensal usa blend entre previsão (FORECAST_SERIES) e
    sazonalidade histórica (monthly_weights). Itens da análise crítica utilizam
    métricas específicas de intervalo e quantidade por evento.
    """
    _ = monthly_price_map  # Mantido para compatibilidade, não utilizado

    if budget is None or budget.empty:
        base_cols = [
            'COD_ITEM', 'DESC_ITEM', 'CLASSE_USO', 'CLASSE_USO_INDUSTRIAL', 'MODEL_USED',
            'QTD_PROJETADA_BASE_2026', 'GASTO_BASE_2026', 'PRECO_2026', 'NUM_TIPO_DESPESA', 'CENTRO_CUSTO', 'CONTA_CONTABIL'
        ]
        qtd_col_names = [f'QTD_M{m:02d}' for m in range(1, 13)]
        gasto_col_names = [f'GASTO_M{m:02d}' for m in range(1, 13)]
        return pd.DataFrame(columns=base_cols + qtd_col_names + gasto_col_names)

    meses = list(range(1, 13))
    ano_orc = getattr(cfg, 'ano_orcamento', 2026) if cfg is not None else 2026
    start_ord = ano_orc * 12
    end_ord = start_ord + 11

    item_expense_map, item_cc_share_map = _build_item_expense_center_maps(df_consolidado)

    def _safe_float_local(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        try:
            out = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(out):
            return None
        return out

    def _parse_year_month(value: Any) -> tuple[int, int] | None:
        if isinstance(value, str):
            match = re.search(r'(\d{4})\D*(\d{1,2})', value)
            if match:
                try:
                    ano = int(match.group(1))
                    mes = int(match.group(2))
                    if 1 <= mes <= 12:
                        return ano, mes
                except ValueError:
                    return None
        elif hasattr(value, 'year') and hasattr(value, 'month'):
            try:
                ano = int(value.year)
                mes = int(value.month)
                if 1 <= mes <= 12:
                    return ano, mes
            except Exception:
                return None
        return None

    def _schedule_critico_item(row: Any) -> Dict[int, float] | None:
        total_qtd = _safe_float_local(row.get(QTD_BASE_COL))
        if total_qtd is None or total_qtd <= 0:
            total_qtd = _safe_float_local(row.get(QTD_FINAL_COL))
        if total_qtd is None or total_qtd <= 0:
            return None

        intervalo = _safe_float_local(row.get('CORREIA_INTERVALO_APLICADO_MESES'))
        if intervalo is None or intervalo <= 0:
            intervalo = _safe_float_local(row.get('INTERVALO_MEDIO_MESES'))
        if intervalo is None or not math.isfinite(intervalo) or intervalo <= 0:
            return None
        intervalo_int = max(1, int(round(intervalo)))

        qtd_evento = _safe_float_local(row.get('CORREIA_QTD_EVENTO_REFERENCIA'))
        if qtd_evento is None or qtd_evento <= 0:
            qtd_evento = _safe_float_local(row.get('QTD_MEDIA_OCORRENCIA'))
        if qtd_evento is None or qtd_evento <= 0:
            qtd_evento = _safe_float_local(row.get('QTD_ULTIMA_OCORRENCIA'))
        if qtd_evento is None or qtd_evento <= 0:
            qtd_evento = total_qtd

        trocas_ref = _safe_float_local(row.get('CORREIA_TROCAS_2026'))
        max_eventos = int(round(trocas_ref)) if trocas_ref and trocas_ref > 0 else None

        first_ord: int | None = None
        prox_info = _parse_year_month(row.get('CORREIA_PROXIMA_TROCA'))
        if prox_info:
            prox_ord = prox_info[0] * 12 + (prox_info[1] - 1)
            if prox_ord < start_ord:
                diff = start_ord - prox_ord
                steps = max(0, (diff + intervalo_int - 1) // intervalo_int)
                prox_ord += steps * intervalo_int
            if start_ord <= prox_ord <= end_ord:
                first_ord = prox_ord

        if first_ord is None:
            ano_ultimo = _safe_float_local(row.get('ULTIMO_ANO_CONSUMO'))
            mes_ultimo = _safe_float_local(row.get('ULTIMO_MES_CONSUMO'))
            if ano_ultimo is not None and mes_ultimo is not None:
                ano_int = int(round(ano_ultimo))
                mes_int = max(1, min(12, int(round(mes_ultimo))))
                last_ord = ano_int * 12 + (mes_int - 1)
                current = last_ord + intervalo_int
                if current < start_ord:
                    diff = start_ord - current
                    steps = max(0, (diff + intervalo_int - 1) // intervalo_int)
                    current += steps * intervalo_int
                if start_ord <= current <= end_ord:
                    first_ord = current

        if first_ord is None or first_ord > end_ord:
            return None

        eventos_por_mes: Dict[int, float] = {}
        restante = float(total_qtd)
        eventos_realizados = 0
        guard = 0
        current_ord = first_ord
        while current_ord <= end_ord and restante > 0 and guard < 120:
            if max_eventos is not None and eventos_realizados >= max_eventos:
                break
            mes = (current_ord % 12) + 1
            alocar = qtd_evento if qtd_evento > 0 else restante
            if alocar > restante:
                alocar = restante
            eventos_por_mes[mes] = eventos_por_mes.get(mes, 0.0) + alocar
            restante -= alocar
            eventos_realizados += 1
            current_ord += intervalo_int
            guard += 1

        if not eventos_por_mes:
            return None

        if restante > 0:
            ultimo_mes = max(eventos_por_mes.keys())
            eventos_por_mes[ultimo_mes] = eventos_por_mes.get(ultimo_mes, 0.0) + restante
            restante = 0.0

        qty_map = {m: 0.0 for m in meses}
        for mes, qtd in eventos_por_mes.items():
            if 1 <= mes <= 12:
                qty_map[mes] = float(qtd)

        distribuido = sum(qty_map.values())
        delta = float(total_qtd) - distribuido
        if abs(delta) > 1e-6:
            alvo = max((m for m, v in qty_map.items() if v > 0), default=max(eventos_por_mes.keys()))
            qty_map[alvo] = qty_map.get(alvo, 0.0) + delta

        return qty_map

    # Carrega pesos históricos por item
    pesos_por_item: Dict[str, Dict[int, float]] = {}
    if monthly_weights is not None and not monthly_weights.empty:
        for cod, grp in monthly_weights.groupby('COD_ITEM'):
            cod_key = str(cod)
            serie = grp.groupby('MES')['PESO'].sum()
            total = float(serie.sum())
            if total > 0:
                pesos_dict: Dict[int, float] = {}
                for m, val in serie.items():
                    try:
                        if isinstance(m, (np.integer, int)):
                            mes_int = int(m)
                        else:
                            mes_int = int(str(m))
                    except Exception:
                        continue
                    pesos_dict[mes_int] = float(val / total)
                pesos_por_item[cod_key] = pesos_dict
            else:
                pesos_por_item[cod_key] = {}

    alpha = float(get_param('orcamento.mensal.blend_forecast_hist', 0.7))
    alpha = float(np.clip(alpha, 0.0, 1.0))

    linhas = []
    for _, row in budget.iterrows():
        cod = row.get('COD_ITEM')
        cod_key = _normalize_cod_item(cod) or ""
        desc = row.get('DESC_ITEM', None)
        classe_uso = row.get('CLASSE_USO', None)
        classe_uso_industrial = row.get('CLASSE_USO_INDUSTRIAL', None)
        model_used_raw = row.get('MODEL_USED', None)
        is_critico_item = _is_critico(row)

        if _is_model_inativo(model_used_raw) and not (_ignorar_inativo_critico() and _is_critico(row)):
            continue

        total_qtd_base = row.get(QTD_BASE_COL, np.nan)
        total_qtd_base = float(total_qtd_base) if pd.notna(total_qtd_base) else np.nan

        total_qtd_final = row.get(QTD_FINAL_COL, np.nan)
        total_qtd_final = float(total_qtd_final) if pd.notna(total_qtd_final) else np.nan

        if pd.notna(total_qtd_base):
            total_qtd_ref = total_qtd_base
        elif pd.notna(total_qtd_final):
            total_qtd_ref = total_qtd_final
        else:
            taxa = row.get('PREDICTED_CONSUMPTION_RATE', np.nan)
            total_qtd_ref = float(taxa) * 12 if pd.notna(taxa) else 0.0

        if pd.isna(total_qtd_final) and pd.notna(total_qtd_ref):
            total_qtd_final = float(total_qtd_ref)

        preco_ref = row.get('PRECO_2026', np.nan)
        preco_ref = float(preco_ref) if pd.notna(preco_ref) else np.nan

        gasto_base_total = row.get('GASTO_BASE_2026', np.nan)
        gasto_base_total = float(gasto_base_total) if pd.notna(gasto_base_total) else np.nan

        cod_key_lookup = cod_key
        if not cod_key_lookup:
            cod_key_lookup = _normalize_cod_item(row.get('COD_ITEM_ORIGINAL')) or ""
        hist = pesos_por_item.get(cod_key, {})
        hist_full = {m: hist.get(m, 0.0) for m in meses}
        s = sum(hist_full.values())
        if s <= 0:
            hist_full = {m: 1.0 / 12.0 for m in meses}
        else:
            hist_full = {m: v / s for m, v in hist_full.items()}

        fc_raw = row.get('FORECAST_SERIES')
        fc_vals: list[float] = []
        if isinstance(fc_raw, (str, bytes)):
            try:
                data = json.loads(fc_raw)
                if isinstance(data, list):
                    fc_vals = [max(float(x), 0.0) for x in data]
            except Exception:
                fc_vals = []
        elif isinstance(fc_raw, (list, tuple)):
            try:
                fc_vals = [max(float(x), 0.0) for x in fc_raw]
            except Exception:
                fc_vals = []
        if fc_vals:
            if len(fc_vals) < 12:
                fc_vals = fc_vals + [0.0] * (12 - len(fc_vals))
            else:
                fc_vals = fc_vals[:12]
        fc_total = float(sum(fc_vals))
        fc_full = {m: (fc_vals[m-1] / fc_total) if fc_total > 0 else 0.0 for m in meses}

        if fc_total > 0:
            applied = {m: alpha * fc_full[m] + (1 - alpha) * hist_full[m] for m in meses}
        else:
            applied = hist_full
        applied_sum = sum(applied.values())
        if applied_sum <= 0:
            qty_shares = {m: 1.0 / 12.0 for m in meses}
        else:
            qty_shares = {m: applied[m] / applied_sum for m in meses}

        qty_values = _schedule_critico_item(row) if is_critico_item else None
        if qty_values is None:
            qty_values = {m: float(total_qtd_ref) * qty_shares[m] for m in meses}

        qtd_cols_item = {m: float(qty_values.get(m, 0.0)) for m in meses}
        if pd.notna(preco_ref):
            preco_scalar = float(preco_ref)
            gasto_cols_item = {m: qtd_cols_item[m] * preco_scalar for m in meses}
        else:
            gasto_cols_item = {m: np.nan for m in meses}

        tipo_raw = item_expense_map.get(cod_key_lookup)
        tipo_display, tipo_norm = _normalize_tipo_despesa(tipo_raw)

        center_shares = item_cc_share_map.get(cod_key_lookup)
        if not center_shares:
            center_shares = {'0000': 1.0}
        else:
            total_share = sum(center_shares.values())
            if total_share <= 0:
                center_shares = {'0000': 1.0}
            else:
                center_shares = {k: float(v) / total_share for k, v in center_shares.items()}

        center_items = list(center_shares.items())
        for idx_share, (centro_raw, share) in enumerate(center_items, start=1):
            centro_norm = _normalize_centro_custo(centro_raw)
            conta_contabil = f"{centro_norm}{tipo_norm}"
            qtd_cols = {
                f'QTD_M{m:02d}': qtd_cols_item[m] * share for m in meses
            }
            if pd.notna(preco_ref):
                gasto_cols = {
                    f'GASTO_M{m:02d}': gasto_cols_item[m] * share for m in meses
                }
            else:
                gasto_cols = {f'GASTO_M{m:02d}': np.nan for m in meses}

            base_info = {
                'COD_ITEM': cod,
                'DESC_ITEM': desc,
                'CLASSE_USO': classe_uso,
                'CLASSE_USO_INDUSTRIAL': classe_uso_industrial,
                'MODEL_USED': model_used_raw,
                QTD_BASE_COL: total_qtd_base * share if pd.notna(total_qtd_base) else np.nan,
                'GASTO_BASE_2026': gasto_base_total * share if pd.notna(gasto_base_total) else np.nan,
                'PRECO_2026': preco_ref,
                'NUM_TIPO_DESPESA': tipo_display,
                'CENTRO_CUSTO': centro_norm,
                'CONTA_CONTABIL': conta_contabil,
            }

            linhas.append(base_info | {k: float(v) for k, v in qtd_cols.items()} | {k: float(v) if not pd.isna(v) else np.nan for k, v in gasto_cols.items()})

    cols_order = [
        'COD_ITEM', 'DESC_ITEM', 'CLASSE_USO', 'CLASSE_USO_INDUSTRIAL', 'MODEL_USED',
        'QTD_PROJETADA_BASE_2026', 'GASTO_BASE_2026', 'PRECO_2026',
        'NUM_TIPO_DESPESA', 'CENTRO_CUSTO', 'CONTA_CONTABIL'
    ] + [f'QTD_M{m:02d}' for m in range(1, 13)] + [f'GASTO_M{m:02d}' for m in range(1, 13)]
    df_out = pd.DataFrame(linhas)
    if not df_out.empty:
        df_out = df_out[cols_order]
    return df_out

# ===== Exportação do orçamento =====
def exportar_orcamento(budget: pd.DataFrame, resumo: dict, out_dir: Path,
                      df_consolidado: pd.DataFrame | None = None,
                      monthly_weights: pd.DataFrame | None = None,
                      cfg: Optional[BudgetConfig] = None) -> Path:
    """
    Exporta o orçamento calculado para Excel, incluindo resumo e detalhes.
    """
    # Nome do arquivo com ano
    ano = budget['ANO_ORCAMENTO'].iloc[0] if 'ANO_ORCAMENTO' in budget.columns else 2026
    out_path = Path(out_dir) / f'ORCAMENTO_2026_SOLIDOS_ML.xlsx'
    
    # Prepara DataFrame para exportação com colunas principais
    cols_principais = [
        'COD_ITEM', 'DESC_ITEM', 'CLASSE_ABC',
        'UM', 'PREDICTED_CONSUMPTION_RATE', 'QTD_PROJETADA_BASE_2026', QTD_FINAL_COL,
        'PRECO_ULTIMO', 'PRECO_2026', 'GASTO_BASE_2026',
        'MARGEM_SEGURANCA_PCT', 'GASTO_COM_MARGEM_2026',
        'MODEL_USED', 'QUALITY_SCORE', 'BAIXA_CONFIABILIDADE',
        # Novas métricas do seletor
        'ADI', 'CV2', 'TREND_STRENGTH', 'SEASONALITY_SIMPLE', 
        'OCCURRENCES_TOTAL', 'OCCURRENCES_12M',
        'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA',
        'CLASSE_USO', 'CLASSE_USO_INDUSTRIAL', 'CLASSE_USO_INDUSTRIAL_ORD', 'USE_RATE',
        'INTERVALO_MEDIO_MESES', 'QTD_MEDIA_OCORRENCIA',
        'QTD_ULTIMA_OCORRENCIA', 'ULTIMO_ANO_CONSUMO', 'ULTIMO_MES_CONSUMO'
    ]
    
    budget_export = budget[[c for c in cols_principais if c in budget.columns]].copy()
    
    # **VALIDAÇÃO CRÍTICA ANTES DE EXPORTAR**
    if 'CLASSE_USO' not in budget.columns:
        logger.warning("[ALERTA] CLASSE_USO ausente no orçamento final!")
    else:
        # Debug: exporta itens ainda sem CLASSE_USO (se houver)
        itens_sem_classe = budget[budget['CLASSE_USO'].isna()]
        if len(itens_sem_classe) > 0:
            debug_path = Path(out_dir) / 'DEBUG_ITENS_SEM_CLASSE_USO.csv'
            itens_sem_classe[['COD_ITEM', 'DESC_ITEM', 'MODEL_USED', 
                              'PREDICTED_CONSUMPTION_RATE', 'PRECO_2026']].to_csv(
                debug_path, index=False, encoding='utf-8-sig'
            )
            logger.warning(f"⚠️ {len(itens_sem_classe)} itens ainda sem CLASSE_USO - veja {debug_path}")
        else:
            logger.info(f"[OK] Todos os {len(budget)} itens possuem CLASSE_USO preenchida")
    
    
    analise_critica_df = _build_analise_critica_df(budget)
    # Preços sazonais por mês, derivados do consolidado original
    monthly_price_map = _build_monthly_price_map(df_consolidado)

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        # Aba principal do orçamento
        budget_export.to_excel(writer, sheet_name='ORCAMENTO_2026', index=False)

        if not analise_critica_df.empty:
            analise_critica_df.to_excel(writer, sheet_name='ANALISE_CRITICA', index=False)
        
        # Resumo por classe ABC
        if 'CLASSE_ABC' in budget.columns:
            # Verifica quais colunas existem antes de agregar
            abc_agg = {}
            if 'COD_ITEM' in budget.columns:
                abc_agg['COD_ITEM'] = 'count'
            if 'GASTO_COM_MARGEM_2026' in budget.columns:
                abc_agg['GASTO_COM_MARGEM_2026'] = 'sum'
            if 'MARGEM_SEGURO' in budget.columns:
                abc_agg['MARGEM_SEGURO'] = 'mean'
        
            if abc_agg:
                abc_summary = budget.groupby('CLASSE_ABC').agg(abc_agg).reset_index()
                
                # Renomeia dinamicamente conforme colunas geradas
                new_cols = ['CLASSE_ABC']
                if 'COD_ITEM' in abc_agg:
                    new_cols.append('QTD_ITENS')
                if 'GASTO_COM_MARGEM_2026' in abc_agg:
                    new_cols.append('VALOR_TOTAL')
                if 'MARGEM_SEGURO' in abc_agg:
                    new_cols.append('MARGEM_MEDIA')
                
                abc_summary.columns = new_cols
                
                if 'VALOR_TOTAL' in abc_summary.columns:
                    abc_summary['PCT_VALOR'] = (abc_summary['VALOR_TOTAL'] / abc_summary['VALOR_TOTAL'].sum() * 100).round(2)
                
                abc_summary.to_excel(writer, sheet_name='RESUMO_ABC', index=False)
        
        # Top 50 itens por valor
        # Seleciona apenas colunas realmente presentes
        top_50_cols = [c for c in cols_principais[:10] if c in budget.columns]
        if 'CLASSE_USO_INDUSTRIAL' in budget.columns and 'CLASSE_USO_INDUSTRIAL' not in top_50_cols:
            insert_idx = 3 if 'CLASSE_ABC' in top_50_cols else len(top_50_cols)
            top_50_cols.insert(insert_idx, 'CLASSE_USO_INDUSTRIAL')
        top_50 = budget.nlargest(50, 'GASTO_COM_MARGEM_2026')[top_50_cols]
        top_50.to_excel(writer, sheet_name='TOP_50_ITENS', index=False)
        
        # Resumo executivo - agrupamento por classe de uso industrial/matriz de modelo
        resumo_exec = budget.copy()

        classe_industrial_series = resumo_exec.get('CLASSE_USO_INDUSTRIAL')
        if classe_industrial_series is None:
            resumo_exec['CLASSE_USO_INDUSTRIAL'] = 'SEM_CLASSIFICACAO'
        else:
            resumo_exec['CLASSE_USO_INDUSTRIAL'] = classe_industrial_series.fillna('SEM_CLASSIFICACAO')

        classe_uso_series = resumo_exec.get('CLASSE_USO')
        if classe_uso_series is None:
            resumo_exec['CLASSE_USO'] = 'SEM_CLASSIFICACAO'
        else:
            resumo_exec['CLASSE_USO'] = classe_uso_series.fillna('SEM_CLASSIFICACAO')

        model_used_series = resumo_exec.get('MODEL_USED')
        if model_used_series is None:
            resumo_exec['MODEL_USED'] = 'DESCONHECIDO'
        else:
            resumo_exec['MODEL_USED'] = model_used_series.fillna('DESCONHECIDO')

        valor_base = resumo_exec.get('GASTO_BASE_2026')
        if valor_base is None:
            resumo_exec['GASTO_BASE_2026'] = 0.0
        else:
            resumo_exec['GASTO_BASE_2026'] = pd.to_numeric(valor_base, errors='coerce').fillna(0.0)

        valor_margem = resumo_exec.get('GASTO_COM_MARGEM_2026')
        if valor_margem is None:
            resumo_exec['GASTO_COM_MARGEM_2026'] = 0.0
        else:
            resumo_exec['GASTO_COM_MARGEM_2026'] = pd.to_numeric(valor_margem, errors='coerce').fillna(0.0)

        group_cols = ['CLASSE_USO_INDUSTRIAL', 'CLASSE_USO', 'MODEL_USED']
        resumo_exec_grp = resumo_exec.groupby(group_cols, dropna=False).agg({
            'COD_ITEM': 'count',
            'GASTO_BASE_2026': 'sum',
            'GASTO_COM_MARGEM_2026': 'sum',
            'CLASSE_USO_INDUSTRIAL_ORD': 'first'
        }).reset_index()

        resumo_exec_grp = resumo_exec_grp.rename(columns={
            'COD_ITEM': 'QTD_ITENS',
            'GASTO_BASE_2026': 'SOMA_GASTO_BASE_2026',
            'GASTO_COM_MARGEM_2026': 'SOMA_GASTO_COM_MARGEM_2026'
        })

        resumo_exec_grp = resumo_exec_grp.sort_values(
            ['CLASSE_USO_INDUSTRIAL_ORD', 'CLASSE_USO', 'MODEL_USED'],
            kind='stable'
        ).reset_index(drop=True)

        total_row = {
            'CLASSE_USO_INDUSTRIAL': 'Total Geral',
            'CLASSE_USO': '',
            'MODEL_USED': '',
            'QTD_ITENS': resumo_exec_grp['QTD_ITENS'].sum(),
            'SOMA_GASTO_BASE_2026': resumo_exec_grp['SOMA_GASTO_BASE_2026'].sum(),
            'SOMA_GASTO_COM_MARGEM_2026': resumo_exec_grp['SOMA_GASTO_COM_MARGEM_2026'].sum(),
            'CLASSE_USO_INDUSTRIAL_ORD': 999
        }
        resumo_exec_final = pd.concat(
            [resumo_exec_grp, pd.DataFrame([total_row])],
            ignore_index=True
        )

        resumo_exec_final.to_excel(writer, sheet_name='RESUMO_EXECUTIVO', index=False)
        
        # Consolidado original (para auditoria)
        if df_consolidado is not None and not df_consolidado.empty:
            limite = get_param('exportacao.limitar_consolidado_original_linhas', 200000)
            if len(df_consolidado) > limite:
                df_consolidado_export = df_consolidado.head(limite)
            else:
                df_consolidado_export = df_consolidado
            df_consolidado_export.to_excel(writer, sheet_name='CONSOLIDADO_ORIGINAL', index=False)

        orcamento_mensal = _gerar_orcamento_mensal(budget, monthly_weights, monthly_price_map, cfg, df_consolidado)
        if not orcamento_mensal.empty:
            orcamento_mensal.to_excel(writer, sheet_name='ORCAMENTO_MENSAL', index=False)
    
    # JSON resumo
    json_path = out_dir / 'resumo_orcamento_2026.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(resumo, f, indent=2, ensure_ascii=False, default=str)
    
    logger.info(f"Orçamento exportado: {out_path}")
    logger.info(f"Resumo JSON: {json_path}")

    # Incrementa abas de compatibilidade (NÃO REMOVE NADA)
    try:
        out_dir = Path(out_dir)
        pred_csv = out_dir / 'predicoes.csv'
        aval_csv = out_dir / 'avaliacao.csv'
        append_ml_tabs_to_excel(Path(out_path), pred_csv, aval_csv)
        logger.info("Abas de compatibilidade (PREVISOES/CLASSIFICACAO) adicionadas com sucesso.")
    except Exception as e:
        # Evita quebrar exportação caso haja algum problema de arquivo ausente
        logger.warning(f"Aviso: não foi possível adicionar abas de compatibilidade: {e}")

    # **DEBUG**: Exporta itens sem CLASSE_USO para análise
    if 'CLASSE_USO' in budget.columns:
        itens_sem_classe = budget[budget['CLASSE_USO'].isna()]
        
        if not itens_sem_classe.empty:
            debug_path = out_dir / 'DEBUG_ITENS_SEM_CLASSE_USO.csv'
            itens_sem_classe[['COD_ITEM', 'DESC_ITEM', 'MODEL_USED', 
                              'PREDICTED_CONSUMPTION_RATE', 'PRECO_2026']].to_csv(debug_path, index=False)
            logger.warning(f"⚠️ DEBUG: {len(itens_sem_classe)} itens sem CLASSE_USO exportados para {debug_path}")
    
    return out_path

# ===== NOVO: Pipeline completo com integração ML =====
def run_budget_projection(input_path: str | Path, output_dir: str | Path, 
                         aux_path: Optional[str | Path] = None,
                         cfg: Optional[BudgetConfig] = None) -> Tuple[Path, Dict]:
    """Pipeline completo de projeção orçamentária"""
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cfg = cfg or BudgetConfig()

    # Define aux_path padrão se não fornecido
    if aux_path is None:
        aux_path_cfg = get_param('sistema.paths.tab_aux', 'data/TAB_AUX.xlsx')
        aux_path = Path(aux_path_cfg)
        if not aux_path.is_absolute():
            aux_path = Path(__file__).resolve().parent.parent / aux_path
    else:
        aux_path = Path(aux_path)

    logger.info("=== PROJEÇÃO ORÇAMENTÁRIA 2026 - SÓLIDOS ML ===")
    logger.info(f"Entrada: {input_path}")
    logger.info(f"Saída: {output_dir}")
    logger.info(f"AUX: {aux_path} (existe: {aux_path.exists()})")

    # 1) Executa pipeline ML se necessário
    # Agora os CSVs são esperados na raiz de orcamento_2026; ainda suportamos ml_results como fallback
    ml_dir = _find_ml_output_dir(output_dir)
    need_run_pipeline = not (ml_dir / 'predicoes.csv').exists()
    if need_run_pipeline:
        logger.info("Executando pipeline ML...")
        PipelineConfig = None
        try:
            from Modulo_Solidos_ML.pipeline import Config as PipelineConfig, run_pipeline
        except Exception:
            from Modulo_Solidos_ML.pipeline import run_pipeline
        ml_config = PipelineConfig(horizonte_previsao_meses=cfg.horizonte_meses) if PipelineConfig is not None else None
        try:
            if ml_config is not None:
                run_pipeline(input_path, output_dir, ml_config, getattr(cfg, 'modo_orcamento', 'essencial'))
            else:
                run_pipeline(input_path, output_dir)
        except TypeError:
            if ml_config is not None:
                run_pipeline(input_path, output_dir, ml_config)
            else:
                run_pipeline(input_path, output_dir)
        logger.info("Pipeline ML concluído.")
        # Reavaliar localização após rodar o pipeline
        ml_dir = _find_ml_output_dir(output_dir)
    else:
        logger.info(f"Usando resultados ML existentes em: {ml_dir}")

    # 2) Carrega dados históricos
    logger.info("Carregando dados históricos...")
    # Função local para carregar dados históricos
    def carregar_dados_historicos(input_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        df_consolidado = load_and_preprocess(input_path)
        mensal_qtd, mensal_valor = monthly_qtd_val(df_consolidado)
        return mensal_qtd, mensal_valor, df_consolidado
    mensal_qtd, mensal_valor, df_consolidado = carregar_dados_historicos(input_path)

    monthly_weights = build_monthly_distribution(mensal_qtd)

    # 2b) Exporta MENSAL_SERIES + INDICADORES para auditoria
    ind = build_indicadores(mensal_qtd, df_consolidado)
    ml_wb = export_ml_consumo(mensal_qtd, mensal_valor, ind, output_dir)
    logger.info(f"ML_CONSUMO_SOLIDOS.xlsx gerado em: {ml_wb}")

    # 3) Integra ML com preços
    logger.info("Integrando previsões ML com métricas de preço...")
    budget_base = integrar_ml_com_precos(ml_dir, mensal_qtd, monthly_price(df_consolidado), Path(aux_path))

    # 4) Calcula projeção orçamentária (inclui modo 'proposto')
    logger.info("Calculando projeção orçamentária...")
    budget = calcular_projecao_orcamentaria(budget_base, cfg)

    # 5) Validação final antes de exportar
    logger.info("\n" + "="*60)
    logger.info("VALIDAÇÃO DO ORÇAMENTO GERADO")
    logger.info("="*60)
    
    colunas_obrigatorias = ['COD_ITEM', 'DESC_ITEM', 'CLASSE_ABC', 'CLASSE_USO', 
                            QTD_FINAL_COL, 'PRECO_2026', 'GASTO_COM_MARGEM_2026']
    
    for col in colunas_obrigatorias:
        if col in budget.columns:
            nao_nulos = budget[col].notna().sum()
            logger.info(f"[OK] {col:30s}: {nao_nulos}/{len(budget)} ({100*nao_nulos/len(budget):.1f}%)")
        else:
            logger.error(f"[ERRO] {col:30s}: COLUNA AUSENTE")
    
    if 'CLASSE_USO' in budget.columns:
        logger.info(f"\nDistribuição de CLASSE_USO:")
        for classe, count in budget['CLASSE_USO'].value_counts().items():
            logger.info(f"   {classe:15s}: {count:5d} itens ({100*count/len(budget):5.1f}%)")
    
    logger.info("="*60 + "\n")

    # 6) Exporta resultados para o diretório forçado
    resumo = {
        'total_itens': len(budget),
        'orcamento_total_2026': float(budget['GASTO_COM_MARGEM_2026'].sum())
    }
    excel_path = exportar_orcamento(
        budget,
        resumo,
        output_dir,
        df_consolidado=df_consolidado,
        monthly_weights=monthly_weights,
        cfg=cfg,
    )
    logger.info(f"Orçamento salvo em: {excel_path}")

    return excel_path, resumo

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Projeção Orçamentária 2026 - Sólidos ML')
    parser.add_argument('-i', '--input', 
                       default=str(Path(__file__).resolve().parents[1] / 'data/BASE_HISTORICA.xlsx'),
                       help='Arquivo histórico de entrada')
    parser.add_argument('-o', '--output', 
                       default=str(Path(__file__).resolve().parent / 'orcamento_2026'),
                       help='Diretório de saída')
    parser.add_argument('--aux', help='Caminho para data/TAB_AUX.xlsx (opcional)')
    parser.add_argument('--margem', type=float, default=0.15, 
                       help='Margem de segurança base (pct)')
    
    args = parser.parse_args()
    
    cfg = BudgetConfig(margem_seguranca_pct=args.margem)
    excel_path, resumo = run_budget_projection(args.input, args.output, args.aux, cfg)
    
    logger.info(f"\nConcluído! Orçamento salvo em: {excel_path}")
def _coerce_cod_item_dtype(df: pd.DataFrame, target_dtype: Any) -> pd.DataFrame:
    """
    Ajusta a coluna COD_ITEM para o dtype de referência.
    Remove linhas cujo valor não possa ser convertido.
    """
    if 'COD_ITEM' not in df.columns:
        return df

    df = df.copy()
    try:
        dtype_target = np.dtype(target_dtype)
    except (TypeError, ValueError):
        dtype_target = np.dtype('O')

    kind = dtype_target.kind

    if kind in {'i', 'u', 'f'}:
        coerced = pd.to_numeric(df['COD_ITEM'], errors='coerce')
        mask = coerced.notna()
        df = df.loc[mask].copy()
        coerced = coerced.loc[mask]
        df['COD_ITEM'] = coerced.astype(dtype_target)
    else:
        df['COD_ITEM'] = df['COD_ITEM'].astype(str)

    return df





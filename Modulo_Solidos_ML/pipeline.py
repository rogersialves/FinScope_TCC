
import sys
import json
import math
import argparse
from pathlib import Path

# Garante que o diretório raiz do projeto esteja no sys.path quando o script é executado diretamente.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
# + leitura de parâmetros
from core.fin_params import get_param
from core.logger import get_logger
from core.pipeline_utils import (
    normalize_cod_item_series,
    padronizar_desc_item as padronizar_desc_item_util,
    consolidar_duplicatas_exatas as consolidar_duplicatas_util,
)

_logger = get_logger(__name__)
from dataclasses import dataclass
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List, Union  # Tipagens auxiliares

import numpy as np

# Assegura logs legíveis em ambientes que não estão em UTF-8
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore
except Exception as exc:  # noqa: BLE001 - ambiente atípico
    _logger.debug("Não foi possível reconfigurar stdout: %s", exc)

# Importa filtros de negócio compartilhados


from Modulo_Solidos_ML.data_import import load_and_preprocess



# Dependências opcionais para Modo Inteligente

try:

    from statsmodels.tsa.arima.model import ARIMA  # type: ignore

    _ARIMA_AVAILABLE = True

except ImportError:  # pragma: no cover - dependente de ambiente

    _ARIMA_AVAILABLE = False



try:

    from sklearn.ensemble import RandomForestClassifier  # type: ignore

    from sklearn.model_selection import train_test_split  # type: ignore

    from sklearn.metrics import accuracy_score  # type: ignore

    _SKLEARN_AVAILABLE = True

except ImportError:  # pragma: no cover - dependente de ambiente

    _SKLEARN_AVAILABLE = False



@dataclass

class Config:

    horizonte_previsao_meses: int = 3

    alpha_grid: Tuple[float, ...] = (0.05, 0.1, 0.2, 0.3)

    critical_cod_items: set[str] | None = None

    ignore_inactive_for_critical: bool = True



# ===== Utilidades =====

def _padronizar_desc_item(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper para compatibilidade - usa padronizar_desc_item do core."""
    return padronizar_desc_item_util(df)



def _consolidar_duplicatas_exatas(df: pd.DataFrame) -> pd.DataFrame:
    """Wrapper para compatibilidade - usa consolidar_duplicatas_exatas do core."""
    return consolidar_duplicatas_util(df)



def _normalize_cod_item(series: pd.Series) -> pd.Series:
    """Wrapper para compatibilidade - usa normalize_cod_item_series do core."""
    return normalize_cod_item_series(series)



# ===== 1) Carregar dados =====

def load_data(input_path: str | Path) -> pd.DataFrame:

    """

    Carrega dados para o ML.

    - Se o arquivo já for o HISTORICO_FINAL.xlsx, apenas lê a planilha CONSOLIDADO.

    - Caso contrário, aplica os filtros via data_import.load_and_preprocess e gera HISTORICO_FINAL.xlsx.

    """

    input_path = Path(input_path)

    _logger.info("=== CARREGANDO DADOS PARA ML ===")

    

    # Se o arquivo é HISTORICO_FINAL.xlsx e existe, carrega direto

    if input_path.name.upper() == 'HISTORICO_FINAL.xlsx'.upper() and input_path.exists():

        xls = pd.ExcelFile(input_path)

        sheet = 'CONSOLIDADO' if 'CONSOLIDADO' in xls.sheet_names else xls.sheet_names[0]

        df = pd.read_excel(xls, sheet_name=sheet)

        # Garante colunas esperadas pelo pipeline

        req = ['COD_ITEM', 'DESC_ITEM', 'QUANTIDADE', 'VALOR', 'DATA_ENTREGA']

        missing = [c for c in req if c not in df.columns]

        if missing:

            raise KeyError(f"Colunas ausentes em HISTORICO_FINAL.xlsx: {missing}")

        _logger.info("Dados (filtrado) carregados: %d registros; %d itens.", len(df), df['COD_ITEM'].nunique())

        out = df[req].copy()

        out['COD_ITEM'] = _normalize_cod_item(out['COD_ITEM'])

        return out

    

    # Se HISTORICO_FINAL.xlsx não existe ou outro arquivo foi informado, processa BASE_HISTORICA.xlsx

    if input_path.name.upper() == 'HISTORICO_FINAL.xlsx'.upper() and not input_path.exists():

        _logger.warning("HISTORICO_FINAL.xlsx não encontrado em %s", input_path)

        # Busca BASE_HISTORICA.xlsx a partir da configuração (fallback: data/)

        default_hist = Path(__file__).resolve().parents[1] / 'data/BASE_HISTORICA.xlsx'

        historico_path = Path(get_param('sistema.paths.historico', str(default_hist)))  # Volta 2 níveis: MODELO_ORCAMENTO_ML -> Modulo_Solidos_ML -> raiz

        if not historico_path.exists():

            raise FileNotFoundError(f"BASE_HISTORICA.xlsx não encontrado em {historico_path}")

        _logger.info("Usando BASE_HISTORICA.xlsx de: %s", historico_path)

        input_path = historico_path

    

    # Processa BASE_HISTORICA.xlsx e salva HISTORICO_FINAL.xlsx no diretório correto

    _logger.info("Processando e filtrando: %s", input_path)

    df_filtrado = load_and_preprocess(input_path)

    

    # Cria HISTORICO_FINAL.xlsx no diretório orcamento_2026

    output_dir = Path("Modulo_Solidos_ML/orcamento_2026")

    output_dir.mkdir(parents=True, exist_ok=True)

    historico_final_path = output_dir / 'HISTORICO_FINAL.xlsx'

    

    _logger.info("Salvando HISTORICO_FINAL.xlsx em: %s", historico_final_path)

    with pd.ExcelWriter(historico_final_path, engine='openpyxl') as writer:

        df_filtrado.to_excel(writer, sheet_name='CONSOLIDADO', index=False)

    

    _logger.info("Dados após filtros: %d registros", len(df_filtrado))

    _logger.info("Itens únicos após filtros: %d", df_filtrado['COD_ITEM'].nunique())

    

    colunas_ml = ['COD_ITEM', 'DESC_ITEM', 'QUANTIDADE', 'VALOR', 'DATA_ENTREGA']

    # Se UM existir, inclui

    if 'UM' in df_filtrado.columns:

        colunas_ml.insert(2, 'UM')

    out = df_filtrado[colunas_ml].copy()

    out['COD_ITEM'] = _normalize_cod_item(out['COD_ITEM'])

    return out



# ===== 2) Validação e limpeza =====

def validate_and_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    issues = []

    # Datas válidas

    df['DATA_ENTREGA'] = pd.to_datetime(df['DATA_ENTREGA'], errors='coerce')

    min_date = pd.Timestamp('2010-01-01')

    max_date = pd.Timestamp.today().normalize()

    invalid_date_mask = (df['DATA_ENTREGA'].isna()) | (df['DATA_ENTREGA'] < min_date) | (df['DATA_ENTREGA'] > max_date)

    if invalid_date_mask.any():

        issues.append({'tipo': 'DATA_INVALIDA', 'registros': int(invalid_date_mask.sum())})

        df = df.loc[~invalid_date_mask].copy()



    # Quantidade/valor não-negativos

    df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0)

    df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)

    neg_mask = (df['QUANTIDADE'] < 0) | (df['VALOR'] < 0)

    if neg_mask.any():

        issues.append({'tipo': 'NEGATIVOS_REMOVIDOS', 'registros': int(neg_mask.sum())})

        df = df.loc[~neg_mask].copy()



    # Duplicatas exatas já tratadas em _consolidar_duplicatas_exatas

    return df, pd.DataFrame(issues)





# ===== 3) Série mensal =====

def build_monthly_series(df: pd.DataFrame) -> pd.DataFrame:

    df['ANO_MES'] = pd.to_datetime(df['DATA_ENTREGA']).dt.to_period('M')  # type: ignore[reportAttributeAccessIssue]

    g = df.groupby(['COD_ITEM', 'ANO_MES'])[['QUANTIDADE', 'VALOR']].sum().rename(columns={'QUANTIDADE': 'QTD_MENSAL', 'VALOR': 'VALOR_MENSAL'})

    g = g.reset_index()

    # Calendário completo por item

    out = []

    for cod, sub in g.groupby('COD_ITEM'):

        pmin = sub['ANO_MES'].min()

        pmax = sub['ANO_MES'].max()

        full = pd.DataFrame({'ANO_MES': pd.period_range(pmin, pmax, freq='M')})

        merged = full.merge(sub, on='ANO_MES', how='left')

        merged['COD_ITEM'] = cod

        merged['QTD_MENSAL'] = merged['QTD_MENSAL'].fillna(0)

        merged['VALOR_MENSAL'] = merged['VALOR_MENSAL'].fillna(0)

        out.append(merged[['COD_ITEM', 'ANO_MES', 'QTD_MENSAL', 'VALOR_MENSAL']])

    mensal = pd.concat(out, ignore_index=True)

    if not mensal.empty and 'COD_ITEM' in mensal.columns:

        mensal['COD_ITEM'] = _normalize_cod_item(mensal['COD_ITEM'])

    return mensal





# ===== 4) Indicadores =====

def compute_indicators(mensal: pd.DataFrame, original: pd.DataFrame) -> pd.DataFrame:

    # Primeira/última data do item (no histórico original)

    base = original.copy()

    base['DATA_ENTREGA'] = pd.to_datetime(base['DATA_ENTREGA'], errors='coerce')

    first = base.groupby('COD_ITEM')['DATA_ENTREGA'].min().rename('PRIMEIRA_DATA')

    last = base.groupby('COD_ITEM')['DATA_ENTREGA'].max().rename('ULTIMA_DATA')



    # Métricas por item com base no mensal completo

    mensal['HAS_CONS'] = mensal['QTD_MENSAL'] > 0

    meses_com = mensal.groupby('COD_ITEM')['HAS_CONS'].sum().rename('MESES_COM_CONSUMO')

    # meses analisados desde 1ª ocorrência

    first_period = mensal.loc[mensal['HAS_CONS']].groupby('COD_ITEM')['ANO_MES'].min().rename('FIRST_PERIOD')

    last_period = mensal.groupby('COD_ITEM')['ANO_MES'].max().rename('LAST_PERIOD')



    def _months_diff_inclusive(fp, lp) -> int:

        try:

            if isinstance(fp, pd.Period) and isinstance(lp, pd.Period):

                return int((lp.year - fp.year) * 12 + (lp.month - fp.month) + 1)

            # tenta parsear como string YYYY-MM

            def _ym(x):

                xs = str(x)

                parts = xs.split('-')

                return int(parts[0]), int(parts[1])

            y1, m1 = _ym(fp)

            y2, m2 = _ym(lp)

            return int((y2 - y1) * 12 + (m2 - m1) + 1)

        except (ValueError, TypeError, AttributeError):  # parsing de período inválido

            return 0



    rows = []

    for cod in mensal['COD_ITEM'].unique():

        fp = first_period.get(cod, pd.NA)

        lp = last_period.get(cod, pd.NA)

        mc = int(meses_com.get(cod, 0))

        if pd.isna(fp) or pd.isna(lp):

            total = 0

            taxa = 0.0

            gap_post = 0

        else:

            total = _months_diff_inclusive(fp, lp)

            taxa = float(mc / total) if total > 0 else 0.0

            # meses sem ocorrência após última ocorrência

            ssub = mensal[(mensal['COD_ITEM'] == cod)]

            pos = ssub[ssub['HAS_CONS']].index.tolist()

            if pos:

                last_idx = pos[-1]

                gap_post = int(len(ssub) - (ssub.index.get_loc(last_idx) + 1))

            else:

                gap_post = total

        rows.append({

            'COD_ITEM': cod,

            'MESES_ANALISADOS_DESDE_1A_OCORRENCIA': total,

            'MESES_COM_CONSUMO': mc,

            'TAXA_MESES_COM_CONSUMO': taxa,

            'MESES_SEM_OCORRENCIA_APOS_ULTIMA_OCORRENCIA': gap_post

        })

    ind = pd.DataFrame(rows).merge(first, on='COD_ITEM', how='left').merge(last, on='COD_ITEM', how='left')

    if 'COD_ITEM' in ind.columns:

        ind['COD_ITEM'] = _normalize_cod_item(ind['COD_ITEM'])

    return ind





# ===== 5) ADI e CV² + Classificação =====

def compute_adi_cv2(mensal: pd.DataFrame) -> pd.DataFrame:

    rows = []

    for cod, sub in mensal.groupby('COD_ITEM'):

        s = sub.sort_values('ANO_MES')

        q = s['QTD_MENSAL'].values.astype(float)

        pos = q > 0  # type: ignore[reportOperatorIssue]

        mc = int(pos.sum())

        

        if mc == 0:

            adi = math.inf

            cv2 = math.inf

        elif mc == 1:

            # **CORREÇÃO**: Para 1 única ocorrência, ADI deve ser muito alto

            total_per = len(q)  # período total observado

            adi = float(total_per)  # ADI = período total (já que só houve 1 demanda)

            cv2 = math.inf  # variabilidade indefinida com 1 ponto

        else:

            # ADI = período analisado / número de demandas

            idx_pos = np.where(pos)[0]

            primeiro_idx = idx_pos[0]

            ultimo_idx = idx_pos[-1]

            periodo_ativo = ultimo_idx - primeiro_idx + 1  # do primeiro ao último consumo

            adi = float(periodo_ativo / mc) if mc > 0 else math.inf

            

            # CV2: nos meses com consumo

            nz = np.asarray(q[pos])  # type: ignore[reportArgumentType]

            if len(nz) <= 1:

                cv2 = math.inf

            else:

                mu = float(np.mean(nz))  # type: ignore[reportCallIssue]

                sd = float(np.std(nz, ddof=0))  # type: ignore[reportCallIssue]

                cv2 = float((sd / mu) ** 2) if mu > 0 else math.inf

                

        rows.append({'COD_ITEM': cod, 'ADI': adi, 'CV2': cv2})

    out = pd.DataFrame(rows)

    if 'COD_ITEM' in out.columns:

        out['COD_ITEM'] = _normalize_cod_item(out['COD_ITEM'])

    return out



def classify_demand_patterns(adi_cv2: pd.DataFrame) -> pd.DataFrame:

    def _cls(adi, cv2):

        # **CORREÇÃO**: Trata casos especiais primeiro

        if pd.isna(adi) or pd.isna(cv2):

            return 'Lumpy'  # conservador para casos indefinidos

        if adi == math.inf or cv2 == math.inf:

            return 'Lumpy'  # conservador para 1 ocorrência ou sem demanda

        

        # Classificação padrão

        if adi < 1.32 and cv2 < 0.49:

            return 'Smooth'

        elif adi < 1.32 and cv2 >= 0.49:

            return 'Erratic'

        elif adi >= 1.32 and cv2 < 0.49:

            return 'Intermittent'

        else:  # adi >= 1.32 and cv2 >= 0.49

            return 'Lumpy'

    

    out = adi_cv2.copy()

    if 'COD_ITEM' in out.columns:

        out['COD_ITEM'] = _normalize_cod_item(out['COD_ITEM'])

    out['DEMAND_PATTERN'] = [_cls(a, c) for a, c in zip(out['ADI'], out['CV2'])]

    return out



# ===== Métricas de erro e qualidade =====

def _mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:

    y_true = np.asarray(y_true, dtype=float)

    y_pred = np.asarray(y_pred, dtype=float)

    if len(y_true) == 0:

        return np.nan

    return float(np.mean(np.abs(y_true - y_pred)))





def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:

    y_true = np.asarray(y_true, dtype=float)

    y_pred = np.asarray(y_pred, dtype=float)

    if len(y_true) == 0:

        return np.nan

    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))





def _mase(y_true: np.ndarray, y_pred: np.ndarray, m: int = 1) -> float:

    y_true = np.asarray(y_true, dtype=float)

    y_pred = np.asarray(y_pred, dtype=float)

    n = len(y_true)

    if n <= m:

        return np.nan

    # Escala pelo erro do naive sazonal

    denom = np.mean(np.abs(y_true[m:] - y_true[:-m]))

    if denom == 0 or np.isnan(denom):

        return np.nan

    return float(np.mean(np.abs(y_true - y_pred)) / denom)





def _quality_score(mase: float) -> Optional[float]:

    if mase is None or pd.isna(mase):

        return np.nan

    # Escore simples baseado em MASE (menor é melhor)

    if mase <= 0.5:

        return 90.0

    if mase <= 1.0:

        return 75.0

    if mase <= 1.5:

        return 60.0

    if mase <= 2.0:

        return 50.0

    return 40.0





# ===== 6) Modelos base (SES e Croston) =====

def _ses_forecast(y: np.ndarray, horizon: int, alphas: tuple=(0.05,0.1,0.2,0.3)) -> tuple[np.ndarray, float, float, Dict[str, Any]]:

    # Validação temporal simples: última parte como validação

    n = len(y)

    if n == 0 or len(alphas) == 0:

        return np.zeros(horizon, dtype=float), np.nan, np.nan, {'alpha': np.nan}

    best = None

    val_size = max(1, int(n * 0.2))

    train = y[:-val_size] if n > (val_size + 1) else y

    val = y[-val_size:] if n > (val_size + 1) else np.array([])



    for alpha in alphas:

        lvl = None

        preds = []

        # treino + validação walk-forward

        series = np.concatenate([train, val])

        for i, obs in enumerate(series):

            lvl = obs if lvl is None else alpha * obs + (1 - alpha) * lvl

            # previsão 1 passo à frente é o nível atual

            preds.append(lvl)

        # Avalia na validação (prev de t usa nível de t-1; ajuste simples)

        if len(val) > 0:

            val_pred = np.array(preds[len(train)-1:-1])  # alinhar

            mase = _mase(val, val_pred, m=1)

        else:

            mase = np.nan

        if best is None or (not pd.isna(mase) and mase < best[1]):

            best = (alpha, mase, lvl)

    if best is None:

        return np.zeros(horizon, dtype=float), np.nan, np.nan, {'alpha': np.nan}

    alpha, mase, last_level = best

    # Previsão multi-passos: nível constante

    if last_level is None or pd.isna(last_level):

        forecast = np.zeros(horizon, dtype=float)

    else:

        forecast = np.array([last_level] * horizon, dtype=float)

    return forecast, mase, alpha, {'alpha': alpha}



def _croston_forecast(y: np.ndarray, horizon: int, alpha: float=0.1) -> tuple[np.ndarray, Optional[float], Dict[str, Any]]:

    # Implementação Croston padrão (previsão = demanda média / intervalo médio)

    y = np.asarray(y, dtype=float)

    n = len(y)

    a_d = None  # nível da demanda

    a_p = None  # nível do intervalo

    p = 1

    last = 0

    for t in range(n):

        if y[t] > 0:

            a_d = y[t] if a_d is None else a_d + alpha * (y[t] - a_d)

            a_p = p if a_p is None else a_p + alpha * (p - a_p)

            p = 1

            last = t

        else:

            p += 1

    if a_d is None or a_p is None or a_p == 0:

        fc = np.zeros(horizon, dtype=float)

    else:

        rate = a_d / a_p

        fc = np.array([rate] * horizon, dtype=float)

    return fc, None, {'alpha': alpha}



def _croston_forecast_safe(y: np.ndarray, horizon: int, alpha: float=0.1) -> tuple[np.ndarray, Optional[float], Dict[str, Any]]:

    """Versão segura do Croston que trata casos extremos com poucas ocorrências"""

    y = np.asarray(y, dtype=float)

    non_zero_mask = y > 0

    ocorrencias = np.sum(non_zero_mask)

    n = len(y)

    

    # Remove DEBUG print para reduzir spam de log

    # print(f"DEBUG - Item com {int(ocorrencias)} ocorrências em {n} períodos")

    

    if ocorrencias == 0:

        # Nenhuma demanda histórica

        return np.zeros(horizon, dtype=float), None, {'alpha': alpha, 'strategy': 'no_demand'}

    

    if ocorrencias == 1:

        # **CORREÇÃO**: Item ultra-raro - usa abordagem conservadora

        valor_unico = float(np.max(y))

        

        # Para itens com 1 única ocorrência em muitos anos: 

        # Assume que é um item de renovação muito rara

        if n >= 36:  # 3+ anos de histórico

            # Item provavelmente de reposição muito ocasional

            # Distribui ao longo de 10+ anos (muito conservador)

            periodo_renovacao = n * 5  # 5x o período observado

            taxa_mensal = valor_unico / periodo_renovacao

        else:

            # Período curto: ainda mais conservador

            periodo_renovacao = n * 10

            taxa_mensal = valor_unico / periodo_renovacao

        

        # Remove DEBUG print

        # print(f"DEBUG - Valor único: {valor_unico}, período: {periodo_renovacao}, taxa: {taxa_mensal}")

        

        fc = np.array([taxa_mensal] * horizon, dtype=float)

        return fc, None, {

            'alpha': alpha, 

            'strategy': 'ultra_rare_single', 

            'period_estimate': int(periodo_renovacao),

            'monthly_rate': taxa_mensal

        }

    

    if ocorrencias == 2:

        # Duas ocorrências: calcula intervalo real e extrapola

        indices_demanda = np.where(non_zero_mask)[0]

        intervalo_real = indices_demanda[1] - indices_demanda[0]

        valor_medio = float(np.mean(y[non_zero_mask]))

        

        # Assume que o padrão continua (com margem de segurança)

        intervalo_esperado = intervalo_real * 1.5  # 50% mais conservador

        taxa_mensal = valor_medio / intervalo_esperado

        

        # Remove DEBUG print

        # print(f"DEBUG - 2 ocorrências: intervalo real {intervalo_real}, taxa: {taxa_mensal}")

        

        fc = np.array([taxa_mensal] * horizon, dtype=float)

        return fc, None, {

            'alpha': alpha, 

            'strategy': 'two_occurrences', 

            'real_interval': int(intervalo_real),

            'expected_interval': intervalo_esperado

        }

    

    if ocorrencias <= 5:

        # Poucas ocorrências: usa abordagem de renovação

        demandas = y[non_zero_mask]

        indices_demanda = np.where(non_zero_mask)[0]

        

        # Intervalo médio entre demandas

        if len(indices_demanda) > 1:

            intervalos = np.diff(indices_demanda)

            intervalo_medio = float(np.mean(intervalos))

        else:

            intervalo_medio = float(n)

        

        valor_medio = float(np.mean(demandas))

        

        # Aplica fator conservador para poucos dados

        fator_conservador = 2.0  # Dobra o intervalo (mais conservador)

        intervalo_ajustado = intervalo_medio * fator_conservador

        

        taxa_mensal = valor_medio / intervalo_ajustado

        

        fc = np.array([taxa_mensal] * horizon, dtype=float)

        return fc, None, {

            'alpha': alpha, 

            'strategy': 'few_occurrences_renewal', 

            'occurrences': int(ocorrencias),

            'avg_interval': intervalo_medio,

            'conservative_interval': intervalo_ajustado

        }

    

    # Ocorrências suficientes: usa Croston padrão

    return _croston_forecast(y, horizon, alpha)



# ===== Modo Proposto - Modelos Avançados =====

def _croston_modified_forecast(y: np.ndarray, horizon: int, alpha: float = 0.1, beta: float = 0.1) -> tuple[np.ndarray, Optional[float], Dict[str, Any]]:

    """Croston Modificado com ajuste simples de tendência."""

    y = np.asarray(y, dtype=float)

    n = len(y)

    

    croston_cfg = get_param('ml.croston_modificado', {})

    trend_limit_short = float(croston_cfg.get('trend_limit_short', 0.15))

    trend_limit_long = float(croston_cfg.get('trend_limit_long', 0.3))

    min_periods_trend = int(croston_cfg.get('min_periods_for_trend', 6))

    min_periods_trend = max(min_periods_trend, 1)



    # Séries muito curtas não devem usar tendência nem Croston modificado

    if n < max(6, min_periods_trend):

        # Fallback para Croston Safe (sem tendência)

        return _croston_forecast_safe(y, horizon, alpha)

    

    trend = np.polyfit(np.arange(n), y, 1)[0] if n > 3 else 0.0

    

    # **CORREÇÃO**: Limitar tendência a um threshold razoável (séries curtas recebem limite menor)

    if n <= 12:

        limit = max(trend_limit_short, 0.0)

    else:

        limit = max(trend_limit_long, 0.0)

    trend = float(np.clip(trend, -limit, limit))

    

    a_d = None

    a_p = None

    p = 1

    for t in range(n):

        if y[t] > 0:

            demanda_ajustada = y[t] + trend * t

            a_d = demanda_ajustada if a_d is None else a_d + alpha * (demanda_ajustada - a_d)

            a_p = p if a_p is None else a_p + beta * (p - a_p)

            p = 1

        else:

            p += 1

    if a_d is None or a_p is None or a_p == 0:

        fc = np.zeros(horizon, dtype=float)

        meta = {'alpha': alpha, 'beta': beta, 'trend': trend, 'rate': 0.0}

        return fc, None, meta



    rate = float(a_d / a_p)

    positivos = y[y > 0]

    media_positiva = float(positivos.mean()) if positivos.size else 0.0

    max_permitido = media_positiva * 3 if media_positiva > 0 else None



    valores = []

    for i in range(horizon):

        valor = max(0.0, rate + trend * (n + i))

        if max_permitido is not None:

            valor = min(valor, max_permitido)

        valores.append(valor)

    fc = np.array(valores, dtype=float)



    if np.allclose(fc, 0.0) and rate > 0.0:

        fallback_fc, fallback_metric, fallback_meta = _croston_forecast_safe(y, horizon, alpha)

        combined_meta = {

            'alpha': alpha,

            'beta': beta,

            'trend': trend,

            'rate': rate,

            'fallback': 'croston_safe_due_to_zero_forecast'

        }

        combined_meta.update({f'safe_{k}': v for k, v in fallback_meta.items()})

        return fallback_fc, fallback_metric, combined_meta



    meta = {'alpha': alpha, 'beta': beta, 'trend': trend, 'rate': rate}

    return fc, None, meta



def _arima_forecast(y: np.ndarray, horizon: int) -> tuple[np.ndarray, Optional[float], Dict[str, Any]]:

    """ARIMA opcional por busca simples de (p,d,q) via AIC."""

    if not _ARIMA_AVAILABLE:

        return np.zeros(horizon, dtype=float), None, {'error': 'ARIMA not available'}

    try:

        best_aic = float('inf')

        best_model = None

        best_order = None

        for p in range(3):

            for d in range(2):

                for q in range(3):

                    try:

                        model = ARIMA(y, order=(p, d, q))  # type: ignore[reportPossiblyUnboundVariable]

                        result = model.fit()

                        if result.aic < best_aic:

                            best_aic = result.aic

                            best_model = result

                            best_order = (p, d, q)

                    except Exception:  # noqa: BLE001 - ARIMA grid search

                        continue

        if best_model is None:

            return np.zeros(horizon, dtype=float), None, {'error': 'No ARIMA fit'}

        forecast = best_model.forecast(steps=horizon)

        forecast = np.maximum(forecast, 0)

        return forecast, best_aic, {'order': best_order, 'aic': best_aic}

    except Exception as e:

        return np.zeros(horizon, dtype=float), None, {'error': str(e)}



# Seletor ML (RandomForest) para escolher modelo no modo inteligente

_MODEL_SELECTOR = None



def _train_model_selector() -> Optional['RandomForestClassifier']:

    if not _SKLEARN_AVAILABLE:

        return None

    # Dados simulados para bootstrap do seletor

    features = []

    labels = []

    rng = np.random.default_rng(42)

    for _ in range(1000):

        adi = rng.exponential(2.0)

        cv2 = rng.exponential(0.5)

        trend_strength = rng.uniform(0, 1)

        seasonality = int(rng.choice([0, 1], p=[0.7, 0.3]))

        occurrences = int(rng.poisson(5))

        features.append([adi, cv2, trend_strength, seasonality, occurrences])

        if adi > 1.5 and cv2 > 0.5:

            label = 'croston_modified'

        elif trend_strength > 0.7:

            label = 'arima'

        elif adi < 1.0 and cv2 < 0.3:

            label = 'ses'

        else:

            label = 'croston_safe'

        labels.append(label)

    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42)  # type: ignore[reportPossiblyUnboundVariable]

    clf = RandomForestClassifier(n_estimators=100, random_state=42)  # type: ignore[reportPossiblyUnboundVariable]

    clf.fit(X_train, y_train)

    try:

        acc = accuracy_score(y_test, clf.predict(X_test))  # type: ignore[reportPossiblyUnboundVariable]

        _logger.info("Seletor ML (RF) treinado — acurácia simulada: %.2f", acc)

    except Exception as exc:  # noqa: BLE001 - seletor opcional

        _logger.debug("Falha ao calcular acurácia do seletor: %s", exc)

    return clf



def _get_model_selector():

    global _MODEL_SELECTOR

    if _MODEL_SELECTOR is None:

        _MODEL_SELECTOR = _train_model_selector()

    return _MODEL_SELECTOR



# Renomear função _forecast_proposto para _forecast_inteligente

def _forecast_inteligente(y: np.ndarray, horizon: int, adi: float, cv2: float,

                         trend_strength: float = 0.0, seasonality: int = 0, cfg: Optional[Config] = None):

    """

    Seleciona o modelo de previsão conforme parâmetros e habilitação no YAML.

    """

    modelos_cfg = get_param('ml.modelos_habilitados', {})

    croston_params = get_param('ml.croston_modificado', {})

    ses_enabled = bool(modelos_cfg.get('ses', True))

    croston_safe_enabled = bool(modelos_cfg.get('croston_safe', True))

    croston_mod_enabled = bool(modelos_cfg.get('croston_modificado', True)) and croston_params.get('habilitado', True)

    arima_enabled = bool(modelos_cfg.get('arima', False)) and _ARIMA_AVAILABLE

    alpha_default = croston_params.get('alpha_default', 0.1)

    beta_default = croston_params.get('beta_default', 0.1)

    ses_alphas = cfg.alpha_grid if cfg is not None else (0.05, 0.1, 0.2, 0.3)



    n = len(y)

    positives = int(np.sum(np.asarray(y) > 0))



    def _pick_choice(preferred: List[str]) -> Optional[str]:

        order = preferred + ['croston_modified', 'croston_safe', 'ses', 'arima']

        seen: set[str] = set()

        for cand in order:

            if cand in seen:

                continue

            seen.add(cand)

            if cand == 'croston_modified' and croston_mod_enabled:

                return 'croston_modified'

            if cand == 'croston_safe' and croston_safe_enabled:

                return 'croston_safe'

            if cand == 'ses' and ses_enabled:

                return 'ses'

            if cand == 'arima' and arima_enabled and n >= 18:

                return 'arima'

        return None



    def _run_choice(choice_key: Optional[str]) -> tuple[np.ndarray, str, Dict[str, Any]]:

        if choice_key == 'croston_modified':

            fc, metric, meta = _croston_modified_forecast(

                y, horizon, alpha=alpha_default, beta=beta_default

            )

            return fc, 'Croston_Modificado', meta

        if choice_key == 'croston_safe':

            fc, metric, meta = _croston_forecast_safe(y, horizon, alpha=alpha_default)

            return fc, 'Croston_Safe', meta

        if choice_key == 'ses':

            fc, mase, alpha, meta_ses = _ses_forecast(y, horizon, ses_alphas)

            meta_ses = dict(meta_ses)

            meta_ses['alpha'] = alpha

            meta_ses['mase'] = mase

            return fc, 'SES', meta_ses

        if choice_key == 'arima':

            fc, metric, meta = _arima_forecast(y, horizon)

            return fc, 'ARIMA', meta

        return np.zeros(horizon, dtype=float), 'MODEL_DISABLED', {'error': 'Nenhum modelo habilitado ou disponível'}



    # Séries muito curtas ou com poucas ocorrências usam abordagens conservadoras

    if n < 12 or positives <= 3:

        return _run_choice(_pick_choice(['croston_safe', 'croston_modified', 'ses']))



    if trend_strength > 0.5 and n < 18:

        return _run_choice(_pick_choice(['croston_safe', 'croston_modified', 'ses']))



    occurrences = positives

    clf = _get_model_selector()



    fs_adi = float(adi)

    fs_cv2 = float(cv2)

    fs_trend = float(trend_strength)

    fs_season = float(seasonality)

    fs_occ = float(occurrences)



    feat = np.array([fs_adi, fs_cv2, fs_trend, fs_season, fs_occ], dtype=float)

    finite_mask = np.isfinite(feat)

    if not np.all(finite_mask):

        feat[~finite_mask] = 0.0

    feat[0] = np.clip(feat[0], 0.0, 10.0)

    feat[1] = np.clip(feat[1], 0.0, 10.0)

    feat[2] = np.clip(feat[2], 0.0, 5.0)

    feat[3] = 1.0 if feat[3] >= 0.5 else 0.0

    feat[4] = np.clip(feat[4], 0.0, 120.0)



    choice: Optional[str] = None

    if clf is not None and hasattr(clf, 'predict'):

        try:

            X = np.asarray([feat], dtype=float)

            predicted = str(clf.predict(X)[0])

            choice = _pick_choice([predicted])

        except Exception as exc:  # noqa: BLE001 - seletor opcional

            _logger.debug("Seletor ML falhou na predição: %s", exc)

            clf = None

            choice = None



    if choice is None:

        if adi >= 1.32 and cv2 >= 0.49:

            choice = _pick_choice(['croston_modified', 'croston_safe', 'ses'])

        elif adi >= 1.32:

            choice = _pick_choice(['croston_safe', 'croston_modified', 'ses'])

        elif trend_strength > 0.7:

            choice = _pick_choice(['arima', 'croston_modified', 'ses'])

        else:

            choice = _pick_choice(['ses', 'croston_safe', 'croston_modified'])



    if choice is None:

        choice = _pick_choice([])



    return _run_choice(choice)



def select_and_forecast(mensal: pd.DataFrame, classes: pd.DataFrame, cfg: Config,

                        modo_orcamento: str = 'essencial') -> tuple[pd.DataFrame, pd.DataFrame]:

    """

    Seleciona o modelo adequado e gera previsões mensais,

    adicionando validação de gaps e itens inativos.

    """

    preds_rows: List[Dict[str, Any]] = []

    eval_rows: List[Dict[str, Any]] = []



    if mensal.empty:

        return pd.DataFrame(preds_rows), pd.DataFrame(eval_rows)



    local = mensal.copy()

    if not isinstance(local['ANO_MES'].iloc[0], pd.Period):

        local['ANO_MES'] = pd.PeriodIndex(local['ANO_MES'], freq='M')



    global_last = local['ANO_MES'].max()



    validacao_cfg = get_param('ml.validacao', {}) or {}
    gap_thresh = int(get_param('ml.validacao.gap_max_permitido', validacao_cfg.get('gap_max_permitido', 6)))

    gap_penalty = float(get_param('ml.validacao.gap_penalidade', validacao_cfg.get('gap_penalidade', 0.5)))

    gap_penalty = np.clip(gap_penalty, 0.0, 1.0)

    meses_inativo_cfg = None
    if isinstance(validacao_cfg, dict):
        meses_inativo_cfg = validacao_cfg.get('meses_inativo')
    if meses_inativo_cfg is None:
        meses_inativo_cfg = get_param('modelo_inativo.meses_sem_uso_para_inativo', 24)
    inativo_threshold = int(meses_inativo_cfg)

    review_ratio = float(get_param('ml.validacao.revisao_ratio', 5.0))

    adi_threshold = float(get_param('ml.validacao.limite_adi_croston', 1.32))



    modelos_cfg = get_param('ml.modelos_habilitados', {})

    croston_params = get_param('ml.croston_modificado', {})

    croston_modificado_global = croston_params.get('habilitado', True)

    enable_croston_mod = bool(modelos_cfg.get('croston_modificado', True)) and croston_modificado_global

    enable_croston_safe = bool(modelos_cfg.get('croston_safe', True))

    enable_ses = bool(modelos_cfg.get('ses', True))

    selector_enabled = bool(get_param('ml.seletor_inteligente.habilitado', True))

    use_selector = selector_enabled and modo_orcamento in {'inteligente', 'proposto'}

    alpha_default = croston_params.get('alpha_default', 0.1)

    beta_default = croston_params.get('beta_default', 0.1)



    # Normaliza COD_ITEM para string

    mensal['COD_ITEM'] = mensal['COD_ITEM'].astype(str)

    classes['COD_ITEM'] = classes['COD_ITEM'].astype(str)



    for cod, sub in mensal.groupby('COD_ITEM'):

        cod_python = str(cod)

        rowc = classes.loc[classes['COD_ITEM'] == cod_python]

        cls = rowc['DEMAND_PATTERN'].iat[0] if not rowc.empty and 'DEMAND_PATTERN' in rowc.columns else ''

        adi = float(rowc['ADI'].iat[0]) if not rowc.empty and 'ADI' in rowc.columns and pd.notna(rowc['ADI'].iat[0]) else float('nan')

        cv2 = float(rowc['CV2'].iat[0]) if not rowc.empty and 'CV2' in rowc.columns and pd.notna(rowc['CV2'].iat[0]) else float('nan')



        # Série de consumo

        y = sub['QTD_MENSAL'].to_numpy(dtype=float)

        positive_count = int(np.sum(y > 0))



        # Gap temporal

        periods = sub['ANO_MES'].sort_values().to_list()

        months_since_last = (global_last.year - periods[-1].year) * 12 + (global_last.month - periods[-1].month) if periods else 0

        max_gap = max([(periods[i+1].year - periods[i].year) * 12 + (periods[i+1].month - periods[i].month) for i in range(len(periods)-1)] or [0])

        gap_alert = max_gap > gap_thresh

        item_inativo = (positive_count == 0) or (months_since_last >= inativo_threshold)

        try:

            critical_set = getattr(cfg, 'critical_cod_items', None)

            ignore_flag = bool(getattr(cfg, 'ignore_inactive_for_critical', True))

            if ignore_flag and item_inativo and critical_set and (cod_python in critical_set):

                item_inativo = False

        except AttributeError:  # cfg sem atributos esperados

            pass

        meta: Dict[str, Any] = {

            'max_gap': max_gap,

            'meses_sem_uso': months_since_last,

            'classe_padrao': cls,

            'adi': adi,

            'cv2': cv2

        }

        inactive_label = str(get_param("modelo_inativo.status", "INACTIVE")).strip()
        if item_inativo:
            forecast = np.zeros(cfg.horizonte_previsao_meses, dtype=float)
            model_used = inactive_label
        else:

            croston_condition = (

                cls in {'Intermittent', 'Lumpy'}

                or adi >= adi_threshold

                or np.isinf(adi) or np.isinf(cv2)

                or positive_count <= 3

                or gap_alert

            )

            use_croston = croston_condition and (enable_croston_mod or enable_croston_safe)



            if use_croston:

                if enable_croston_mod:

                    forecast, _, meta_cro = _croston_modified_forecast(

                        y, cfg.horizonte_previsao_meses,

                        alpha=alpha_default,

                        beta=beta_default

                    )

                    model_used = 'Croston_Modificado'

                    meta.update({f'croston_{k}': v for k, v in meta_cro.items()})

                elif enable_croston_safe:

                    forecast, _, meta_cro = _croston_forecast_safe(

                        y, cfg.horizonte_previsao_meses, alpha=alpha_default

                    )

                    model_used = 'Croston_Safe'

                    meta.update({f'croston_{k}': v for k, v in meta_cro.items()})

                elif enable_ses:

                    forecast, mase_ses, alpha_ses, meta_ses = _ses_forecast(

                        y, cfg.horizonte_previsao_meses, cfg.alpha_grid

                    )

                    model_used = 'SES'

                    meta.update({'alpha': alpha_ses})

                    meta.update({f'ses_{k}': v for k, v in meta_ses.items()})

                    meta['mase_ses'] = mase_ses

                else:

                    forecast = np.zeros(cfg.horizonte_previsao_meses, dtype=float)

                    model_used = 'DESATIVADO'

                    meta['model_warning'] = 'Nenhum modelo habilitado'

            else:

                if enable_ses:

                    forecast, mase_ses, alpha_ses, meta_ses = _ses_forecast(

                        y, cfg.horizonte_previsao_meses, cfg.alpha_grid

                    )

                    model_used = 'SES'

                    meta.update({'alpha': alpha_ses})

                    meta.update({f'ses_{k}': v for k, v in meta_ses.items()})

                    meta['mase_ses'] = mase_ses

                elif enable_croston_mod:

                    forecast, _, meta_cro = _croston_modified_forecast(

                        y, cfg.horizonte_previsao_meses,

                        alpha=alpha_default,

                        beta=beta_default

                    )

                    model_used = 'Croston_Modificado'

                    meta.update({f'croston_{k}': v for k, v in meta_cro.items()})

                elif enable_croston_safe:

                    forecast, _, meta_cro = _croston_forecast_safe(

                        y, cfg.horizonte_previsao_meses, alpha=alpha_default

                    )

                    model_used = 'Croston_Safe'

                    meta.update({f'croston_{k}': v for k, v in meta_cro.items()})

                else:

                    forecast = np.zeros(cfg.horizonte_previsao_meses, dtype=float)

                    model_used = 'DESATIVADO'

                    meta['model_warning'] = 'Nenhum modelo habilitado'



            if use_selector:

                trend_strength = float(abs(np.polyfit(range(len(y)), y, 1)[0])) if len(y) > 3 else 0.0

                seasonality = 1 if len(y) >= 12 and (np.std(y) > (np.mean(y) * 0.5 if np.mean(y) > 0 else 0)) else 0

                forecast, model_used, meta_int = _forecast_inteligente(

                    y, cfg.horizonte_previsao_meses, adi, cv2, trend_strength, seasonality, cfg

                )

                meta.update({f'int_{k}': v for k, v in meta_int.items()})



            forecast = np.asarray(forecast, dtype=float)

            if gap_alert:

                forecast = forecast * gap_penalty

                meta['gap_penalty'] = gap_penalty



        pred_rate = float(np.mean(forecast)) if forecast.size else 0.0



        review_alert = False

        if not item_inativo:

            review_alert = not _validar_forecast_vs_historico(

                forecast, y, int(cod_python) if isinstance(cod_python, int) else cod_python, ratio_threshold=review_ratio

            )



        meta['review_alert'] = review_alert

        meta['item_inativo'] = item_inativo

        meta['gap_alert'] = gap_alert



        tail = min(len(y), cfg.horizonte_previsao_meses)

        if not item_inativo and tail > 1 and len(forecast) >= tail:

            mase_value = _mase(y[-tail:], forecast[:tail], m=1)

            rmse = _rmse(y[-tail:], forecast[:tail])

            mae = _mae(y[-tail:], forecast[:tail])

        else:

            mase_value = np.nan

            rmse = np.nan

            mae = np.nan



        preds_rows.append({

            'COD_ITEM': cod_python,

            'MODEL_USED': model_used,

            'PREDICTED_CONSUMPTION_RATE': pred_rate,

            'ITEM_INATIVO': item_inativo,

            'GAP_ALERT': gap_alert,

            'REVIEW_ALERT': review_alert,

            'MESES_SEM_USO': months_since_last,

            'FORECAST_SERIES': json.dumps([float(x) for x in forecast]),

            'ADI': adi,

            'CV2': cv2,

            'CLASSE_USO': cls,

            **{f'META_{k.upper()}': v for k, v in meta.items()}

        })



        eval_rows.append({

            'COD_ITEM': cod_python,

            'MASE': mase_value,

            'RMSE': rmse,

            'MAE': mae,

            'BIAS': np.nan,

            'ITEM_INATIVO': item_inativo,

            'GAP_ALERT': gap_alert,

            'REVIEW_ALERT': review_alert,

            'ADI': adi,

            'CV2': cv2,

            'CLASSE_USO': cls

        })



    return pd.DataFrame(preds_rows), pd.DataFrame(eval_rows)





def _validar_forecast_vs_historico(forecast: np.ndarray, y: np.ndarray, cod_item: Union[int, str],

                                   ratio_threshold: float = 5.0) -> bool:

    """

    Valida se a previsão é proporcional ao histórico.

    """

    y = np.asarray(y, dtype=float)

    forecast = np.asarray(forecast, dtype=float)



    historico_pos = y[y > 0]

    media_historica = float(historico_pos.mean()) if historico_pos.size else 0.0

    media_forecast = float(forecast.mean()) if forecast.size else 0.0



    if media_historica <= 0:

        return True



    ratio = media_forecast / media_historica if media_historica else np.inf

    if ratio > ratio_threshold:

        _logger.warning("[ALERTA] COD_ITEM %s: forecast %.2f é %.1f× maior que histórico %.2f", cod_item, media_forecast, ratio, media_historica)

        return False



    return True





# ===== 8) Score e flags =====

def evaluate_and_score(indic: pd.DataFrame, eval_df: pd.DataFrame, cfg: Config) -> pd.DataFrame:

    out = eval_df.copy()

    if 'COD_ITEM' in out.columns:

        out['COD_ITEM'] = _normalize_cod_item(out['COD_ITEM'])

    quality_enabled = bool(get_param('ml.quality_score.habilitado', True))

    if quality_enabled:

        out['QUALITY_SCORE'] = out['MASE'].apply(_quality_score)

    else:

        out['QUALITY_SCORE'] = np.nan

    # Flags

    base = indic[['COD_ITEM', 'MESES_ANALISADOS_DESDE_1A_OCORRENCIA', 'MESES_COM_CONSUMO']].copy()

    if 'COD_ITEM' in base.columns:

        base['COD_ITEM'] = _normalize_cod_item(base['COD_ITEM'])

    out = out.merge(base, on='COD_ITEM', how='left')

    out['BAIXA_CONFIABILIDADE'] = (out['MESES_ANALISADOS_DESDE_1A_OCORRENCIA'].fillna(0) < 12) | (out['MESES_COM_CONSUMO'].fillna(0) <= 2)

    out['DRIFT_POSSIVEL'] = False  # placeholder

    return out





# ===== 9) Export =====

def export_outputs(base_dir: Path, mensal: pd.DataFrame, indicadores: pd.DataFrame, classes: pd.DataFrame,

                   preds: pd.DataFrame, eval_df: pd.DataFrame, catalogo_desc: pd.DataFrame,

                   excel_name: str = 'ML_CONSUMO_SOLIDOS.xlsx'):

    base_dir.mkdir(parents=True, exist_ok=True)

    for df in (mensal, indicadores, classes, preds, eval_df, catalogo_desc):

        if isinstance(df, pd.DataFrame) and 'COD_ITEM' in df.columns:

            df['COD_ITEM'] = _normalize_cod_item(df['COD_ITEM'])

    # Merge de descrições e UM (se disponível)

    map_desc = catalogo_desc[['COD_ITEM', 'DESC_ITEM']].drop_duplicates('COD_ITEM')

    indicadores = indicadores.merge(map_desc, on='COD_ITEM', how='left')

    classes = classes.merge(map_desc, on='COD_ITEM', how='left')

    preds = preds.merge(map_desc, on='COD_ITEM', how='left')

    eval_df = eval_df.merge(map_desc, on='COD_ITEM', how='left')



    if 'UM' in catalogo_desc.columns:

        map_um = catalogo_desc[['COD_ITEM', 'UM']].drop_duplicates('COD_ITEM')

        indicadores = indicadores.merge(map_um, on='COD_ITEM', how='left')

        classes = classes.merge(map_um, on='COD_ITEM', how='left')

        preds = preds.merge(map_um, on='COD_ITEM', how='left')

        eval_df = eval_df.merge(map_um, on='COD_ITEM', how='left')



    # Excel

    xlsx_path = base_dir / excel_name

    with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:

        mensal.assign(ANO_MES=mensal['ANO_MES'].astype(str)).to_excel(writer, sheet_name='MENSAL_SERIES', index=False)

        indicadores.to_excel(writer, sheet_name='INDICADORES', index=False)

        classes.to_excel(writer, sheet_name='CLASSIFICACAO', index=False)

        preds.to_excel(writer, sheet_name='PREVISOES', index=False)

        eval_df.to_excel(writer, sheet_name='AVALIACAO', index=False)



    # CSV/JSON principais

    preds.to_csv(base_dir / 'predicoes.csv', index=False)

    eval_df.to_csv(base_dir / 'avaliacao.csv', index=False)

    # Relatório gerencial simples

    report = {

        'total_items_processed': int(len(classes)),

        'distribution_by_pattern': classes['DEMAND_PATTERN'].value_counts(dropna=False).to_dict(),

        'quality_summary': {

            'mean_quality': float(eval_df['QUALITY_SCORE'].mean(skipna=True)) if 'QUALITY_SCORE' in eval_df else None

        }

    }

    (base_dir / 'relatorio.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    return xlsx_path





def _append_ml_tabs_to_excel(target_excel: Path, predicoes_csv: Path, avaliacao_csv: Path) -> None:

    """Anexa/atualiza as abas PREDICOES_ML e AVALIACAO_ML em um Excel existente."""

    predicoes_csv = Path(predicoes_csv)

    avaliacao_csv = Path(avaliacao_csv)

    target_excel = Path(target_excel)

    if not target_excel.exists():

        return

    pred_df = pd.read_csv(predicoes_csv) if predicoes_csv.exists() else None

    eval_df = pd.read_csv(avaliacao_csv) if avaliacao_csv.exists() else None

    if pred_df is None and eval_df is None:

        return

    try:

        with pd.ExcelWriter(target_excel, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:

            if pred_df is not None:

                pred_df.to_excel(writer, sheet_name="PREDICOES_ML", index=False)

            if eval_df is not None:

                eval_df.to_excel(writer, sheet_name="AVALIACAO_ML", index=False)

    except TypeError:

        # Fallback para pandas antigos

        existing = {}

        with pd.ExcelFile(target_excel) as xls:

            for sheet in xls.sheet_names:

                existing[sheet] = pd.read_excel(xls, sheet)

        with pd.ExcelWriter(target_excel, engine="openpyxl") as writer:

            for name, df in existing.items():

                df.to_excel(writer, sheet_name=name, index=False)

            if pred_df is not None:

                pred_df.to_excel(writer, sheet_name="PREDICOES_ML", index=False)

            if eval_df is not None:

                eval_df.to_excel(writer, sheet_name="AVALIACAO_ML", index=False)



# ===== Orquestração =====

def run_pipeline(input_path: str | Path, output_dir: str | Path, cfg: Optional[Config] = None, 

                modo_orcamento: str = 'essencial') -> Path:

    cfg = cfg or Config()

    input_path = Path(input_path)

    output_dir = Path(output_dir)



    _logger.info("=== PIPELINE ML - PREVISÃO DE DEMANDA ===")

    _logger.info("Entrada: %s", input_path)

    _logger.info("Saída: %s", output_dir)



    # 1) Carrega dados (já filtrados ou filtrando agora)

    raw = load_data(input_path)



    # 2) Limpeza adicional

    cleaned, issues = validate_and_clean(raw)

    if len(issues) > 0:

        _logger.warning("Issues de validação: %s", issues.to_dict('records'))



    # 3) Série mensal

    mensal = build_monthly_series(cleaned)

    _logger.info("Série mensal: %d linhas; %d itens.", len(mensal), mensal['COD_ITEM'].nunique())



    # 4) Indicadores e classificação

    indicadores = compute_indicators(mensal, cleaned)

    adi_cv2 = compute_adi_cv2(mensal)

    classes = classify_demand_patterns(adi_cv2)

    # Conjunto de itens críticos por descrição (ignora INACTIVE nas previsões)

    terms = get_param("orcamento.analise_critica.descricoes_palavras", [])

    try:

        terms = [str(t).strip().upper() for t in terms if str(t).strip()]

    except (TypeError, AttributeError):  # terms inválido

        terms = []

    if terms:

        cat = cleaned[["COD_ITEM","DESC_ITEM"]].dropna().copy()

        cat["COD_ITEM"] = cat["COD_ITEM"].astype(str)

        cat["DESC_UP"] = cat["DESC_ITEM"].astype(str).str.upper()

        mask = False

        for t in terms:

            cond = cat["DESC_UP"].str.contains(t, na=False, regex=False)

            mask = cond if isinstance(mask, bool) else (mask | cond)

        critical_set = set(cat.loc[mask, "COD_ITEM"]) if not isinstance(mask, bool) else set()

    else:

        critical_set = set()

    cfg.critical_cod_items = critical_set

    cfg.ignore_inactive_for_critical = bool(get_param("orcamento.analise_critica.ignorar_model_inactive", True))

    # 5) Previsões e avaliação

    preds, eval_df = select_and_forecast(mensal, classes, cfg, modo_orcamento)

    eval_scored = evaluate_and_score(indicadores, eval_df, cfg)



    # 6) Export (gera Excel + CSVs predicoes.csv e avaliacao.csv)

    xlsx_path = export_outputs(output_dir, mensal, indicadores, classes, preds, eval_scored, cleaned)



    # 7) Anexa abas PREDICOES_ML/AVALIACAO_ML também neste Excel (compatibilidade com outras rotinas)

    _append_ml_tabs_to_excel(

        target_excel=xlsx_path,

        predicoes_csv=output_dir / 'predicoes.csv',

        avaliacao_csv=output_dir / 'avaliacao.csv'

    )



    _logger.info("[OK] Pipeline concluído. Relatório: %s", xlsx_path)

    return xlsx_path





# ===== CLI =====

def build_argparser():

    p = argparse.ArgumentParser(description='Pipeline ML — Taxa de Consumo (Sólidos)')

    p.add_argument('comando', choices=['ml', 'orcamento'], nargs='?', default='ml',

                   help='comando a executar: ml (apenas previsão) ou orcamento (previsão + orçamento)')

    

    # Defaults agora podem vir do params.yaml (fallback mantém os atuais)

    default_input = get_param('sistema.paths.historico', str(Path(__file__).resolve().parents[1] / 'data/BASE_HISTORICA.xlsx'))

    default_output = get_param('sistema.paths.out_orcamento', str(Path(__file__).resolve().parent / 'orcamento_2026'))

    default_horizon = int(get_param('ml.horizon_meses_default', 3))



    p.add_argument('-i', '--input', default=default_input,

                   help='Arquivo de entrada (BASE_HISTORICA.xlsx será processado automaticamente)')

    p.add_argument('-o', '--output', default=default_output,

                   help='Diretório de saída')

    p.add_argument('--horizon', type=int, default=default_horizon, help='Horizonte de previsão em meses')

    p.add_argument('--margem', type=float, default=0.15, help='Margem base para orçamento (pct)')

    return p



def main():

    args = build_argparser().parse_args()

    cfg = Config(horizonte_previsao_meses=args.horizon)

    out = run_pipeline(args.input, args.output, cfg)

    _logger.info("Pipeline ML concluído. Relatórios: %s", out)



if __name__ == '__main__':

    main()


















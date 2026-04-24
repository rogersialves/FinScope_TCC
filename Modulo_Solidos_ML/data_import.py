import sys
from pathlib import Path
import pandas as pd
import numpy as np

# habilita imports da raiz (fin_utils)
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.fin_params import get_param
from core.logger import get_logger
from fin_utils import (
    filter_por_fonte,
    filter_por_conta_contabil,
    aplicar_filtros_basicos,
    find_centro_custo_column,
    remover_por_grupo_budget,
)

_logger = get_logger(__name__)

def _norm(s: str) -> str:
    return str(s).strip().upper().replace(' ', '_')

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    norm = {c: _norm(c) for c in df.columns}
    for cand in candidates:
        nc = _norm(cand)
        for c, n in norm.items():
            if n == nc:
                return c
    return None

def _find_value_col(df: pd.DataFrame) -> str | None:
    """Encontra de forma segura a coluna de VALOR TOTAL do item.
    Regras:
    - Preferir colunas cujo nome contenha 'VALOR' ou comece com 'VL'
    - EXCLUIR colunas que contenham 'CUSTO', 'CENTRO' ou variações (para não confundir com CENTRO_CUSTO/CENTRO_CUSTOS)
    - Entre candidatas, escolhe a com maior soma numérica absoluta (proxy de coluna monetária principal)
    - Fallback para nomes comuns: VALOR_TOTAL, VL_TOTAL, VALOR
    """
    norm = {c: _norm(c) for c in df.columns}
    def is_value_like(name_norm: str) -> bool:
        if ('VALOR' in name_norm) or name_norm.startswith('VL'):
            if ('CUSTO' in name_norm) or ('CENTRO' in name_norm) or ('CENTROCUSTO' in name_norm):
                return False
            return True
        return False

    candidates = [c for c, n in norm.items() if is_value_like(n)]
    if candidates:
        best, best_sum = None, -1
        for c in candidates:
            s = pd.to_numeric(df[c], errors='coerce')
            total = s.abs().sum(skipna=True)
            if total > best_sum:
                best, best_sum = c, total
        if best is not None:
            return best

    # Fallback conservador
    for cand in ['VALOR_TOTAL', 'VL_TOTAL', 'VALOR']:
        nc = _norm(cand)
        for c, n in norm.items():
            if n == nc:
                return c
    return None

def load_and_preprocess(input_path: str | Path, aux_path: Path | None = None):
    """
    Carrega e pré-processa dados do histórico.
    aux_path é opcional e reservado para uso futuro (ex.: carregar TAB_AUX para enriquecimento).
    """
    xlsx = Path(input_path)
    xl = pd.ExcelFile(xlsx)
    sheet = xl.sheet_names[0]  # 1ª planilha (especificação)
    df_raw = pd.read_excel(xlsx, sheet_name=sheet)

    # localizar colunas-base
    col_cod = _find_col(df_raw, ['COD_ITEM'])
    col_desc = _find_col(df_raw, ['DESC_ITEM'])
    col_qtd = _find_col(df_raw, ['QUANTIDADE', 'QTD', 'QTDE'])
    # Coluna de VALOR deve ser o total monetário do item (não confundir com CENTRO_CUSTO/CENTRO_CUSTOS ou 'CUSTO')
    col_val = _find_value_col(df_raw)
    col_data = _find_col(df_raw, ['DATA_ENTREGA', 'DATA_EMISSAO', 'EMISSAO', 'DATA'])
    col_cc = find_centro_custo_column(df_raw.columns)
    col_conta = _find_col(df_raw, ['CONTA_CONTABIL', 'CONTACONTABIL', 'CONTACONTA', 'CONTA'])

    for name, col in [('COD_ITEM', col_cod), ('QUANTIDADE', col_qtd), ('VALOR (TOTAL)', col_val), ('DATA_ENTREGA', col_data)]:
        if not col:
            raise KeyError(f"Coluna obrigatória ausente para {name}.")

    # padronizar
    rename_map = {col_cod: 'COD_ITEM', col_qtd: 'QUANTIDADE', col_val: 'VALOR', col_data: 'DATA_ENTREGA'}
    if col_desc: rename_map[col_desc] = 'DESC_ITEM'
    if col_cc: rename_map[col_cc] = 'CENTRO_CUSTO'
    if col_conta: rename_map[col_conta] = 'CONTA_CONTABIL'
    df = df_raw.rename(columns=rename_map)

    # validações
    df['DATA_ENTREGA'] = pd.to_datetime(df['DATA_ENTREGA'], errors='coerce')
    df = df.dropna(subset=['DATA_ENTREGA'])
    df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0)
    df['VALOR'] = pd.to_numeric(df['VALOR'], errors='coerce').fillna(0)
    df = df[(df['QUANTIDADE'] >= 0) & (df['VALOR'] >= 0)]

    df, _ = filter_por_fonte(df)
    df, _ = filter_por_conta_contabil(df)
    df, _ = remover_por_grupo_budget(df, col_override='GRUPO_BUDGET')

    antes_basicos = len(df)
    df = aplicar_filtros_basicos(
        df,
        col_cc='CENTRO_CUSTO',
        col_desc='DESC_ITEM',
        col_data='DATA_ENTREGA',
        col_grupo_budget='GRUPO_BUDGET',
    )
    if len(df) != antes_basicos:
        _logger.debug(f"Registros após filtros básicos (CC/ano/parada): {len(df)}/{antes_basicos}")

    # PRECO_UNITARIO = VALOR / QUANTIDADE (apenas quando QUANTIDADE > 0)
    df = df[df['QUANTIDADE'] > 0].copy()
    df['PRECO_UNITARIO'] = df['VALOR'] / df['QUANTIDADE']

    # colunas finais
    cols_out = ['COD_ITEM', 'DESC_ITEM', 'QUANTIDADE', 'VALOR', 'DATA_ENTREGA', 'PRECO_UNITARIO']
    if 'UM' in df.columns:
        cols_out.insert(2, 'UM')  # posiciona próximo a DESC_ITEM
    for extra in ['CENTRO_CUSTO', 'CONTA_CONTABIL', 'GRUPO_BUDGET', 'NUM_TIPO_DESPESA', 'DESCRICAO_DESPESA', 'TAG_DETECTADA_OBS', 'TAG_MOTIVO']:
        if extra in df.columns:
            cols_out.append(extra)
    return df[cols_out].copy()

def monthly_qtd_val(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna séries mensais de QTD e VALOR com grade OTIMIZADA:
    - Só expande até o último mês com dados do próprio item (não global)
    - Remove expansão desnecessária que causava OOM
    """
    d = df.copy()
    d['ANO_MES'] = pd.to_datetime(d['DATA_ENTREGA'], errors='coerce').dt.to_period('M')
    d = d.dropna(subset=['ANO_MES'])

    # agregação mensal
    agg = (d.groupby(['COD_ITEM', 'ANO_MES'])
             .agg(QUANTIDADE=('QUANTIDADE','sum'),
                  VALOR=('VALOR','sum'))
             .reset_index())

    if agg.empty:
        return (pd.DataFrame(columns=['COD_ITEM','ANO_MES','QTD_MENSAL']),
                pd.DataFrame(columns=['COD_ITEM','ANO_MES','VALOR_MENSAL']))

    # OTIMIZAÇÃO: expansão inteligente por item (não até último global)
    def _expand_smart(item_df: pd.DataFrame) -> pd.DataFrame:
        start = item_df['ANO_MES'].min()
        end = item_df['ANO_MES'].max()  # último mês COM DADOS do item
        idx = pd.period_range(start, end, freq='M')
        item_df = (item_df.set_index('ANO_MES')
                           .reindex(idx, fill_value=0)
                           .rename_axis('ANO_MES')
                           .reset_index())
        return item_df

    # CORREÇÃO: Remove include_groups (compatibilidade com pandas antigo)
    exp_list = []
    for cod_item, group in agg.groupby('COD_ITEM'):
        expanded = _expand_smart(group)
        expanded['COD_ITEM'] = cod_item  # Garante que COD_ITEM está presente
        exp_list.append(expanded)
    
    if exp_list:
        exp = pd.concat(exp_list, ignore_index=True)
    else:
        exp = pd.DataFrame(columns=['COD_ITEM', 'ANO_MES', 'QUANTIDADE', 'VALOR'])

    qtd = exp[['COD_ITEM','ANO_MES','QUANTIDADE']].rename(columns={'QUANTIDADE':'QTD_MENSAL'})
    val = exp[['COD_ITEM','ANO_MES','VALOR']].rename(columns={'VALOR':'VALOR_MENSAL'})
    return qtd, val

def monthly_price(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['ANO_MES'] = pd.to_datetime(df['DATA_ENTREGA']).dt.to_period('M')  # type: ignore[union-attr]
    # preço médio ponderado no mês = soma(VALOR)/soma(QUANTIDADE)
    agg = df.groupby(['COD_ITEM', 'ANO_MES']).agg(VALOR=('VALOR','sum'), QTD=('QUANTIDADE','sum')).reset_index()
    agg['PRECO_MEDIO_MENSAL'] = np.where(agg['QTD']>0, agg['VALOR']/agg['QTD'], np.nan)
    return agg[['COD_ITEM','ANO_MES','PRECO_MEDIO_MENSAL']]

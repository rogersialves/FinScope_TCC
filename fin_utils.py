"""
Utilitários gerais do FinScope.

Este módulo contém funções utilitárias compartilhadas por diversos módulos
do projeto, incluindo:

* Extração e validação de TAGs de equipamentos
* Parser de argumentos CLI padrão
* Mapeamentos de classificação de consumo
* Carregamento de catálogos auxiliares (TABS_AUX)

Example:
    Parser CLI padrão::
    
        from fin_utils import cli_parser_defaults
        parser = cli_parser_defaults("input.xlsx", "output.xlsx")
        args = parser.parse_args()
        
    Aplicar TAGs via catálogo::
    
        from fin_utils import aplicar_tags_dataframe
        df, pendencias = aplicar_tags_dataframe(df, aux_path)
"""

from core.fin_params import get_param
import pandas as pd
import numpy as _np
import re
from pathlib import Path

# Mapeamento de tipos de padrão para ordenação em relatórios
ORDER_MAP = {
    'Rotineiro': '1-Rotineiro',
    'Intermitente': '2-Intermitente',
    'Ocasional': '3-Ocasional',
    'Raro': '4-Raro',
    'Muito Raro': '5-Muito Raro',
}


def cli_parser_defaults(default_input: str, default_output: str):
    """
    Cria um parser de argumentos CLI com opções padrão do FinScope.
    
    Args:
        default_input: Caminho padrão do arquivo de entrada.
        default_output: Caminho padrão do arquivo de saída.
        
    Returns:
        argparse.ArgumentParser configurado com opções --input, --output,
        --consolidado-out e --year.
        
    Example:
        >>> parser = cli_parser_defaults("data/input.xlsx", "output/result.xlsx")
        >>> args = parser.parse_args(["--year", "2025"])
        >>> args.year
        2025
    """
    import argparse
    p = argparse.ArgumentParser(description="FinScope module runner")
    p.add_argument('--input', '-i', default=default_input, help='Arquivo de entrada (xlsx)')
    p.add_argument('--output', '-o', default=default_output, help='Arquivo agregado de saída (xlsx)')
    p.add_argument('--consolidado-out', '-c', default=None, help='Arquivo consolidado de saída (opcional)')
    p.add_argument('--year', '-y', type=int, default=2026, help='Ano referência para projeções')
    return p


# ===== Extração de TAGs baseada na TABS_AUX (catálogo + fallback por CC_GERAL) =====

# Padrão regex para TAGs de equipamentos: 31SC01, 31TD01A, 01GAS01, etc.
_TAG_REGEX = re.compile(r'\b(\d{2}[A-Z]{2,3}\d{2}[A-Z0-9]?)\b')


def _norm_cols(name: str) -> str:
    """Remove caracteres não alfanuméricos e converte para uppercase."""
    return re.sub(r'[^A-Z0-9]', '', str(name).upper())


def _to_int_like(val):
    """Extrai parte inteira de um valor para comparação de centros de custo."""
    try:
        s = str(val)
        dig = re.search(r'(\d+)', s)
        return int(dig.group(1)) if dig else None
    except Exception:
        return None


def load_tabs_aux_catalog(aux_path: Path) -> dict:
    """
    Carrega catálogo TABS_AUX para resolução de TAGs.
    
    Args:
        aux_path: Caminho para o arquivo TAB_AUX.xlsx.
        
    Returns:
        Dicionário contendo:
        - tags_catalog: conjunto de TAGs válidas (TAG + TAG_GERAL)
        - cc_to_tag_geral: mapa Centro de Custo -> TAG_GERAL
        - prefixos_fabrica: prefixos por fábrica para desempate
        
    Raises:
        FileNotFoundError: Se aux_path não existir.
        
    Example:
        >>> catalog = load_tabs_aux_catalog(Path("data/TAB_AUX.xlsx"))
        >>> "31SC01" in catalog["tags_catalog"]
        True
    """
    df = pd.read_excel(aux_path, sheet_name='TABS_AUX')
    # Normalização de colunas (aceita variações)
    colmap = {c: _norm_cols(c) for c in df.columns}
    inv = {v: k for k, v in colmap.items()}
    # Colunas esperadas (variações comuns)
    col_tag = inv.get('TAG') or inv.get('TAGS')
    col_tag_geral = inv.get('TAGGERAL') or inv.get('TAG_GERAL')
    col_cc_geral = inv.get('CCGERAL') or inv.get('CC_GERAL') or inv.get('CENTRODECUSTOS') or inv.get('CENTRODECUSTO')
    # Catálogo de TAGs
    tags = set()
    if col_tag and col_tag in df.columns:
        tags |= set(str(x).strip().upper() for x in df[col_tag].dropna().astype(str))
    if col_tag_geral and col_tag_geral in df.columns:
        tags |= set(str(x).strip().upper() for x in df[col_tag_geral].dropna().astype(str))
    # Mapa CC_GERAL -> TAG_GERAL
    cc_to_tag_geral = {}
    if col_cc_geral and col_tag_geral and col_cc_geral in df.columns and col_tag_geral in df.columns:
        for _, r in df[[col_cc_geral, col_tag_geral]].dropna().iterrows():
            cc = _to_int_like(r[col_cc_geral])
            tg = str(r[col_tag_geral]).strip().upper()
            if cc:
                cc_to_tag_geral[cc] = tg
    # Prefixos de fábrica (regras do usuário)
    prefixos_fabrica = {
        'NUTRICAO': ['39', '48'],
        'LIQUIDOS': ['34', '35'],
        'ETAR': ['51', '53', '55'],
        'EXTRATO': ['38'],
        'SOLIDOS': []  # múltiplos; não prioriza prefixo
    }
    return {
        'tags_catalog': tags,
        'cc_to_tag_geral': cc_to_tag_geral,
        'prefixos_fabrica': prefixos_fabrica
    }


def inferir_fabrica_por_cc(cc_num: int | None) -> str | None:
    """
    Infere a fábrica a partir do Centro de Custo.
    
    Args:
        cc_num: Número do centro de custo.
        
    Returns:
        Nome da fábrica ('SOLIDOS', 'NUTRICAO', 'LIQUIDOS', 'EXTRATO') ou None.
        
    Example:
        >>> inferir_fabrica_por_cc(6500)
        'SOLIDOS'
        >>> inferir_fabrica_por_cc(7160)
        'NUTRICAO'
    """
    if cc_num is None:
        return None
    # Faixa SOLIDOS parametrizável (default 6000–6675)
    cc_rng = get_param('filtros.centro_custo_range', [6000, 6675])
    try:
        lo, hi = int(cc_rng[0]), int(cc_rng[1])
    except Exception:
        lo, hi = 6000, 6675
    if cc_num == 7160:
        return 'NUTRICAO'
    if cc_num == 7250:
        return 'LIQUIDOS'
    if cc_num == 8205:
        return 'EXTRATO'
    if lo <= cc_num <= hi:
        return 'SOLIDOS'
    return None


def extrair_tags_da_obs(texto_observacao: str | None) -> list[str]:
    """
    Extrai códigos de TAG de equipamentos do texto de observação.
    
    Args:
        texto_observacao: Texto da observação (campo OBSERVACAO).
        
    Returns:
        Lista de TAGs encontradas no padrão XX[YYY]##[Z] (ex: 31SC01, 01GAS01A).
        
    Example:
        >>> extrair_tags_da_obs("Material para TAG 31SC01 e 31TD02")
        ['31SC01', '31TD02']
    """
    if not isinstance(texto_observacao, str):
        return []
    txt = texto_observacao.upper()
    return _TAG_REGEX.findall(txt)


def resolver_tag(tag_atual: str | None, obs: str | None, cc_num: int | None, catalogo: dict) -> tuple[str | None, str]:
    """
    Resolve a TAG para um registro considerando múltiplas fontes.
    
    Ordem de prioridade:
    1. TAG já preenchida e válida (mantém original)
    2. TAG detectada na OBS e presente no catálogo TABS_AUX
    3. TAG detectada na OBS por prioridade de prefixo da fábrica
    4. Fallback por CC_GERAL -> TAG_GERAL
    
    Args:
        tag_atual: TAG atual do registro (pode ser None ou vazia).
        obs: Texto do campo OBSERVACAO.
        cc_num: Número do Centro de Custo.
        catalogo: Catálogo carregado via load_tabs_aux_catalog().
        
    Returns:
        Tupla (tag_resolvida, motivo) onde motivo indica a fonte.
        
    Example:
        >>> resolver_tag(None, "Material 31SC01", 6500, catalogo)
        ('31SC01', 'OBS_CATALOGO')
    """
    tags_catalog = catalogo.get('tags_catalog', set())
    cc_to_tag_geral = catalogo.get('cc_to_tag_geral', {})
    pref = catalogo.get('prefixos_fabrica', {})
    fabrica = inferir_fabrica_por_cc(cc_num)
    # 1) TAG atual válida
    if isinstance(tag_atual, str) and tag_atual.strip():
        t = tag_atual.strip().upper()
        if t in tags_catalog or _TAG_REGEX.fullmatch(t):
            return t, 'TAG_ORIGINAL_MANTIDA'
    # 2) Candidatas na observação validadas pelo catálogo
    cand = [t.strip().upper() for t in extrair_tags_da_obs(obs if isinstance(obs, str) else None)]
    validas = [t for t in cand if t in tags_catalog]
    if validas:
        # Se houver fábrica, prioriza prefixos da fábrica
        if fabrica and pref.get(fabrica):
            for pre in pref[fabrica]:
                for t in validas:
                    if t.startswith(pre):
                        return t, 'OBS_CATALOGO_PREF_FABRICA'
        return validas[0], 'OBS_CATALOGO'
    # 3) Candidatas por padrão e prioridade de prefixo (mesmo se não estiverem no catálogo ainda)
    if cand and fabrica and pref.get(fabrica):
        for pre in pref[fabrica]:
            for t in cand:
                if t.startswith(pre):
                    return t, 'OBS_PADRAO_PREF_FABRICA'
    # 4) Fallback por CC_GERAL -> TAG_GERAL
    if cc_num in cc_to_tag_geral:
        return cc_to_tag_geral[cc_num], 'FALLBACK_CC_GERAL'
    # Sem resolução
    return None, 'PENDENTE_REVISAO'


def aplicar_tags_dataframe(df: pd.DataFrame, aux_path: Path, col_tag='TAG', col_obs='OBSERVACAO', col_cc='CENTRO_CUSTO') -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica resolução de TAGs em todo o DataFrame.
    
    Processa cada registro e tenta resolver a TAG usando:
    - TAG existente (se válida)
    - OBSERVACAO + catálogo TABS_AUX
    - Fallback por CC_GERAL -> TAG_GERAL
    
    Args:
        df: DataFrame com os dados a processar.
        aux_path: Caminho para TAB_AUX.xlsx.
        col_tag: Nome da coluna de TAG.
        col_obs: Nome da coluna de OBSERVACAO.
        col_cc: Nome da coluna de Centro de Custo.
        
    Returns:
        Tupla (df_atualizado, df_pendencias):
        - df_atualizado: DataFrame com colunas TAG, TAG_DETECTADA_OBS e TAG_MOTIVO
        - df_pendencias: Registros que precisam revisão manual
        
    Example:
        >>> df, pendencias = aplicar_tags_dataframe(df, Path("TAB_AUX.xlsx"))
        >>> print(f"Pendências: {len(pendencias)}")
    """
    catalogo = load_tabs_aux_catalog(aux_path)
    # Normaliza CC numérico
    cc_series = df[col_cc] if col_cc in df.columns else pd.Series([None] * len(df))
    cc_num = cc_series.astype(str).str.extract(r'(\d+)', expand=False).astype('float').astype('Int64')
    out_tag = []
    motivo = []
    tag_obs = []
    # Itera por posição para garantir alinhamento com cc_num
    for pos, (_, row) in enumerate(df.iterrows()):
        tag_atual = row[col_tag] if col_tag in df.columns else None
        obs = row[col_obs] if col_obs in df.columns else None
        ccn_val = cc_num.iloc[pos] if pos < len(cc_num) else None
        ccn = int(ccn_val) if pd.notna(ccn_val) else None
        cand_obs = extrair_tags_da_obs(obs if isinstance(obs, str) else None)
        tag_res, why = resolver_tag(tag_atual, obs, ccn, catalogo)
        out_tag.append(tag_res)
        motivo.append(why)
        tag_obs.append(cand_obs[0] if cand_obs else None)
    # Atribui TAG e colunas auxiliares
    if col_tag not in df.columns:
        df[col_tag] = None
    df[col_tag] = out_tag
    df['TAG_DETECTADA_OBS'] = tag_obs
    df['TAG_MOTIVO'] = motivo
    # Pendências
    df_pend = df.loc[df['TAG_MOTIVO'] == 'PENDENTE_REVISAO', [col_cc, col_obs, col_tag, 'TAG_DETECTADA_OBS', 'TAG_MOTIVO']].copy()
    df_pend.rename(columns={col_cc: 'CENTRO_CUSTO', col_obs: 'OBSERVACAO', col_tag: 'TAG'}, inplace=True)
    return df, df_pend


def extrair_tag_correto(texto_observacao):
    """
    Extrai a primeira TAG do texto de observação (compatibilidade).
    
    Args:
        texto_observacao: Texto da observação.
        
    Returns:
        Primeira TAG encontrada ou None.
        
    Note:
        Função mantida para compatibilidade. Prefira usar extrair_tags_da_obs().
    """
    cands = extrair_tags_da_obs(texto_observacao)
    return cands[0] if cands else None


# ======= Preços: variação, IPCA e projeção 2026 (biblioteca principal) =======

def _ensure_period_month(df, col):
    """Garante que a coluna seja um PeriodIndex mensal."""
    if df.empty:
        return df
    out = df.copy()
    out[col] = pd.PeriodIndex(out[col], freq='M') if not _np.issubdtype(out[col].dtype, _np.number) else out[col]
    # Se já vier como string YYYY-MM está ok; PeriodIndex converte
    return out


def compute_price_metrics(mensal_qtd: pd.DataFrame, mensal_valor: pd.DataFrame | None, tab_aux_path: Path | str | None = None, ano_base: int = 2025) -> pd.DataFrame:
    """
    Calcula métricas de preço por item a partir de séries mensais.
    
    Métricas calculadas:
    - PRECO_ULTIMO: Preço unitário do último mês com consumo
    - PRECO_ANTERIOR: Preço unitário do mês anterior com consumo
    - VAR_PRECO_PCT: Variação percentual (ULTIMO/ANTERIOR - 1)
    - PRECO_BASE_ano: Preço ajustado por IPCA até ano_base
    - PRECO_2026: Projeção com ajuste configurável
    
    Args:
        mensal_qtd: DataFrame com colunas COD_ITEM, ANO_MES, QTD_MENSAL.
        mensal_valor: DataFrame com colunas COD_ITEM, ANO_MES, VALOR_MENSAL (opcional).
        tab_aux_path: Caminho para TAB_AUX.xlsx com série IPCA.
        ano_base: Ano base para projeção de preços (default: 2025).
        
    Returns:
        DataFrame com métricas de preço por COD_ITEM. Retorna NaN quando não
        há dados suficientes.
        
    Example:
        >>> precos = compute_price_metrics(mensal_qtd, mensal_valor, "TAB_AUX.xlsx")
    """
    cols_out = ['COD_ITEM', 'PRECO_ULTIMO', 'PRECO_ANTERIOR', 'VAR_PRECO_PCT', 'ANO_ULTIMO_PRECO', f'PRECO_BASE_{ano_base}', 'PRECO_2026']
    if mensal_valor is None or mensal_valor.empty or mensal_qtd is None or mensal_qtd.empty:
        return pd.DataFrame(columns=cols_out)

    q = mensal_qtd.copy()
    v = mensal_valor.copy()
    q = _ensure_period_month(q, 'ANO_MES')
    v = _ensure_period_month(v, 'ANO_MES')
    # Junta para obter preço unitário mensal
    df = q.merge(v, on=['COD_ITEM', 'ANO_MES'], how='left')
    if 'QTD_MENSAL' in df.columns:
        df['QTD_MENSAL'] = pd.to_numeric(df['QTD_MENSAL'], errors='coerce')
    else:
        df['QTD_MENSAL'] = _np.nan
    if 'VALOR_MENSAL' in df.columns:
        df['VALOR_MENSAL'] = pd.to_numeric(df['VALOR_MENSAL'], errors='coerce')
    else:
        df['VALOR_MENSAL'] = _np.nan
    df = df[(df['QTD_MENSAL'] > 0) & df['VALOR_MENSAL'].notna()]
    if df.empty:
        return pd.DataFrame(columns=cols_out)
    df['PRECO_UNIT'] = df['VALOR_MENSAL'] / df['QTD_MENSAL']
    df = df.dropna(subset=['PRECO_UNIT'])
    if df.empty:
        return pd.DataFrame(columns=cols_out)

    # Para cada item, ordenar por período e pegar último e penúltimo
    out_rows = []
    for cod, g in df.groupby('COD_ITEM'):
        g = g.sort_values('ANO_MES')
        if len(g) == 0:
            continue
        ultimo = g.iloc[-1]
        preco_ultimo = float(ultimo['PRECO_UNIT']) if pd.notna(ultimo['PRECO_UNIT']) else _np.nan
        ano_ultimo = int(str(ultimo['ANO_MES']).split('-')[0]) if pd.notna(ultimo['ANO_MES']) else _np.nan
        if len(g) >= 2:
            anterior = g.iloc[-2]
            preco_anterior = float(anterior['PRECO_UNIT']) if pd.notna(anterior['PRECO_UNIT']) else _np.nan
        else:
            preco_anterior = _np.nan
        var_pct = (preco_ultimo / preco_anterior - 1.0) if (pd.notna(preco_ultimo) and pd.notna(preco_anterior) and preco_anterior != 0) else _np.nan

        out_rows.append({'COD_ITEM': cod, 'PRECO_ULTIMO': preco_ultimo, 'PRECO_ANTERIOR': preco_anterior, 'VAR_PRECO_PCT': float(var_pct) if pd.notna(var_pct) else _np.nan, 'ANO_ULTIMO_PRECO': ano_ultimo})

    out = pd.DataFrame(out_rows)
    if out.empty:
        return pd.DataFrame(columns=cols_out)

    # Ajuste IPCA até ano_base (ex.: 2025)
    if tab_aux_path is not None:
        try:
            # Lê apenas ANO e IPCA (%) da aba TABS_AUX
            def _usecols(name: str) -> bool:
                n = str(name).strip().upper()
                return n == 'ANO' or 'IPCA' in n
            ipca = pd.read_excel(tab_aux_path, sheet_name='TABS_AUX', usecols=_usecols)
            # Espera colunas ANO, IPCA (%). Normaliza
            col_ano = next((c for c in ipca.columns if str(c).strip().upper() == 'ANO'), None)
            col_ipca = next((c for c in ipca.columns if 'IPCA' in str(c).upper()), None)
            if col_ano and col_ipca:
                ip = ipca[[col_ano, col_ipca]].dropna()
                ip.columns = ['ANO', 'IPCA_PCT']
                ip['ANO'] = ip['ANO'].astype(int)
                ip['IPCA_FATOR'] = 1.0 + (pd.to_numeric(ip['IPCA_PCT'], errors='coerce') / 100.0)
                # Função para fator acumulado (ano_ultimo+1..ano_base)
                def fator_acumulado(ano_ult: int) -> float:
                    if pd.isna(ano_ult):
                        return _np.nan
                    anos = ip[(ip['ANO'] > int(ano_ult)) & (ip['ANO'] <= int(ano_base))]
                    if anos.empty:
                        return 1.0
                    # Usa numpy para produto e converte de forma segura para float
                    return float(_np.prod(anos['IPCA_FATOR'].astype(float).to_numpy()))
                out[f'FATOR_IPCA_ATE_{ano_base}'] = out['ANO_ULTIMO_PRECO'].apply(fator_acumulado)
                out[f'PRECO_BASE_{ano_base}'] = out.apply(lambda r: r['PRECO_ULTIMO'] * r[f'FATOR_IPCA_ATE_{ano_base}'] if pd.notna(r['PRECO_ULTIMO']) and pd.notna(r.get(f'FATOR_IPCA_ATE_{ano_base}', _np.nan)) else _np.nan, axis=1)
            else:
                out[f'PRECO_BASE_{ano_base}'] = _np.nan
        except Exception:
            out[f'PRECO_BASE_{ano_base}'] = _np.nan
    else:
        out[f'PRECO_BASE_{ano_base}'] = _np.nan

    # Projecao 2026 com fator configuravel
    ajuste_pct = get_param('precos.ajuste_projecao_padrao_pct', 0.035)
    try:
        ajuste_pct = float(ajuste_pct)
    except (TypeError, ValueError):
        ajuste_pct = 0.035
    out['PRECO_2026'] = out.get(f'PRECO_BASE_{ano_base}', _np.nan) * (1.0 + ajuste_pct)
    return out[['COD_ITEM', 'PRECO_ULTIMO', 'PRECO_ANTERIOR', 'VAR_PRECO_PCT', 'ANO_ULTIMO_PRECO', f'PRECO_BASE_{ano_base}', 'PRECO_2026']]

# ======= Filtro comum para HISTORICO: CONTA_CONTABIL =======
_ALLOWED_CONTAS = {
    'MATERIAIS',
    'COMBUSTIVEIS E LUBRIFICANTES',
}

def _norm_colname_txt(n: str) -> str:
    return re.sub(r'[^A-Z0-9]', '', str(n).upper())

def find_col(df: pd.DataFrame, targets_norm: set[str]) -> str | None:
    norm_map = {c: _norm_colname_txt(c) for c in df.columns}
    for c, n in norm_map.items():
        if n in targets_norm:
            return c
    return None


def _normalize_num_tipo(valor) -> str | None:
    """
    Normaliza códigos NUM_TIPO_DESPESA para uma string de quatro dígitos.
    Aceita entradas numéricas ou textuais contendo dígitos.
    """
    if pd.isna(valor):
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    match = re.search(r'(\d+)', texto)
    if match:
        try:
            return f"{int(match.group(1)):04d}"
        except Exception:
            pass
    texto = texto.upper()
    return texto if texto else None


def to_numeric_from_text(series: pd.Series, pattern: str = r'(\d+)') -> pd.Series:
    """
    Extrai dígitos de uma série textual e retorna valores numéricos coerentes.
    Valores sem dígitos retornam NaN.
    """
    if series is None:
        return pd.Series(dtype='float64')
    s = series.astype(str)
    digits = s.str.extract(pattern, expand=False)
    return pd.to_numeric(digits, errors='coerce')


def find_centro_custo_column(columns) -> str | None:
    """
    Detecta a coluna de Centro de Custo em uma coleção de nomes.
    """
    norm_map = {c: _norm_colname_txt(c) for c in columns}
    targets = {
        'CENTROCUSTO',
        'CENTRODECUSTO',
        'CENTROCUSTOS',
        'CENTROCUSTO',
        'CENTRO_CUSTO',
        'CC',
        'CCGERAL',
        'CC_GERAL',
        'CENCUSTO',
    }
    for c, norm in norm_map.items():
        if norm in targets:
            return c
    for c, norm in norm_map.items():
        if 'CENTRO' in norm and 'CUSTO' in norm:
            return c
    return None


def to_cc_numeric(series: pd.Series) -> pd.Series:
    """
    Converte uma coluna de Centro de Custo textual em valores numericos (extraindo digitos).
    """
    return to_numeric_from_text(series)


def filter_por_fonte(
    df: pd.DataFrame,
    col_override: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Filtra DataFrame por fontes de dados permitidas.
    
    Mantém apenas linhas cuja fonte pertence à lista configurada em
    ``filtros.fontes_permitidas`` (respeitando o flag ``aplicar_filtro_fonte``).
    
    Args:
        df: DataFrame a filtrar.
        col_override: Nome da coluna de fonte (usa 'FONTE' se None).
        
    Returns:
        Tupla (df_filtrado, coluna_usada).
        
    Example:
        >>> df_filtrado, col = filter_por_fonte(df)
    """
    if df.empty:
        return df, col_override

    aplicar = bool(get_param('filtros.aplicar_filtro_fonte', False))
    if not aplicar:
        return df, None

    fontes_cfg = get_param('filtros.fontes_permitidas', []) or []
    fontes_norm = {
        str(item).strip().upper()
        for item in fontes_cfg
        if isinstance(item, str) and str(item).strip()
    }
    if not fontes_norm:
        return df, None

    col = col_override or find_col(df, {'FONTE'})
    if not col or col not in df.columns:
        return df, None

    serie = df[col].astype(str).str.strip().str.upper()
    mask = serie.isin(fontes_norm)
    return df.loc[mask].copy(), col

def filter_por_conta_contabil(
    df: pd.DataFrame,
    col_override: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Filtra DataFrame pelos códigos NUM_TIPO_DESPESA permitidos.
    
    Caso a coluna normalizada não esteja presente, usa fallback
    para filtro textual sobre CONTA_CONTABIL.
    
    Args:
        df: DataFrame a filtrar.
        col_override: Nome da coluna de tipo de despesa.
        
    Returns:
        Tupla (df_filtrado, coluna_usada).
        
    Example:
        >>> df_filtrado, col = filter_por_conta_contabil(df)
    """
    if df.empty:
        return df, col_override

    aplicar = bool(get_param('filtros.aplicar_filtro_conta_contabil', True))
    if not aplicar:
        return df, None

    permitidos_cfg = get_param('filtros.conta_contabil_permitidas', []) or []
    permitidos_norm = {
        code for code in (_normalize_num_tipo(v) for v in permitidos_cfg) if code
    }

    col_num = None
    if col_override and col_override in df.columns:
        col_num = col_override
    else:
        col_num = find_col(
            df,
            {
                'NUMTIPODESPESA',
                'NUMTIPODESPESAS',
                'TIPODESPESANUM',
                'CODTIPODESPESA',
                'CODIGOTIPODESPESA',
            },
        )

    if col_num and permitidos_norm:
        serie_norm = df[col_num].apply(_normalize_num_tipo)
        mask = serie_norm.isin(permitidos_norm)
        return df.loc[mask].copy(), col_num

    col_textual = col_override or find_col(df, {'CONTACONTABIL', 'CONTACONTA', 'CONTA'})
    if not col_textual or col_textual not in df.columns:
        return df, None

    serie_txt = df[col_textual].astype(str).str.strip().str.upper()
    palavras = [
        str(v).strip().upper()
        for v in permitidos_cfg
        if isinstance(v, str) and str(v).strip()
    ] or ['MATERIAIS', 'COMBUSTIVEIS E LUBRIFICANTES']
    mask = pd.Series(False, index=serie_txt.index)
    for palavra in palavras:
        mask |= serie_txt.str.contains(palavra, na=False)
    return df.loc[mask].copy(), col_textual


def remover_por_grupo_budget(
    df: pd.DataFrame,
    col_override: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Remove linhas cujos grupos de budget estejam na lista de exclusão.
    
    Usa configuração ``filtros.grupos_budget_excluir`` do params.yaml.
    
    Args:
        df: DataFrame a filtrar.
        col_override: Nome da coluna de grupo budget.
        
    Returns:
        Tupla (df_filtrado, coluna_usada).
        
    Example:
        >>> df_filtrado, col = remover_por_grupo_budget(df)
    """
    if df.empty:
        return df, col_override

    aplicar = bool(get_param('filtros.aplicar_filtro_grupo_budget', True))
    grupos_cfg = get_param('filtros.grupos_budget_excluir', []) or []
    if not aplicar or not grupos_cfg:
        return df, None

    col_budget = None
    if col_override and col_override in df.columns:
        col_budget = col_override
    else:
        col_budget = find_col(df, {'GRUPOBUDGET', 'GRUPOSBUDGET', 'BUDGETGRUPO'})
    if not col_budget or col_budget not in df.columns:
        return df, None

    grupos_norm = {
        str(g).strip().upper() for g in grupos_cfg if isinstance(g, str) and g.strip()
    }
    if not grupos_norm:
        return df, col_budget

    serie = df[col_budget].astype(str).str.strip().str.upper()
    mask = serie.isin(grupos_norm)
    if not mask.any():
        return df, col_budget
    return df.loc[~mask].copy(), col_budget

def aplicar_filtros_basicos(
    df: pd.DataFrame,
    col_cc='CENTRO_CUSTO',
    col_desc='DESC_ITEM',
    col_data: str | None = None,
    col_grupo_budget: str | None = 'GRUPO_BUDGET',
) -> pd.DataFrame:
    """
    Aplica conjunto padrão de filtros do FinScope.
    
    Filtros aplicados (conforme params.yaml):
    - Ano mínimo de dados (se aplicar_filtro_ano_minimo=True)
    - Faixa de Centro de Custo (se aplicar_filtro_centro_custo=True)
    - Exclusão de CCs específicos (centro_custo_excluir)
    - Remoção por palavra-chave de parada
    - Exclusão de grupos de budget
    
    Args:
        df: DataFrame a filtrar.
        col_cc: Nome da coluna de Centro de Custo.
        col_desc: Nome da coluna de descrição do item.
        col_data: Nome da coluna de data (para filtro por ano).
        col_grupo_budget: Nome da coluna de grupo budget.
        
    Returns:
        DataFrame filtrado.
        
    Example:
        >>> df_filtrado = aplicar_filtros_basicos(df, col_data='DATA_ENTREGA')
    """
    out = df.copy()
    if col_data and col_data in out.columns and bool(get_param('filtros.aplicar_filtro_ano_minimo', False)):
        ano_min_cfg = get_param('filtros.ano_minimo_dados', None)
        try:
            ano_min_int = int(float(ano_min_cfg))
        except (TypeError, ValueError):
            ano_min_int = None
        if ano_min_int is not None:
            data_series = pd.to_datetime(out[col_data], errors='coerce')
            out[col_data] = data_series
            mask_ano = data_series.dt.year >= ano_min_int
            out = out.loc[mask_ano | data_series.isna()].copy()
    if col_cc in out.columns and bool(get_param('filtros.aplicar_filtro_centro_custo', True)):
        try:
            lo, hi = get_param('filtros.centro_custo_range', [6000, 6675])
            lo, hi = int(lo), int(hi)
            cc_num = pd.to_numeric(out[col_cc], errors='coerce')
            # Faixa permitida
            out = out.loc[((cc_num >= lo) & (cc_num <= hi)) | (cc_num.isna())].copy()

            # Exclusões específicas
            cc_excluir = get_param('filtros.centro_custo_excluir', []) or []
            excluir_set = set()
            if isinstance(cc_excluir, (list, tuple, set)):
                for v in cc_excluir:
                    try:
                        excluir_set.add(int(float(v)))
                    except (TypeError, ValueError):
                        continue
            if excluir_set:
                cc_num2 = pd.to_numeric(out[col_cc], errors='coerce')
                mask_excluir = cc_num2.isin(excluir_set)
                if mask_excluir.any():
                    out = out.loc[~mask_excluir | cc_num2.isna()].copy()
        except Exception:
            pass
    if col_desc in out.columns and bool(get_param('filtros.remover_itens_palavra_parada', True)):
        palavra = str(get_param('filtros.palavra_parada', 'PARADA')).upper()
        mask_parada = out[col_desc].astype(str).str.upper().str.contains(palavra, na=False)
        out = out.loc[~mask_parada].copy()

    col_budget = None
    if col_grupo_budget and col_grupo_budget in out.columns:
        col_budget = col_grupo_budget
    else:
        col_budget = find_col(out, {'GRUPOBUDGET', 'GRUPOSBUDGET', 'BUDGETGRUPO'})
    if col_budget:
        out, _ = remover_por_grupo_budget(out, col_budget)

    return out

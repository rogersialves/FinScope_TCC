"""
Utilitários compartilhados para pipelines de processamento de dados.

Este módulo contém funções que são usadas por múltiplos pipelines
(Modulo_Solidos, Modulo_Solidos_ML, etc.) para evitar duplicação de código.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

import numpy as np
import pandas as pd

from core.logger import get_logger

_logger = get_logger(__name__)


# =============================================================================
# Normalização de Dados
# =============================================================================


def normalize_cod_item_value(valor: Any) -> Optional[str]:
    """
    Normaliza um valor de COD_ITEM para string estável sem casas decimais.

    Args:
        valor: Valor a ser normalizado (int, float, str, numpy types).

    Returns:
        String normalizada ou None se valor inválido.

    Examples:
        >>> normalize_cod_item_value(123)
        '123'
        >>> normalize_cod_item_value(123.0)
        '123'
        >>> normalize_cod_item_value('ABC-123')
        'ABC-123'
        >>> normalize_cod_item_value(None)
        None
    """
    if valor is None:
        return None

    # Trata numpy types
    if isinstance(valor, (np.integer, int)):
        return str(int(valor))

    if isinstance(valor, (np.floating, float)):
        if np.isfinite(valor) and float(valor).is_integer():
            return str(int(valor))
        return str(valor).strip()

    text = str(valor).strip()
    if text == "" or text.lower() in {"nan", "none", "nat"}:
        return None

    return text


def normalize_cod_item_series(series: pd.Series) -> pd.Series:
    """
    Padroniza uma coluna COD_ITEM como string limpa preservando valores ausentes.

    Args:
        series: Series pandas com valores de COD_ITEM.

    Returns:
        Series com valores normalizados.
    """
    return series.map(normalize_cod_item_value, na_action=None)


def normalize_num_tipo_despesa(valor: Any) -> Optional[str]:
    """
    Normaliza códigos de NUM_TIPO_DESPESA para strings com quatro dígitos.

    Args:
        valor: Valor a ser normalizado.

    Returns:
        String com 4 dígitos ou None se inválido.

    Examples:
        >>> normalize_num_tipo_despesa(802)
        '0802'
        >>> normalize_num_tipo_despesa('0802')
        '0802'
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None

    texto = str(valor).strip()
    if not texto:
        return None

    digits = "".join(ch for ch in texto if ch.isdigit())
    if digits:
        try:
            return f"{int(digits):04d}"
        except Exception:
            return digits

    return texto.upper() or None


def padronizar_desc_item(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza a coluna DESC_ITEM removendo espaços extras.

    Args:
        df: DataFrame com colunas COD_ITEM e DESC_ITEM.

    Returns:
        DataFrame com DESC_ITEM padronizada.
    """
    if "DESC_ITEM" not in df.columns or "COD_ITEM" not in df.columns:
        return df

    df = df.copy()

    # Remove espaços duplicados
    df["DESC_ITEM"] = (
        df["DESC_ITEM"]
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

    # Usa a primeira descrição encontrada por COD_ITEM
    mapa = df.dropna(subset=["COD_ITEM"]).groupby("COD_ITEM")["DESC_ITEM"].first()
    df["DESC_ITEM"] = df["COD_ITEM"].map(mapa).fillna(df["DESC_ITEM"])

    return df


# =============================================================================
# Detecção de Colunas
# =============================================================================


def detectar_colunas_data_qtd_valor(
    df: pd.DataFrame,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Detecta automaticamente colunas de data, quantidade e valor no DataFrame.

    Args:
        df: DataFrame para análise.

    Returns:
        Tupla (col_data, col_qtd, col_valor) com nomes das colunas ou None.

    Examples:
        >>> df = pd.DataFrame({'DATA': [...], 'QUANTIDADE': [...], 'VALOR': [...]})
        >>> detectar_colunas_data_qtd_valor(df)
        ('DATA', 'QUANTIDADE', 'VALOR')
    """
    candidatos_data = [
        "DATA",
        "DATA_MOV",
        "DT_MOV",
        "EMISSAO",
        "DATA_EMISSAO",
        "DATA_PEDIDO",
        "DATA_ENTREGA",
    ]
    candidatos_qtd = [
        "QTD",
        "QUANTIDADE",
        "QTDE",
        "QTD_MOV",
        "QTD_ITEM",
        "QUANT",
        "QDE",
        "QTD_SOLIC",
        "QTD_APROV",
        "QTD_ATENDIDA",
    ]
    candidatos_valor = [
        "VALOR",
        "VL_TOTAL",
        "VALOR_TOTAL",
        "CUSTO_TOTAL",
        "CUSTO",
        "PRECO",
        "VALOR_ITEM",
        "VL_ITEM",
    ]

    col_data = next((c for c in candidatos_data if c in df.columns), None)
    col_qtd = next((c for c in candidatos_qtd if c in df.columns), None)
    col_valor = next((c for c in candidatos_valor if c in df.columns), None)

    return col_data, col_qtd, col_valor


# =============================================================================
# Consolidação de Duplicatas
# =============================================================================


def consolidar_duplicatas_exatas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove registros exatamente idênticos nos campos principais.

    Consolida registros duplicados em COD_ITEM, DESC_ITEM, QUANTIDADE,
    VALOR e DATA_ENTREGA mantendo apenas um por combinação.

    Args:
        df: DataFrame a ser consolidado.

    Returns:
        DataFrame sem duplicatas exatas.
    """
    req = ["COD_ITEM", "DESC_ITEM", "QUANTIDADE", "VALOR", "DATA_ENTREGA"]
    missing = [c for c in req if c not in df.columns]

    if missing:
        _logger.debug(
            "Colunas faltantes para consolidar duplicatas: %s", missing
        )
        return df

    return df.drop_duplicates(subset=req, keep="first")


# =============================================================================
# Mapeamentos de Descrição
# =============================================================================


def criar_mapa_descricao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria um mapa COD_ITEM -> DESC_ITEM a partir do DataFrame.

    Args:
        df: DataFrame com colunas COD_ITEM e DESC_ITEM.

    Returns:
        DataFrame com mapeamento único COD_ITEM -> DESC_ITEM.
    """
    if "DESC_ITEM" not in df.columns:
        return pd.DataFrame(columns=["COD_ITEM", "DESC_ITEM"])

    return (
        df[["COD_ITEM", "DESC_ITEM"]]
        .dropna(subset=["COD_ITEM"])
        .drop_duplicates(subset=["COD_ITEM"])
    )


# =============================================================================
# Agregações Temporais
# =============================================================================


def preparar_colunas_temporais(
    df: pd.DataFrame, col_data: str
) -> pd.DataFrame:
    """
    Adiciona colunas de período temporal ao DataFrame.

    Args:
        df: DataFrame com coluna de data.
        col_data: Nome da coluna de data.

    Returns:
        DataFrame com colunas ANO, ANO_MES e ANO_TRIM adicionadas.
    """
    df = df.copy()
    df[col_data] = pd.to_datetime(df[col_data], errors="coerce")
    df = df.dropna(subset=[col_data])

    df["ANO"] = df[col_data].dt.year.astype("Int64")  # type: ignore[union-attr]
    df["ANO_MES"] = df[col_data].dt.to_period("M").astype(str)  # type: ignore[union-attr]
    df["ANO_TRIM"] = df[col_data].dt.to_period("Q").astype(str)  # type: ignore[union-attr]

    return df


def extract_date_components(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Extrai componentes de data (ano, mês, período) de uma coluna."""
    if df.empty or date_col not in df.columns:
        return df
    
    df = df.copy()
    
    # Converter para datetime
    dt_series = pd.to_datetime(df[date_col], errors="coerce")
    
    # Extrair usando .dt accessor
    df["ANO"] = dt_series.dt.year
    df["MES"] = dt_series.dt.month
    df["PERIODO_M"] = dt_series.dt.to_period("M")
    df["PERIODO_Y"] = dt_series.dt.to_period("Y")
    
    return df


def agregar_mensal(
    df: pd.DataFrame, col_qtd: str, map_desc: pd.DataFrame
) -> pd.DataFrame:
    """
    Agrega dados mensalmente por COD_ITEM.

    Args:
        df: DataFrame com colunas COD_ITEM, ANO_MES e coluna de quantidade.
        col_qtd: Nome da coluna de quantidade.
        map_desc: Mapa de descrições COD_ITEM -> DESC_ITEM.

    Returns:
        DataFrame agregado mensalmente.
    """
    mensal = (
        df.groupby(["COD_ITEM", "ANO_MES"])[col_qtd]
        .sum()
        .rename("QTD_MENSAL")
        .reset_index()
        .sort_values(["COD_ITEM", "ANO_MES"])
    )

    mensal = mensal.merge(map_desc, on="COD_ITEM", how="left")
    return mensal[["COD_ITEM", "DESC_ITEM", "ANO_MES", "QTD_MENSAL"]]


def agregar_trimestral(
    df: pd.DataFrame, col_qtd: str, map_desc: pd.DataFrame
) -> pd.DataFrame:
    """
    Agrega dados trimestralmente por COD_ITEM.

    Args:
        df: DataFrame com colunas COD_ITEM, ANO_TRIM e coluna de quantidade.
        col_qtd: Nome da coluna de quantidade.
        map_desc: Mapa de descrições COD_ITEM -> DESC_ITEM.

    Returns:
        DataFrame agregado trimestralmente.
    """
    trimestral = (
        df.groupby(["COD_ITEM", "ANO_TRIM"])[col_qtd]
        .sum()
        .rename("QTD_TRIMESTRE")
        .reset_index()
        .sort_values(["COD_ITEM", "ANO_TRIM"])
    )

    trimestral = trimestral.merge(map_desc, on="COD_ITEM", how="left")
    return trimestral[["COD_ITEM", "DESC_ITEM", "ANO_TRIM", "QTD_TRIMESTRE"]]


def agregar_anual(
    df: pd.DataFrame, col_qtd: str, map_desc: pd.DataFrame
) -> pd.DataFrame:
    """
    Agrega dados anualmente por COD_ITEM.

    Args:
        df: DataFrame com colunas COD_ITEM, ANO e coluna de quantidade.
        col_qtd: Nome da coluna de quantidade.
        map_desc: Mapa de descrições COD_ITEM -> DESC_ITEM.

    Returns:
        DataFrame agregado anualmente.
    """
    anual = (
        df.groupby(["COD_ITEM", "ANO"])[col_qtd]
        .sum()
        .rename("QTD_ANO")
        .reset_index()
        .sort_values(["COD_ITEM", "ANO"])
    )

    anual = anual.merge(map_desc, on="COD_ITEM", how="left")
    return anual[["COD_ITEM", "DESC_ITEM", "ANO", "QTD_ANO"]]


def filter_by_year(df: pd.DataFrame, date_col: str, min_year: int) -> pd.DataFrame:
    """Filtra DataFrame por ano mínimo."""
    if df.empty or date_col not in df.columns:
        return df
    
    df = df.copy()
    dt_series = pd.to_datetime(df[date_col], errors="coerce")
    
    # Usar .dt.year em vez de .year diretamente
    mask = dt_series.dt.year >= min_year
    
    return df[mask]

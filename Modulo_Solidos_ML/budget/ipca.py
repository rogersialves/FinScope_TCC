"""
Funções para correção de valores com IPCA.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from core.logger import get_logger

_logger = get_logger(__name__)


def load_ipca_fatores(aux_path: Path) -> Optional[pd.DataFrame]:
    """
    Carrega fatores de correção IPCA do arquivo auxiliar.
    
    Args:
        aux_path: Caminho para o arquivo TAB_AUX.xlsx
        
    Returns:
        DataFrame com colunas ['ANO', 'IPCA_FATOR'] ou None se falhar
    """
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
        
        return out[['ANO', 'IPCA_FATOR']]
        
    except FileNotFoundError:
        _logger.debug("Arquivo IPCA não encontrado: %s", aux_path)
        return None
    except Exception as e:
        _logger.warning("Erro ao carregar fatores IPCA: %s", e)
        return None


def fator_acumulado_ipca(
    ipca: Optional[pd.DataFrame],
    ano_origem: int,
    ano_dest: int
) -> float:
    """
    Calcula fator de correção IPCA acumulado entre dois anos.
    
    Args:
        ipca: DataFrame com fatores IPCA por ano
        ano_origem: Ano inicial
        ano_dest: Ano final
        
    Returns:
        Fator multiplicador acumulado (1.0 se não houver correção)
        
    Example:
        >>> fator = fator_acumulado_ipca(ipca_df, 2023, 2026)
        >>> valor_corrigido = valor_original * fator
    """
    if ipca is None or pd.isna(ano_origem) or pd.isna(ano_dest):
        return 1.0
        
    if ano_dest <= ano_origem:
        return 1.0
        
    rng = ipca[(ipca['ANO'] > int(ano_origem)) & (ipca['ANO'] <= int(ano_dest))]
    
    if rng.empty:
        return 1.0
        
    return float(np.prod(rng['IPCA_FATOR'].astype(float).to_numpy()))

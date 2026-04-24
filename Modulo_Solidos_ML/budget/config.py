"""
Configurações e constantes para projeção orçamentária.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


# Ordem amigável para classes de uso (para relatórios/ordenação)
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

# Nomes de colunas padrão
QTD_BASE_COL = 'QTD_PROJETADA_BASE_2026'
QTD_FINAL_COL = 'QTD_PROJETADA_COM_MARGEM'
QTD_FINAL_LEGACY_COL = 'QTD_PROJETADA_2026'


@dataclass
class BudgetConfig:
    """
    Configuração para projeção orçamentária.
    
    Attributes:
        ano_orcamento: Ano alvo do orçamento (default: 2026)
        margem_seguranca_pct: Margem de segurança percentual (default: 0.15 = 15%)
        ajuste_inflacao_anual: Taxa de inflação anual para correção (default: 0.035 = 3.5%)
        horizonte_meses: Horizonte de previsão em meses (default: 12)
        modo_orcamento: Modo de cálculo - 'essencial' | 'fidelidade' | 'inteligente'
    """
    ano_orcamento: int = 2026
    margem_seguranca_pct: float = 0.15
    ajuste_inflacao_anual: float = 0.035
    horizonte_meses: int = 12
    modo_orcamento: str = 'essencial'

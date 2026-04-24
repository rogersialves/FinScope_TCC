"""
Funções de exportação de orçamento.

Este módulo re-exporta as funções de exportação do budget_projection.py original
para manter compatibilidade durante a refatoração incremental.
"""

from __future__ import annotations

# Re-exporta do módulo original para compatibilidade
from Modulo_Solidos_ML.budget_projection import (
    export_ml_consumo,
    exportar_orcamento,
)

__all__ = [
    "export_ml_consumo",
    "exportar_orcamento",
]

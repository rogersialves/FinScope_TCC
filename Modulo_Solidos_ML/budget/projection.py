"""
Funções de projeção orçamentária.

Este módulo re-exporta as funções principais do budget_projection.py original
para manter compatibilidade durante a refatoração incremental.
"""

from __future__ import annotations

# Re-exporta do módulo original para compatibilidade
from Modulo_Solidos_ML.budget_projection import (
    calcular_projecao_orcamentaria,
    integrar_ml_com_precos,
    run_budget_projection,
)

__all__ = [
    "calcular_projecao_orcamentaria",
    "integrar_ml_com_precos", 
    "run_budget_projection",
]

"""
Pacote de projeção orçamentária com Machine Learning.

Este pacote divide o budget_projection.py original em módulos menores:
- config: Configurações e dataclasses
- metrics: Cálculo de métricas (uso, ADI, CV²)
- ipca: Funções de correção IPCA
- projection: Projeção principal
- export: Exportação Excel/JSON

Para manter compatibilidade, todas as funções públicas são re-exportadas aqui.
"""

from Modulo_Solidos_ML.budget.config import BudgetConfig
from Modulo_Solidos_ML.budget.ipca import (
    load_ipca_fatores,
    fator_acumulado_ipca,
)
from Modulo_Solidos_ML.budget.metrics import (
    build_indicadores,
    build_usage_metrics,
    build_monthly_distribution,
    compute_adi_cv2_local,
    build_selector_features,
)
from Modulo_Solidos_ML.budget.projection import (
    calcular_projecao_orcamentaria,
    integrar_ml_com_precos,
    run_budget_projection,
)
from Modulo_Solidos_ML.budget.export import (
    export_ml_consumo,
    exportar_orcamento,
)

__all__ = [
    # Config
    "BudgetConfig",
    # IPCA
    "load_ipca_fatores",
    "fator_acumulado_ipca",
    # Metrics
    "build_indicadores",
    "build_usage_metrics",
    "build_monthly_distribution",
    "compute_adi_cv2_local",
    "build_selector_features",
    # Projection
    "calcular_projecao_orcamentaria",
    "integrar_ml_com_precos",
    "run_budget_projection",
    # Export
    "export_ml_consumo",
    "exportar_orcamento",
]

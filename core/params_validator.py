"""
Validação de parâmetros do FinScope.

Este módulo fornece funções para validar o arquivo params.yaml,
garantindo que todos os parâmetros necessários estejam presentes
e com valores válidos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Callable

from core.fin_params import load_params as _load_params_core, get_param
from core.logger import get_logger

_logger = get_logger(__name__)


@dataclass
class ValidationRule:
    """Regra de validação para um parâmetro."""
    path: str
    required: bool = True
    expected_type: type | tuple[type, ...] | None = None
    validator: Callable[[Any], bool] | None = None
    default: Any = None
    description: str = ""


@dataclass
class ValidationResult:
    """Resultado de uma validação."""
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# Schema de validação do params.yaml
PARAMS_SCHEMA: list[ValidationRule] = [
    # Sistema
    ValidationRule("sistema.versao", True, str, description="Versão do sistema"),
    ValidationRule("sistema.paths.historico", True, str, description="Caminho do arquivo histórico"),
    ValidationRule("sistema.paths.tab_aux", True, str, description="Caminho do TAB_AUX"),
    
    # Filtros
    ValidationRule("filtros.centro_custo_range", True, list, 
                   lambda x: len(x) == 2 and all(isinstance(i, int) for i in x),
                   description="Range de centro de custo [inicio, fim]"),
    ValidationRule("filtros.aplicar_filtro_centro_custo", False, bool, default=True),
    ValidationRule("filtros.aplicar_filtro_conta_contabil", False, bool, default=True),
    ValidationRule("filtros.conta_contabil_permitidas", False, list),
    ValidationRule("filtros.aplicar_filtro_grupo_budget", False, bool, default=True),
    ValidationRule("filtros.grupos_budget_excluir", False, list),
    
    # ML
    ValidationRule("ml.horizon_meses_default", True, int, lambda x: 1 <= x <= 60,
                   description="Horizonte de previsão em meses"),
    ValidationRule("ml.meses_minimos_historico", True, int, lambda x: x >= 1,
                   description="Meses mínimos de histórico"),
    ValidationRule("ml.quality_score_minimo", True, (int, float), lambda x: 0 <= x <= 100,
                   description="Score mínimo de qualidade"),
    ValidationRule("ml.thresholds.adi", True, (int, float), lambda x: x > 0,
                   description="Threshold ADI"),
    ValidationRule("ml.thresholds.cv2", True, (int, float), lambda x: x > 0,
                   description="Threshold CV²"),
    
    # Orçamento
    ValidationRule("orcamento.ano_orcamento", True, int, lambda x: 2020 <= x <= 2100,
                   description="Ano do orçamento"),
    ValidationRule("orcamento.margem_seguranca_padrao", True, (int, float), lambda x: 0 <= x <= 1,
                   description="Margem de segurança padrão"),
    ValidationRule("orcamento.ajuste_inflacao_anual", True, (int, float), lambda x: -0.5 <= x <= 0.5,
                   description="Ajuste de inflação anual"),
    ValidationRule("orcamento.modo_padrao", True, str, 
                   lambda x: x in ("essencial", "fidelidade", "inteligente"),
                   description="Modo padrão de orçamento"),
    
    # Preços
    ValidationRule("precos.aplicar_ipca", False, bool, default=True),
    ValidationRule("precos.ano_base_preco", True, int, lambda x: 2000 <= x <= 2100,
                   description="Ano base de preços"),
    
    # PIS/COFINS
    ValidationRule("piscofins.aliquota", True, (int, float), lambda x: 0 < x < 1,
                   description="Alíquota PIS/COFINS"),
    ValidationRule("piscofins.anos_default", True, list, 
                   lambda x: all(isinstance(i, int) for i in x),
                   description="Anos default para análise"),
]


def validate_params(config_path: Path | str | None = None) -> ValidationResult:
    """
    Valida os parâmetros carregados do params.yaml.
    
    Args:
        config_path: Caminho opcional para o arquivo de configuração.
                     Se não fornecido, usa o padrão.
    
    Returns:
        ValidationResult com status, erros e warnings.
    """
    result = ValidationResult(is_valid=True)
    
    # Carrega os parâmetros (config_path é ignorado - usa paths padrão)
    try:
        _load_params_core(refresh=True)
    except Exception as e:
        result.is_valid = False
        result.errors.append(f"Falha ao carregar params.yaml: {e}")
        return result
    
    # Valida cada regra
    for rule in PARAMS_SCHEMA:
        value = get_param(rule.path, rule.default)
        
        # Verifica se é obrigatório e está presente
        if rule.required and value is None:
            result.is_valid = False
            result.errors.append(f"Parâmetro obrigatório ausente: {rule.path}")
            continue
        
        # Se não é obrigatório e está ausente, apenas avisa
        if value is None:
            if rule.default is not None:
                result.warnings.append(
                    f"Parâmetro '{rule.path}' não definido, usando default: {rule.default}"
                )
            continue
        
        # Verifica tipo
        if rule.expected_type and not isinstance(value, rule.expected_type):
            result.is_valid = False
            result.errors.append(
                f"Tipo inválido para '{rule.path}': esperado {rule.expected_type}, "
                f"obtido {type(value).__name__}"
            )
            continue
        
        # Executa validador customizado
        if rule.validator:
            try:
                if not rule.validator(value):
                    result.is_valid = False
                    result.errors.append(
                        f"Valor inválido para '{rule.path}': {value} ({rule.description})"
                    )
            except Exception as e:
                result.is_valid = False
                result.errors.append(f"Erro ao validar '{rule.path}': {e}")
    
    return result


def validate_and_log() -> bool:
    """
    Valida params.yaml e loga os resultados.
    
    Returns:
        True se válido, False caso contrário.
    """
    result = validate_params()
    
    if result.warnings:
        for warning in result.warnings:
            _logger.warning(warning)
    
    if result.errors:
        _logger.error("Erros de validação em params.yaml:")
        for error in result.errors:
            _logger.error(f"  - {error}")
        return False
    
    _logger.info("params.yaml validado com sucesso")
    return True


if __name__ == "__main__":
    # Executa validação via linha de comando
    import sys
    
    success = validate_and_log()
    sys.exit(0 if success else 1)

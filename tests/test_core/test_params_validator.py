"""Testes para o validador de parâmetros."""

import pytest
from pathlib import Path
from unittest.mock import patch

from core.params_validator import (
    validate_params,
    validate_and_log,
    ValidationRule,
    ValidationResult,
    PARAMS_SCHEMA,
)


class TestValidationRule:
    """Testes para a classe ValidationRule."""
    
    def test_default_values(self):
        """Testa valores default da regra."""
        rule = ValidationRule(path="test.path")
        assert rule.required is True
        assert rule.expected_type is None
        assert rule.validator is None
        assert rule.default is None
        assert rule.description == ""
    
    def test_custom_values(self):
        """Testa valores customizados da regra."""
        rule = ValidationRule(
            path="ml.horizon",
            required=True,
            expected_type=int,
            validator=lambda x: x > 0,
            default=12,
            description="Horizonte de previsão"
        )
        assert rule.path == "ml.horizon"
        assert rule.required is True
        assert rule.expected_type == int
        assert rule.validator is not None and rule.validator(5) is True
        assert rule.default == 12


class TestValidationResult:
    """Testes para a classe ValidationResult."""
    
    def test_default_result(self):
        """Testa resultado de validação padrão."""
        result = ValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == []
    
    def test_result_with_errors(self):
        """Testa resultado com erros."""
        result = ValidationResult(
            is_valid=False,
            errors=["Erro 1", "Erro 2"]
        )
        assert result.is_valid is False
        assert len(result.errors) == 2


class TestParamsSchema:
    """Testes para o schema de validação."""
    
    def test_schema_has_required_paths(self):
        """Verifica se o schema contém os caminhos principais."""
        paths = [rule.path for rule in PARAMS_SCHEMA]
        
        assert "sistema.versao" in paths
        assert "ml.horizon_meses_default" in paths
        assert "orcamento.ano_orcamento" in paths
        assert "piscofins.aliquota" in paths
    
    def test_schema_validators_are_callable(self):
        """Verifica se os validadores são funções válidas."""
        for rule in PARAMS_SCHEMA:
            if rule.validator is not None:
                assert callable(rule.validator)


class TestValidateParams:
    """Testes para a função validate_params."""
    
    def test_validate_real_params(self):
        """Testa validação com o params.yaml real."""
        result = validate_params()
        
        # O arquivo real deve ser válido
        assert result.is_valid is True, f"Erros: {result.errors}"
    
    def test_validate_reports_warnings(self):
        """Verifica se warnings são reportados para parâmetros opcionais."""
        result = validate_params()
        
        # Resultado deve ser válido mesmo com warnings
        assert isinstance(result.warnings, list)


class TestValidateAndLog:
    """Testes para a função validate_and_log."""
    
    def test_validate_and_log_returns_bool(self):
        """Verifica se retorna booleano."""
        result = validate_and_log()
        assert isinstance(result, bool)
    
    def test_validate_and_log_success(self):
        """Testa validação com sucesso."""
        result = validate_and_log()
        assert result is True


def test_schema_validators_all_callable():
    """Testa se todos os validadores no schema são callable."""
    for rule in PARAMS_SCHEMA:
        if rule.validator is not None:
            assert callable(rule.validator), f"Validator para {rule.path} não é callable"

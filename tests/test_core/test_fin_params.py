"""
Testes para core/fin_params.py

Cobertura:
- load_params() com e sem arquivo
- get_param() com caminhos válidos e inválidos
- merge de configurações
- cache e refresh
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pytest


class TestLoadParams:
    """Testes para a função load_params()."""

    def test_load_params_returns_dict(self) -> None:
        """load_params deve sempre retornar um dict."""
        from core import fin_params

        # Força refresh para garantir nova leitura
        result = fin_params.load_params(refresh=True)
        assert isinstance(result, dict)

    def test_load_params_cache(self) -> None:
        """load_params deve usar cache quando refresh=False."""
        from core import fin_params

        # Primeira chamada (pode ou não usar cache)
        first = fin_params.load_params(refresh=True)
        
        # Segunda chamada deve retornar mesmo objeto (cache)
        second = fin_params.load_params(refresh=False)
        assert first is second

    def test_load_params_refresh_clears_cache(self) -> None:
        """load_params com refresh=True deve recarregar."""
        from core import fin_params

        # Carrega inicial
        fin_params.load_params(refresh=True)
        
        # Modifica cache interno para verificar refresh
        if fin_params._CACHE is not None:
            original_cache = fin_params._CACHE.copy()
            fin_params._CACHE["_test_key"] = "test_value"
            
            # Refresh deve recarregar do arquivo
            refreshed = fin_params.load_params(refresh=True)
            
            # _test_key não deve existir após refresh (veio do arquivo)
            assert "_test_key" not in refreshed

    def test_load_params_from_custom_path(self, tmp_path: Path) -> None:
        """load_params deve respeitar FINSCOPE_CONFIG env var."""
        from core import fin_params

        # Cria arquivo temporário de config
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text("test:\n  key: value123\n", encoding="utf-8")

        # Usa env var para apontar para arquivo
        with mock.patch.dict(os.environ, {"FINSCOPE_CONFIG": str(config_file)}):
            result = fin_params.load_params(refresh=True)
            assert result.get("test", {}).get("key") == "value123"

    def test_load_params_without_yaml_module(self) -> None:
        """load_params deve retornar {} se yaml não estiver disponível."""
        from core import fin_params

        original_yaml = fin_params.yaml
        try:
            fin_params.yaml = None
            result = fin_params.load_params(refresh=True)
            assert result == {}
        finally:
            fin_params.yaml = original_yaml


class TestGetParam:
    """Testes para a função get_param()."""

    def test_get_param_existing_key(self, tmp_path: Path) -> None:
        """get_param deve retornar valor existente."""
        from core import fin_params

        # Setup: cria config com valor conhecido
        config_file = tmp_path / "params.yaml"
        config_file.write_text(
            "nivel1:\n  nivel2:\n    chave: 42\n",
            encoding="utf-8"
        )

        with mock.patch.dict(os.environ, {"FINSCOPE_CONFIG": str(config_file)}):
            fin_params.load_params(refresh=True)
            result = fin_params.get_param("nivel1.nivel2.chave")
            assert result == 42

    def test_get_param_missing_key_returns_default(self) -> None:
        """get_param deve retornar default para chave inexistente."""
        from core import fin_params

        fin_params.load_params(refresh=True)
        result = fin_params.get_param("chave.inexistente.profunda", default="fallback")
        assert result == "fallback"

    def test_get_param_default_is_none(self) -> None:
        """get_param deve retornar None como default padrão."""
        from core import fin_params

        fin_params.load_params(refresh=True)
        result = fin_params.get_param("chave.inexistente")
        assert result is None

    def test_get_param_partial_path(self, tmp_path: Path) -> None:
        """get_param com caminho parcial deve retornar subdict."""
        from core import fin_params

        config_file = tmp_path / "params.yaml"
        config_file.write_text(
            "parent:\n  child1: a\n  child2: b\n",
            encoding="utf-8"
        )

        with mock.patch.dict(os.environ, {"FINSCOPE_CONFIG": str(config_file)}):
            fin_params.load_params(refresh=True)
            result = fin_params.get_param("parent")
            assert isinstance(result, dict)
            assert result.get("child1") == "a"
            assert result.get("child2") == "b"


class TestMerge:
    """Testes para a função _merge()."""

    def test_merge_simple_dicts(self) -> None:
        """_merge deve combinar dicts simples."""
        from core.fin_params import _merge

        a = {"x": 1, "y": 2}
        b = {"y": 3, "z": 4}
        result = _merge(a, b)
        
        assert result == {"x": 1, "y": 3, "z": 4}

    def test_merge_nested_dicts(self) -> None:
        """_merge deve mesclar dicts aninhados recursivamente."""
        from core.fin_params import _merge

        a = {"level1": {"a": 1, "b": 2}}
        b = {"level1": {"b": 99, "c": 3}}
        result = _merge(a, b)
        
        assert result == {"level1": {"a": 1, "b": 99, "c": 3}}

    def test_merge_with_empty(self) -> None:
        """_merge deve lidar com dicts vazios."""
        from core.fin_params import _merge

        a = {"x": 1}
        result1 = _merge(a, {})
        result2 = _merge({}, a)
        
        assert result1 == {"x": 1}
        assert result2 == {"x": 1}

    def test_merge_with_none(self) -> None:
        """_merge deve lidar com None."""
        from core.fin_params import _merge

        a = {"x": 1}
        result = _merge(a, None)  # type: ignore[arg-type]
        assert result == {"x": 1}

"""
Módulo de gerenciamento de parâmetros de configuração do FinScope.

Este módulo fornece funções para carregar e acessar parâmetros de configuração
a partir de arquivos YAML. Suporta merge de múltiplos arquivos de configuração
e cache thread-safe.

Uso:
    from core.fin_params import get_param, load_params
    
    # Obter valor específico com fallback
    ano = get_param("orcamento.ano_orcamento", 2026)
    
    # Carregar todos os parâmetros
    config = load_params()
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import yaml  # pip install pyyaml
except ImportError:
    yaml = None  # type: ignore[assignment]  # fallback: retorna {} se não houver yaml

_LOCK: threading.Lock = threading.Lock()
_CACHE: Optional[Dict[str, Any]] = None

DEFAULT_CONFIG_PATHS: List[Path] = [
    Path('config/params.yaml'),
    Path('config/system_params.yaml'),
]


def _merge(base: Dict[str, Any], override: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Mescla dois dicionários recursivamente.
    
    Args:
        base: Dicionário base que será estendido
        override: Dicionário com valores que sobrescrevem o base
        
    Returns:
        Novo dicionário com valores mesclados (override tem precedência)
        
    Example:
        >>> _merge({"a": 1, "b": {"x": 1}}, {"b": {"y": 2}})
        {"a": 1, "b": {"x": 1, "y": 2}}
    """
    result: Dict[str, Any] = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


def load_params(refresh: bool = False) -> Dict[str, Any]:
    """
    Carrega parâmetros de configuração dos arquivos YAML.
    
    Procura arquivos de configuração na seguinte ordem:
    1. Caminho definido em FINSCOPE_CONFIG (variável de ambiente)
    2. config/params.yaml
    3. config/system_params.yaml
    
    Os arquivos são mesclados, com os posteriores sobrescrevendo os anteriores.
    
    Args:
        refresh: Se True, força recarregamento ignorando cache
        
    Returns:
        Dicionário com todos os parâmetros de configuração
        
    Example:
        >>> config = load_params()
        >>> config.get("orcamento", {}).get("ano_orcamento")
        2026
    """
    global _CACHE
    
    if not refresh and _CACHE is not None:
        return _CACHE
        
    with _LOCK:
        if not refresh and _CACHE is not None:
            return _CACHE
            
        cfg: Dict[str, Any] = {}
        candidates: List[Path] = []
        
        env_path: Optional[str] = os.getenv('FINSCOPE_CONFIG') or os.getenv('FINSCOPE_CONFIG_PATH')
        if env_path:
            candidates.append(Path(env_path))
        candidates.extend(DEFAULT_CONFIG_PATHS)
        
        for path in candidates:
            if path and path.exists() and yaml is not None:
                try:
                    with open(path, 'r', encoding='utf-8') as file:
                        data: Dict[str, Any] = yaml.safe_load(file) or {}
                        cfg = _merge(cfg, data)
                except (OSError, yaml.YAMLError):
                    # Silencioso por segurança - arquivo pode estar corrompido
                    pass
                    
        _CACHE = cfg
        return cfg


def get_param(path: str, default: Any = None) -> Any:
    """
    Obtém um parâmetro de configuração pelo caminho separado por pontos.
    
    Args:
        path: Caminho do parâmetro (ex: "orcamento.ano_orcamento")
        default: Valor retornado se o parâmetro não existir
        
    Returns:
        Valor do parâmetro ou default se não encontrado
        
    Example:
        >>> get_param("ml.horizon_meses_default", 12)
        12
        >>> get_param("filtros.conta_contabil_permitidas", [])
        ["41", "42"]
    """
    node: Any = load_params()
    
    for part in path.split('.'):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
        
    return node


def set_param(path: str, value: Any) -> None:
    """
    Define um parâmetro de configuração em memória (não persiste em arquivo).
    
    Útil para testes ou configurações temporárias durante a sessão.
    
    Args:
        path: Caminho do parâmetro (ex: "orcamento.ano_orcamento")
        value: Valor a ser definido
        
    Example:
        >>> set_param("orcamento.ano_orcamento", 2027)
        >>> get_param("orcamento.ano_orcamento")
        2027
    """
    global _CACHE
    
    with _LOCK:
        if _CACHE is None:
            _CACHE = {}
            
        parts = path.split('.')
        node = _CACHE
        
        for part in parts[:-1]:
            if part not in node or not isinstance(node[part], dict):
                node[part] = {}
            node = node[part]
            
        node[parts[-1]] = value


def clear_cache() -> None:
    """
    Limpa o cache de configurações.
    
    Útil para forçar recarregamento dos arquivos de configuração.
    """
    global _CACHE
    with _LOCK:
        _CACHE = None
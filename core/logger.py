"""
Módulo de logging centralizado para o FinScope.

Este módulo fornece uma interface padronizada de logging para todos os
módulos do projeto, substituindo o uso de print() statements.

Uso:
    from core.logger import get_logger
    logger = get_logger(__name__)
    
    logger.info("Processamento iniciado")
    logger.warning("Arquivo não encontrado, usando padrão")
    logger.error(f"Falha ao processar: {e}")
    logger.exception("Erro com traceback completo")
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


# Cores ANSI para terminal (Windows 10+ e Linux/Mac)
class Colors:
    """Códigos de cores ANSI para formatação de terminal."""
    
    RESET = "\033[0m"
    BOLD = "\033[1m"
    
    # Níveis de log
    DEBUG = "\033[36m"      # Cyan
    INFO = "\033[32m"       # Green
    WARNING = "\033[33m"    # Yellow
    ERROR = "\033[31m"      # Red
    CRITICAL = "\033[35m"   # Magenta
    
    # Elementos
    TIMESTAMP = "\033[90m"  # Gray
    NAME = "\033[34m"       # Blue


class ColoredFormatter(logging.Formatter):
    """Formatter com cores para saída de terminal."""
    
    LEVEL_COLORS = {
        logging.DEBUG: Colors.DEBUG,
        logging.INFO: Colors.INFO,
        logging.WARNING: Colors.WARNING,
        logging.ERROR: Colors.ERROR,
        logging.CRITICAL: Colors.CRITICAL,
    }
    
    def format(self, record: logging.LogRecord) -> str:
        # Cor baseada no nível
        level_color = self.LEVEL_COLORS.get(record.levelno, Colors.RESET)
        
        # Formatar timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        
        # Formatar nome do logger (truncar se muito longo)
        name = record.name
        if len(name) > 25:
            parts = name.split(".")
            if len(parts) > 1:
                name = f"{parts[0]}...{parts[-1]}"
            else:
                name = name[:22] + "..."
        
        # Montar mensagem formatada
        formatted = (
            f"{Colors.TIMESTAMP}{timestamp}{Colors.RESET} "
            f"{level_color}{record.levelname:8}{Colors.RESET} "
            f"{Colors.NAME}{name:25}{Colors.RESET} "
            f"{record.getMessage()}"
        )
        
        # Adicionar exceção se houver
        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        
        return formatted


class PlainFormatter(logging.Formatter):
    """Formatter sem cores para arquivos de log."""
    
    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


# Cache de loggers para evitar duplicação de handlers
_loggers: dict[str, logging.Logger] = {}

# Configuração global
_log_level: int = logging.INFO
_log_file: Optional[Path] = None
_initialized: bool = False


def configure(
    level: int = logging.INFO,
    log_file: Optional[str | Path] = None,
    enable_colors: bool = True
) -> None:
    """
    Configura o sistema de logging global.
    
    Args:
        level: Nível mínimo de log (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Caminho opcional para arquivo de log
        enable_colors: Se True, usa cores no terminal (padrão: True)
    
    Example:
        >>> from core.logger import configure
        >>> configure(level=logging.DEBUG, log_file="finscope.log")
    """
    global _log_level, _log_file, _initialized
    
    _log_level = level
    _log_file = Path(log_file) if log_file else None
    _initialized = True
    
    # Reconfigurar loggers existentes
    for logger in _loggers.values():
        logger.setLevel(_log_level)
        for handler in logger.handlers:
            handler.setLevel(_log_level)


def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger configurado para o módulo especificado.
    
    Args:
        name: Nome do módulo (geralmente __name__)
    
    Returns:
        Logger configurado com handlers de console e arquivo (se configurado)
    
    Example:
        >>> from core.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Processamento iniciado")
        >>> logger.error(f"Erro: {e}")
    """
    global _loggers
    
    # Retornar logger do cache se existir
    if name in _loggers:
        return _loggers[name]
    
    # Criar novo logger
    logger = logging.getLogger(name)
    logger.setLevel(_log_level)
    logger.propagate = False  # Evita duplicação com root logger
    
    # Limpar handlers existentes (evita duplicação em reloads)
    logger.handlers.clear()
    
    # Handler de console com cores
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(_log_level)
    
    # Detectar suporte a cores
    try:
        # Windows 10+ suporta ANSI
        if sys.platform == "win32":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        use_colors = sys.stdout.isatty()
    except Exception:
        use_colors = False
    
    if use_colors:
        console_handler.setFormatter(ColoredFormatter())
    else:
        console_handler.setFormatter(PlainFormatter())
    
    logger.addHandler(console_handler)
    
    # Handler de arquivo (se configurado)
    if _log_file:
        try:
            _log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(
                _log_file, 
                mode="a", 
                encoding="utf-8"
            )
            file_handler.setLevel(_log_level)
            file_handler.setFormatter(PlainFormatter())
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Não foi possível criar arquivo de log: {e}")
    
    # Armazenar no cache
    _loggers[name] = logger
    
    return logger


def set_level(level: int) -> None:
    """
    Altera o nível de log global.
    
    Args:
        level: Novo nível (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    global _log_level
    _log_level = level
    
    for logger in _loggers.values():
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)


# Atalhos convenientes para importação direta
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL


# Logger padrão para uso rápido
_default_logger: Optional[logging.Logger] = None


def info(message: str) -> None:
    """Log de nível INFO usando logger padrão."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("finscope")
    _default_logger.info(message)


def warning(message: str) -> None:
    """Log de nível WARNING usando logger padrão."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("finscope")
    _default_logger.warning(message)


def error(message: str) -> None:
    """Log de nível ERROR usando logger padrão."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("finscope")
    _default_logger.error(message)


def debug(message: str) -> None:
    """Log de nível DEBUG usando logger padrão."""
    global _default_logger
    if _default_logger is None:
        _default_logger = get_logger("finscope")
    _default_logger.debug(message)


if __name__ == "__main__":
    # Teste do módulo
    configure(level=logging.DEBUG)
    
    logger = get_logger("test.module")
    
    logger.debug("Mensagem de debug")
    logger.info("Mensagem informativa")
    logger.warning("Aviso importante")
    logger.error("Erro ocorreu")
    logger.critical("Erro crítico!")
    
    try:
        raise ValueError("Exemplo de exceção")
    except Exception:
        logger.exception("Exceção capturada com traceback")
    
    print("\n✅ Módulo de logging funcionando corretamente!")

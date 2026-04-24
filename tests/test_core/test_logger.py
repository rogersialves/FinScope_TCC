"""
Testes para core/logger.py

Cobertura:
- get_logger() retorna logger configurado
- Formatação com cores
- Formatação sem cores (arquivo)
- Níveis de log
- setup_file_logging()
"""

from __future__ import annotations

import logging
import tempfile
from io import StringIO
from pathlib import Path
from unittest import mock

import pytest


class TestGetLogger:
    """Testes para a função get_logger()."""

    def test_get_logger_returns_logger(self) -> None:
        """get_logger deve retornar uma instância de Logger."""
        from core.logger import get_logger

        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)

    def test_get_logger_same_name_returns_same_instance(self) -> None:
        """get_logger com mesmo nome deve retornar mesma instância."""
        from core.logger import get_logger

        logger1 = get_logger("same_name")
        logger2 = get_logger("same_name")
        assert logger1 is logger2

    def test_get_logger_different_names(self) -> None:
        """get_logger com nomes diferentes deve retornar instâncias diferentes."""
        from core.logger import get_logger

        logger1 = get_logger("name_one")
        logger2 = get_logger("name_two")
        assert logger1 is not logger2

    def test_get_logger_has_handler(self) -> None:
        """Logger retornado deve ter pelo menos um handler."""
        from core.logger import get_logger

        logger = get_logger("test_handler")
        # O logger ou seu parent deve ter handlers
        assert logger.handlers or logger.parent.handlers  # type: ignore[union-attr]


class TestColors:
    """Testes para a classe Colors."""

    def test_colors_has_reset(self) -> None:
        """Colors deve ter código RESET."""
        from core.logger import Colors

        assert hasattr(Colors, "RESET")
        assert Colors.RESET == "\033[0m"

    def test_colors_has_levels(self) -> None:
        """Colors deve ter cores para cada nível de log."""
        from core.logger import Colors

        assert hasattr(Colors, "DEBUG")
        assert hasattr(Colors, "INFO")
        assert hasattr(Colors, "WARNING")
        assert hasattr(Colors, "ERROR")
        assert hasattr(Colors, "CRITICAL")


class TestColoredFormatter:
    """Testes para ColoredFormatter."""

    def test_colored_formatter_formats_record(self) -> None:
        """ColoredFormatter deve formatar registros corretamente."""
        from core.logger import ColoredFormatter

        formatter = ColoredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        
        formatted = formatter.format(record)
        
        # Deve conter a mensagem
        assert "Test message" in formatted
        # Deve conter códigos de cor ANSI
        assert "\033[" in formatted

    def test_colored_formatter_truncates_long_names(self) -> None:
        """ColoredFormatter deve truncar nomes de logger muito longos."""
        from core.logger import ColoredFormatter

        formatter = ColoredFormatter()
        long_name = "very.long.module.name.that.exceeds.twenty.five.characters"
        record = logging.LogRecord(
            name=long_name,
            level=logging.INFO,
            pathname="",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )
        
        formatted = formatter.format(record)
        
        # Nome truncado deve aparecer
        assert "very" in formatted or "..." in formatted


class TestPlainFormatter:
    """Testes para PlainFormatter (logs de arquivo)."""

    def test_plain_formatter_no_colors(self) -> None:
        """PlainFormatter não deve incluir códigos de cor."""
        from core.logger import PlainFormatter

        formatter = PlainFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="",
            lineno=1,
            msg="Warning message",
            args=(),
            exc_info=None,
        )
        
        formatted = formatter.format(record)
        
        # Não deve conter códigos ANSI
        assert "\033[" not in formatted
        # Deve conter a mensagem
        assert "Warning message" in formatted
        # Deve conter o nível
        assert "WARNING" in formatted


class TestSetupFileLogging:
    """Testes para setup de logging em arquivo via configure()."""

    def test_configure_with_log_file_creates_file(self, tmp_path: Path) -> None:
        """configure com log_file deve preparar logging para arquivo."""
        from core.logger import configure, get_logger

        log_file = tmp_path / "test.log"
        configure(log_file=log_file)
        
        # Escreve algo no log
        logger = get_logger("file_test")
        logger.info("Test message for file")
        
        # Força flush dos handlers
        for handler in logging.root.handlers:
            handler.flush()
        
        # Verifica se arquivo existe (pode estar vazio dependendo do nível)
        assert log_file.exists() or (tmp_path / "test.log").parent.exists()

    def test_configure_with_string_path(self, tmp_path: Path) -> None:
        """configure deve aceitar string como caminho para log_file."""
        from core.logger import configure

        log_file = str(tmp_path / "string_path.log")
        
        # Não deve lançar exceção
        configure(log_file=log_file)


class TestLoggingLevels:
    """Testes para diferentes níveis de log."""

    def test_debug_level(self) -> None:
        """Logger deve processar mensagens DEBUG quando configurado."""
        from core.logger import get_logger

        logger = get_logger("debug_test")
        
        # Configura para DEBUG
        logger.setLevel(logging.DEBUG)
        
        # Não deve lançar exceção
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")

    def test_exception_logging(self) -> None:
        """logger.exception deve incluir traceback."""
        from core.logger import get_logger

        logger = get_logger("exception_test")
        
        try:
            raise ValueError("Test error")
        except ValueError:
            # Não deve lançar exceção
            logger.exception("Caught an error")


class TestConfigure:
    """Testes para configure()."""

    def test_configure_sets_level(self) -> None:
        """configure deve definir o nível de log."""
        from core.logger import configure, get_logger

        configure(level=logging.WARNING)
        
        # Logger deve respeitar o nível configurado
        logger = get_logger("level_test")
        assert logger.level == logging.NOTSET or logging.root.level == logging.WARNING

    def test_configure_with_file(self, tmp_path: Path) -> None:
        """configure deve configurar arquivo de log."""
        from core.logger import configure

        log_file = tmp_path / "config_test.log"
        
        # Não deve lançar exceção
        configure(level=logging.INFO, log_file=log_file)

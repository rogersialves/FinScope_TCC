"""
Utilitários assíncronos para leitura de arquivos Excel.

Este módulo fornece funções para leitura paralela e assíncrona de arquivos Excel,
melhorando a performance em operações com múltiplos arquivos ou arquivos grandes.

Uso:
    from core.async_excel import read_excel_async, read_multiple_excel_async
    
    # Leitura assíncrona de um arquivo
    df = await read_excel_async("dados.xlsx")
    
    # Leitura paralela de múltiplos arquivos
    dfs = await read_multiple_excel_async(["file1.xlsx", "file2.xlsx"])
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from core.logger import get_logger

_logger = get_logger(__name__)

# Pool de threads global para operações I/O
_executor: Optional[ThreadPoolExecutor] = None


def _get_executor(max_workers: Optional[int] = None) -> ThreadPoolExecutor:
    """
    Obtém ou cria o executor de threads global.
    
    Args:
        max_workers: Número máximo de workers. Se None, usa os CPUs disponíveis.
        
    Returns:
        ThreadPoolExecutor configurado.
    """
    global _executor
    if _executor is None:
        workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        _executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="excel_io")
        _logger.debug("ThreadPoolExecutor criado com %d workers", workers)
    return _executor


def _sync_read_excel(
    filepath: Union[str, Path],
    sheet_name: Union[str, int, List, None] = 0,
    **kwargs: Any,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Lê arquivo Excel de forma síncrona.
    
    Args:
        filepath: Caminho para o arquivo Excel.
        sheet_name: Nome ou índice da planilha.
        **kwargs: Argumentos adicionais para pd.read_excel.
        
    Returns:
        DataFrame ou dicionário de DataFrames.
    """
    filepath = Path(filepath)
    _logger.debug("Lendo Excel: %s", filepath.name)
    
    try:
        result = pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
        
        if isinstance(result, dict):
            _logger.info("Excel '%s' carregado: %d abas", filepath.name, len(result))
        else:
            _logger.info("Excel '%s' carregado: %d linhas", filepath.name, len(result))
            
        return result
        
    except Exception as e:
        _logger.error("Erro ao ler Excel '%s': %s", filepath.name, e)
        raise


async def read_excel_async(
    filepath: Union[str, Path],
    sheet_name: Union[str, int, List, None] = 0,
    **kwargs: Any,
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Lê arquivo Excel de forma assíncrona.
    
    Esta função executa a leitura em uma thread separada para não bloquear
    o event loop, ideal para aplicações async ou quando há outras tarefas
    a serem executadas durante a leitura.
    
    Args:
        filepath: Caminho para o arquivo Excel.
        sheet_name: Nome ou índice da planilha. Pode ser:
            - str: Nome da aba
            - int: Índice da aba (0-based)
            - list: Lista de nomes/índices
            - None: Todas as abas
        **kwargs: Argumentos adicionais para pd.read_excel.
        
    Returns:
        DataFrame único ou dicionário de DataFrames (se múltiplas abas).
        
    Example:
        >>> df = await read_excel_async("dados.xlsx")
        >>> dfs = await read_excel_async("dados.xlsx", sheet_name=None)
    """
    loop = asyncio.get_event_loop()
    executor = _get_executor()
    
    return await loop.run_in_executor(
        executor,
        lambda: _sync_read_excel(filepath, sheet_name, **kwargs)
    )


async def read_multiple_excel_async(
    filepaths: List[Union[str, Path]],
    sheet_name: Union[str, int, List, None] = 0,
    return_dict: bool = True,
    **kwargs: Any,
) -> Union[Dict[str, pd.DataFrame], List[pd.DataFrame]]:
    """
    Lê múltiplos arquivos Excel em paralelo.
    
    Esta função é ideal para carregar vários arquivos simultaneamente,
    aproveitando I/O paralelo para reduzir o tempo total de carregamento.
    
    Args:
        filepaths: Lista de caminhos para arquivos Excel.
        sheet_name: Nome ou índice da planilha (aplicado a todos os arquivos).
        return_dict: Se True, retorna dict com filename como chave.
        **kwargs: Argumentos adicionais para pd.read_excel.
        
    Returns:
        Dicionário {filename: DataFrame} ou lista de DataFrames.
        
    Example:
        >>> files = ["jan.xlsx", "fev.xlsx", "mar.xlsx"]
        >>> dfs = await read_multiple_excel_async(files)
        >>> df_jan = dfs["jan.xlsx"]
    """
    _logger.info("Lendo %d arquivos Excel em paralelo...", len(filepaths))
    
    tasks = [
        read_excel_async(fp, sheet_name=sheet_name, **kwargs)
        for fp in filepaths
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Processa resultados
    successful = 0
    failed = 0
    
    if return_dict:
        output: Dict[str, Any] = {}
        for filepath, result in zip(filepaths, results):
            filename = Path(filepath).name
            if isinstance(result, Exception):
                _logger.warning("Falha ao ler '%s': %s", filename, result)
                failed += 1
            else:
                output[filename] = result
                successful += 1
        
        _logger.info("Leitura concluída: %d sucesso, %d falhas", successful, failed)
        return output
    else:
        output_list: List[pd.DataFrame] = []
        for filepath, result in zip(filepaths, results):
            if isinstance(result, Exception):
                _logger.warning("Falha ao ler '%s': %s", Path(filepath).name, result)
                failed += 1
            else:
                output_list.append(result)  # type: ignore[arg-type]
                successful += 1
        
        _logger.info("Leitura concluída: %d sucesso, %d falhas", successful, failed)
        return output_list


def _sync_write_excel(
    df: pd.DataFrame,
    filepath: Union[str, Path],
    sheet_name: str = "Sheet1",
    **kwargs: Any,
) -> Path:
    """
    Escreve DataFrame para Excel de forma síncrona.
    
    Args:
        df: DataFrame a ser salvo.
        filepath: Caminho de destino.
        sheet_name: Nome da planilha.
        **kwargs: Argumentos adicionais para df.to_excel.
        
    Returns:
        Path do arquivo criado.
    """
    filepath = Path(filepath)
    _logger.debug("Escrevendo Excel: %s", filepath.name)
    
    try:
        df.to_excel(filepath, sheet_name=sheet_name, index=False, **kwargs)
        _logger.info("Excel '%s' salvo: %d linhas", filepath.name, len(df))
        return filepath
        
    except Exception as e:
        _logger.error("Erro ao escrever Excel '%s': %s", filepath.name, e)
        raise


async def write_excel_async(
    df: pd.DataFrame,
    filepath: Union[str, Path],
    sheet_name: str = "Sheet1",
    **kwargs: Any,
) -> Path:
    """
    Escreve DataFrame para Excel de forma assíncrona.
    
    Args:
        df: DataFrame a ser salvo.
        filepath: Caminho de destino.
        sheet_name: Nome da planilha.
        **kwargs: Argumentos adicionais para df.to_excel.
        
    Returns:
        Path do arquivo criado.
        
    Example:
        >>> await write_excel_async(df, "output.xlsx")
    """
    loop = asyncio.get_event_loop()
    executor = _get_executor()
    
    return await loop.run_in_executor(
        executor,
        lambda: _sync_write_excel(df, filepath, sheet_name, **kwargs)
    )


async def write_multiple_excel_async(
    dataframes: Dict[str, pd.DataFrame],
    output_dir: Union[str, Path],
    **kwargs: Any,
) -> List[Path]:
    """
    Escreve múltiplos DataFrames para arquivos Excel em paralelo.
    
    Args:
        dataframes: Dicionário {filename: DataFrame}.
        output_dir: Diretório de destino.
        **kwargs: Argumentos adicionais para df.to_excel.
        
    Returns:
        Lista de Paths dos arquivos criados.
        
    Example:
        >>> dfs = {"jan.xlsx": df_jan, "fev.xlsx": df_fev}
        >>> paths = await write_multiple_excel_async(dfs, "output/")
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    _logger.info("Escrevendo %d arquivos Excel em paralelo...", len(dataframes))
    
    tasks = [
        write_excel_async(df, output_dir / filename, **kwargs)
        for filename, df in dataframes.items()
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filtra resultados bem-sucedidos
    successful_paths = [r for r in results if isinstance(r, Path)]
    failed = len(results) - len(successful_paths)
    
    if failed > 0:
        _logger.warning("Escrita concluída: %d sucesso, %d falhas", len(successful_paths), failed)
    else:
        _logger.info("Escrita concluída: %d arquivos salvos", len(successful_paths))
    
    return successful_paths


# ============================================================================
# Funções de conveniência para uso síncrono com parallelismo
# ============================================================================

def read_excel_parallel(
    filepaths: List[Union[str, Path]],
    sheet_name: Union[str, int, List, None] = 0,
    max_workers: Optional[int] = None,
    **kwargs: Any,
) -> Dict[str, pd.DataFrame | Dict[str, pd.DataFrame]]:
    """
    Lê múltiplos arquivos Excel em paralelo (versão síncrona).
    
    Esta função é útil quando você não está em um contexto async mas
    ainda quer aproveitar leitura paralela.
    
    Args:
        filepaths: Lista de caminhos para arquivos Excel.
        sheet_name: Nome ou índice da planilha.
        max_workers: Número máximo de threads paralelas.
        **kwargs: Argumentos adicionais para pd.read_excel.
        
    Returns:
        Dicionário {filename: DataFrame}.
        
    Example:
        >>> files = glob.glob("data/*.xlsx")
        >>> dfs = read_excel_parallel(files)
    """
    from concurrent.futures import as_completed
    
    executor = ThreadPoolExecutor(max_workers=max_workers or min(len(filepaths), 8))
    futures = {
        executor.submit(_sync_read_excel, fp, sheet_name, **kwargs): Path(fp).name
        for fp in filepaths
    }
    
    results: Dict[str, pd.DataFrame | Dict[str, pd.DataFrame]] = {}
    
    for future in as_completed(futures):
        filename = futures[future]
        try:
            results[filename] = future.result()
        except Exception as e:
            _logger.warning("Falha ao ler '%s': %s", filename, e)
    
    executor.shutdown(wait=False)
    return results  # type: ignore[return-value]


def shutdown_executor() -> None:
    """
    Encerra o executor global de threads.
    
    Deve ser chamado ao final da aplicação para liberar recursos.
    """
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=True)
        _executor = None
        _logger.debug("ThreadPoolExecutor encerrado")


# Registra cleanup automático
import atexit
atexit.register(shutdown_executor)

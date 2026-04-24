"""
Ferramenta de consolidação do histórico MAN.

Lê todas as planilhas ``CONSOLIDADO`` existentes na pasta
``exportacoes/Relatorios_MAN/man_consolidado`` e gera o arquivo único
``data/BASE_HISTORICA.xlsx``. O processo garante:

* Preservação das novas colunas normatizadas (TAG_DETECTADA_OBS,
  TAG_MOTIVO, DESCRICAO_DESPESA, GRUPO_BUDGET, NUM_TIPO_DESPESA).
* Conversão segura dos campos de data e ordenação pelo ``DATA_ENTREGA``.
* Garantia de que ``NUM_TIPO_DESPESA`` permanece como string com zeros à esquerda.

Example:
    Para executar via linha de comando::
    
        python -m core.base_historica_builder
        
    Ou programaticamente::
    
        from core.base_historica_builder import consolidar_base_historica
        consolidar_base_historica()
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from core.fin_params import get_param


# Constantes padronizadas
SOURCE_DIR = Path("exportacoes/Relatorios_MAN/man_consolidado")
TARGET_FILE = Path("data/BASE_HISTORICA.xlsx")
SHEET_NAME = "CONSOLIDADO"
DATE_COLUMNS = ("DATA_ENTREGA", "DATA_EMISSAO")
NUM_TIPO_COL = "NUM_TIPO_DESPESA"
UM_SHEET = "UNIDADE_MEDIDA"


def _normalize_cod_item_value(valor) -> str | None:
    """Normaliza COD_ITEM em string estável sem casas decimais.

    Regras:
    - None/NaN -> None
    - int/float .0 -> str(int)
    - outras strings -> strip()
    """
    try:
        import numpy as _np  # import local para evitar dependência global
    except Exception:  # pragma: no cover - ambiente sem numpy
        _np = None  # type: ignore

    if valor is None:
        return None
    try:
        # Trata numpy types se disponível
        if _np is not None and isinstance(valor, (_np.integer, _np.floating)):
            if isinstance(valor, _np.floating):
                f = float(valor)
                if _np.isfinite(f) and float(f).is_integer():
                    return str(int(f))
                return str(f).strip()
            return str(int(valor))
    except Exception:
        pass

    # Tipos nativos
    if isinstance(valor, (int,)):
        return str(int(valor))
    if isinstance(valor, float):
        if valor == valor and float(valor).is_integer():  # not NaN and integer-like
            return str(int(valor))
        return str(valor).strip()
    text = str(valor).strip()
    if text == "" or text.lower() in {"nan", "none", "nat"}:
        return None
    return text


def _normalize_num_tipo(valor) -> str | None:
    """Normaliza códigos de NUM_TIPO_DESPESA para strings com quatro dígitos."""
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    digits = "".join(ch for ch in texto if ch.isdigit())
    if digits:
        try:
            return f"{int(digits):04d}"
        except Exception:
            return digits
    return texto.upper() or None


def _apply_config_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica filtros de fonte e NUM_TIPO_DESPESA conforme params.yaml."""
    out = df.copy()

    if "FONTE" in out.columns and bool(get_param("filtros.aplicar_filtro_fonte", False)):
        fontes_cfg = get_param("filtros.fontes_permitidas", []) or []
        fontes_norm = {
            str(item).strip().upper()
            for item in fontes_cfg
            if isinstance(item, str) and str(item).strip()
        }
        if fontes_norm:
            serie = out["FONTE"].astype(str).str.strip().str.upper()
            mask = serie.isin(fontes_norm)
            removidos = (~mask).sum()
            if removidos:
                print(f"Filtro de FONTE: removidos {removidos} registros.")
            out = out.loc[mask].copy()

    if NUM_TIPO_COL in out.columns and bool(get_param("filtros.aplicar_filtro_conta_contabil", True)):
        permitidos_cfg = get_param("filtros.conta_contabil_permitidas", []) or []
        permitidos_norm = {
            _normalize_num_tipo(item)
            for item in permitidos_cfg
            if _normalize_num_tipo(item) is not None
        }
        if permitidos_norm:
            serie = out[NUM_TIPO_COL].apply(_normalize_num_tipo)
            mask = serie.isin(permitidos_norm)
            removidos = (~mask).sum()
            if removidos:
                print(f"Filtro NUM_TIPO_DESPESA: removidos {removidos} registros.")
            out = out.loc[mask].copy()
    return out


def _ensure_directory(path: Path) -> None:
    """Cria diretório pai, caso não exista."""
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _read_consolidado(file_path: Path) -> pd.DataFrame:
    """
    Lê a aba CONSOLIDADO do arquivo informado.

    Adiciona uma coluna auxiliar ``__ARQUIVO_ORIGEM__`` apenas para rastreabilidade.
    """
    df = pd.read_excel(file_path, sheet_name=SHEET_NAME)
    df["__ARQUIVO_ORIGEM__"] = file_path.name
    return df


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos importantes e ordena colunas."""
    df = df.copy()

    # Normaliza datas
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Quantidade e valor para numérico seguro
    for col in ("QUANTIDADE", "VALOR"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Mantem NUM_TIPO_DESPESA normalizado com quatro digitos
    if NUM_TIPO_COL in df.columns:
        df[NUM_TIPO_COL] = df[NUM_TIPO_COL].apply(_normalize_num_tipo)

    # Remove registros sem DATA_ENTREGA
    if "DATA_ENTREGA" in df.columns:
        antes = len(df)
        df = df.dropna(subset=["DATA_ENTREGA"])
        depois = len(df)
        if antes != depois:
            print(f"Removidos {antes - depois} registros sem DATA_ENTREGA.")

    # Ordena por DATA_ENTREGA quando disponível
    if "DATA_ENTREGA" in df.columns:
        df = df.sort_values("DATA_ENTREGA", kind="stable")

    return df


def _load_um_mapping(aux_path: Path) -> pd.DataFrame:
    """Carrega TAB_AUX.xlsx (aba UNIDADE_MEDIDA) e retorna DataFrame com [COD_ITEM, UM].

    - Aceita colunas: cod_item, familia, um
    - Normaliza cod_item para string consistente
    - Remove espaços da coluna UM
    """
    try:
        aux_df = pd.read_excel(aux_path, sheet_name=UM_SHEET)
    except Exception:
        return pd.DataFrame(columns=["COD_ITEM", "UM"])  # silencioso se não existir

    colmap = {c: str(c).strip().lower() for c in aux_df.columns}
    inv = {v: k for k, v in colmap.items()}
    col_cod = inv.get("cod_item") or inv.get("codigo") or inv.get("cod")
    col_um = inv.get("um") or inv.get("unidade") or inv.get("unidade_medida")
    if not col_cod or not col_um or col_cod not in aux_df.columns or col_um not in aux_df.columns:
        return pd.DataFrame(columns=["COD_ITEM", "UM"])  # estrutura inesperada

    out = aux_df[[col_cod, col_um]].dropna().copy()
    out.columns = ["COD_ITEM", "UM"]
    out["COD_ITEM"] = out["COD_ITEM"].map(_normalize_cod_item_value)
    out["UM"] = out["UM"].astype(str).str.strip().str.upper()
    out = out.dropna(subset=["COD_ITEM"]).drop_duplicates(subset=["COD_ITEM"], keep="first")
    return out[["COD_ITEM", "UM"]]


def _gather_columns(frames: Iterable[pd.DataFrame]) -> list[str]:
    """
    Captura a ordem agregada de colunas respeitando o primeiro arquivo,
    adicionando novas colunas ao final.
    """
    ordered: list[str] = []
    seen: set[str] = set()
    for df in frames:
        for col in df.columns:
            if col not in seen:
                ordered.append(col)
                seen.add(col)
    return ordered


def build_base_historica(
    source_dir: Path = SOURCE_DIR,
    target_file: Path = TARGET_FILE,
) -> Path:
    """
    Consolida as planilhas CONSOLIDADO e gera BASE_HISTORICA.xlsx.

    Args:
        source_dir: Diretório contendo os arquivos ``MAN_Consolidado_*.xlsx``.
        target_file: Caminho final do arquivo consolidado.

    Returns:
        Caminho absoluto do arquivo gerado.
    """
    source_dir = source_dir.resolve()
    target_file = target_file.resolve()

    if not source_dir.exists():
        raise FileNotFoundError(f"Diretório de origem não encontrado: {source_dir}")

    excel_files = sorted(source_dir.glob("*.xlsx"))
    if not excel_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .xlsx encontrado em {source_dir}."
        )

    frames: list[pd.DataFrame] = []
    for path in excel_files:
        try:
            df = _read_consolidado(path)
        except ValueError as exc:
            print(f"Aviso: arquivo {path.name} ignorado ({exc}).")
            continue
        frames.append(df)

    if not frames:
        raise RuntimeError("Nenhum dado válido encontrado para consolidar.")

    column_order = _gather_columns(frames)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[column_order]

    combined = _normalize_dataframe(combined)
    registros_antes = len(combined)
    combined = _apply_config_filters(combined)
    if len(combined) != registros_antes:
        print(f"Total apos filtros configurados: {len(combined)}/{registros_antes}")

    # Enriquecimento: adicionar UM via TAB_AUX.xlsx (aba UNIDADE_MEDIDA)
    try:
        aux_cfg = get_param("sistema.paths.tab_aux", "data/TAB_AUX.xlsx") or "data/TAB_AUX.xlsx"
        aux_path = Path(aux_cfg)
        if not aux_path.is_absolute():
            aux_path = ROOT_DIR / aux_path
        um_df = _load_um_mapping(aux_path)
        if not um_df.empty and "COD_ITEM" in combined.columns:
            # Normaliza chave de junção
            key_series = combined["COD_ITEM"].map(_normalize_cod_item_value)
            combined = combined.copy()
            combined["__COD_NORM__"] = key_series
            combined = combined.merge(um_df, left_on="__COD_NORM__", right_on="COD_ITEM", how="left", suffixes=("", "_AUX"))
            # Seleciona coluna UM do aux e posiciona após DESC_ITEM quando possível
            if "UM" in combined.columns:
                um_series = combined.pop("UM")
                # Remove colunas auxiliares
                if "COD_ITEM_AUX" in combined.columns:
                    combined.drop(columns=["COD_ITEM_AUX"], inplace=True)
                if "__COD_NORM__" in combined.columns:
                    # Insert after DESC_ITEM if present, else after COD_ITEM
                    try:
                        if "DESC_ITEM" in combined.columns:
                            pos = int(list(combined.columns).index("DESC_ITEM")) + 1
                        elif "COD_ITEM" in combined.columns:
                            pos = int(list(combined.columns).index("COD_ITEM")) + 1
                        else:
                            pos = len(combined.columns)
                    except Exception:
                        pos = len(combined.columns)
                    combined.insert(pos, "UM", um_series)
                else:
                    combined["UM"] = um_series
            # Limpa coluna auxiliar
            if "__COD_NORM__" in combined.columns:
                combined.drop(columns=["__COD_NORM__"], inplace=True)
    except Exception as _e:
        # Falha silenciosa não deve impedir consolidação
        print(f"Aviso: falha ao enriquecer UM: {_e}")

    _ensure_directory(target_file)
    combined.to_excel(target_file, index=False, sheet_name=SHEET_NAME)
    print(f"Arquivo gerado: {target_file}")
    return target_file


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Constrói o arquivo BASE_HISTORICA.xlsx a partir dos relatórios MAN."
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=SOURCE_DIR,
        help="Pasta com os arquivos MAN_Consolidado (default: exportacoes/Relatorios_MAN/man_consolidado).",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=TARGET_FILE,
        help="Caminho do arquivo XLSX consolidado (default: data/BASE_HISTORICA.xlsx).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    build_base_historica(args.source, args.target)


if __name__ == "__main__":
    main()

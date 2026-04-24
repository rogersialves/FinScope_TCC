import sys
from pathlib import Path
import pandas as pd

# Garante import raiz (fin_utils) quando chamado direto
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.logger import get_logger
from Modulo_Solidos_ML.data_import import load_and_preprocess, monthly_qtd_val, monthly_price

from typing import Optional

_logger = get_logger(__name__)

def prepare_and_save_history(input_path: str | Path,
                             out_dir: Optional[str | Path] = None) -> Path:
    """
    Le BASE_HISTORICA.xlsx, aplica todos os filtros de negocio e salva um HISTORICO_FINAL.xlsx
    consolidado para consumo do pipeline ML.
    """
    input_path = Path(input_path)
    out_base = Path(out_dir) if out_dir else (Path(__file__).resolve().parent / 'MODELO_ORCAMENTO_ML')
    out_base.mkdir(parents=True, exist_ok=True)
    out_file = out_base / 'HISTORICO_FINAL.xlsx'

    _logger.info(f"[1/3] Carregando e filtrando histórico: {input_path}")
    df = load_and_preprocess(input_path)
    _logger.info(f"Registros após filtros: {len(df)} | Itens únicos: {df['COD_ITEM'].nunique()}")

    _logger.info("[2/3] Gerando séries mensais (QTD/VALOR) e preço médio mensal...")
    qtd, val = monthly_qtd_val(df)
    pmp = monthly_price(df)

    _logger.info(f"[3/3] Salvando: {out_file}")
    with pd.ExcelWriter(out_file, engine='openpyxl') as w:
        df.to_excel(w, sheet_name='CONSOLIDADO', index=False)
        qtd.to_excel(w, sheet_name='MENSAL_QTD', index=False)
        val.to_excel(w, sheet_name='MENSAL_VALOR', index=False)
        pmp.to_excel(w, sheet_name='PRECO_MENSAL', index=False)

    return out_file

if __name__ == '__main__':
    # Uso rápido: python Modulo_Solidos_ML/prepare_history.py
    raiz = Path(__file__).resolve().parents[1]
    hist = raiz / 'data/BASE_HISTORICA.xlsx'
    saida = prepare_and_save_history(hist)
    _logger.info(f"[OK] HISTORICO_FINAL.xlsx pronto em: {saida}")

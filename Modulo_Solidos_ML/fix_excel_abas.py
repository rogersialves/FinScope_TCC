from pathlib import Path
import pandas as pd
import numpy as np
from core.fin_params import get_param
from core.logger import get_logger

_logger = get_logger(__name__)

def _derive_classificacao_from_indicadores(indicadores: pd.DataFrame) -> pd.DataFrame:
    df = indicadores.copy()
    # tenta usar coluna existente
    candidates = [c for c in ['DEMAND_PATTERN','PATTERN','TIPO_PADRAO','PADRAO_DEMANDA'] if c in df.columns]
    if candidates:
        col = candidates[0]
        out = df[['COD_ITEM', col]].rename(columns={col: 'DEMAND_PATTERN'})
        return out
    # deriva por ADI/CV2
    if 'COD_ITEM' in df.columns and 'ADI' in df.columns and ('CV2' in df.columns or 'CV²' in df.columns):
        cv2_col = 'CV2' if 'CV2' in df.columns else 'CV²'
        adi = pd.to_numeric(df['ADI'], errors='coerce')
        cv2 = pd.to_numeric(df[cv2_col], errors='coerce')
        conds = [
            (adi < 1.32) & (cv2 < 0.49),
            (adi < 1.32) & (cv2 >= 0.49),
            (adi >= 1.32) & (cv2 < 0.49),
        ]
        choices = ['Smooth', 'Erratic', 'Intermittent']
        pattern = np.select(conds, choices, default='Lumpy')
        return pd.DataFrame({'COD_ITEM': df['COD_ITEM'], 'DEMAND_PATTERN': pattern})
    # fallback vazio
    return pd.DataFrame(columns=['COD_ITEM','DEMAND_PATTERN'])

def append_ml_tabs_to_excel(target_excel: Path, predicoes_csv: Path, avaliacao_csv: Path) -> None:
    if not bool(get_param('integracao.gerar_alias_abas_compat', True)):
        return  # mantém comportamento atual se desabilitado
    # Carrega abas existentes
    with pd.ExcelFile(target_excel) as xls:
        existing = {n: pd.read_excel(xls, sheet_name=n) for n in xls.sheet_names}

    # Carrega CSVs se existirem
    pred_df = pd.read_csv(predicoes_csv) if predicoes_csv.exists() else None
    aval_df = pd.read_csv(avaliacao_csv) if avaliacao_csv.exists() else None

    # Usa INDICADORES para gerar CLASSIFICACAO de compatibilidade
    indicadores = existing.get('INDICADORES', pd.DataFrame())
    classificacao_df = _derive_classificacao_from_indicadores(indicadores)

    alias_map: dict = get_param('integracao.planilha_alias', {'PREVISOES': 'PREDICOES_ML','CLASSIFICACAO': 'INDICADORES'}) or {}
    pred_alias_targets = {
        str(alias)
        for alias, source in alias_map.items()
        if str(source).strip().upper() == 'PREDICOES_ML'
    }
    skip_existing = set(alias_map.keys())
    if pred_df is not None and pred_alias_targets:
        skip_existing.add('PREDICOES_ML')
    if not classificacao_df.empty:
        skip_existing.add('CLASSIFICACAO')

    # Reescreve com abas incrementais (sem remover as existentes)
    with pd.ExcelWriter(target_excel, engine="openpyxl") as writer:
        # 1) Reescreve todas as abas atuais
        for name, df in existing.items():
            if str(name) in skip_existing:
                continue
            df.to_excel(writer, sheet_name=str(name), index=False)
        # 2) Abas incrementais (novas)
        pred_written_as_alias = False
        if pred_df is not None:
            for alias, source in alias_map.items():
                src_name = str(source).strip()
                src_upper = src_name.upper()
                if src_upper == 'PREDICOES_ML':
                    pred_df.to_excel(writer, sheet_name=str(alias), index=False)
                    pred_written_as_alias = True
                else:
                    src_df = existing.get(src_name)
                    if src_df is not None:
                        src_df.to_excel(writer, sheet_name=str(alias), index=False)
        if pred_df is not None and not pred_written_as_alias:
            pred_df.to_excel(writer, sheet_name="PREDICOES_ML", index=False)
        if aval_df is not None:
            aval_df.to_excel(writer, sheet_name="AVALIACAO_ML", index=False)
        if not classificacao_df.empty:
            classificacao_df.to_excel(writer, sheet_name="CLASSIFICACAO", index=False)

def main():
    base = Path("Modulo_Solidos_ML")
    out = base / "orcamento_2026"
    pred = out / "predicoes.csv"
    aval = out / "avaliacao.csv"

    # Atualiza ML_CONSUMO_SOLIDOS.xlsx (se existir em qualquer local)
    for ml_excel in [base / "ML_CONSUMO_SOLIDOS.xlsx",
                     base / "saida_ml" / "ML_CONSUMO_SOLIDOS.xlsx",
                     out / "ML_CONSUMO_SOLIDOS.xlsx"]:
        if ml_excel.exists():
            _logger.info(f"Atualizando {ml_excel}")
            append_ml_tabs_to_excel(ml_excel, pred, aval)

    # Atualiza ORCAMENTO_2026_SOLIDOS_ML.xlsx
    budget_excel = out / "ORCAMENTO_2026_SOLIDOS_ML.xlsx"
    if budget_excel.exists():
        _logger.info(f"Atualizando {budget_excel}")
        append_ml_tabs_to_excel(budget_excel, pred, aval)

if __name__ == "__main__":
    main()

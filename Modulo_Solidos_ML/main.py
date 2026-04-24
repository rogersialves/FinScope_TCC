import sys
from pathlib import Path
# Garante que a raiz do projeto está no sys.path ANTES do import
_current_dir = Path(__file__).resolve().parent
_root = _current_dir.parent  # raiz do workspace: FinScope
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from core.fin_params import get_param
from core.logger import get_logger

# Garante que a raiz (para importar fin_utils no pipeline) esteja no sys.path
_current_dir = Path(__file__).resolve().parent
_root = _current_dir.parent  # raiz do workspace: FinScope
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from Modulo_Solidos_ML.pipeline import build_argparser, run_pipeline, Config
from Modulo_Solidos_ML.budget_projection import BudgetConfig, run_budget_projection

_logger = get_logger(__name__)


def main():
    parser = build_argparser()
    parser.add_argument('--modo', choices=['essencial','fidelidade','inteligente'], default=get_param('orcamento.modo_padrao', 'essencial'),
                        help='Política de orçamento: essencial, fidelidade, inteligente')
    # mantém outros argumentos existentes (margem etc.)
    args = parser.parse_args()

    if args.comando == 'ml':
        run_pipeline(args.input, args.output, Config(horizonte_previsao_meses=args.horizon), args.modo)
    elif args.comando == 'orcamento':
        # Diretório forçado com possibilidade de override via params (sem alterar regra de forçar)
        forced_default = Path(__file__).resolve().parent / 'orcamento_2026'
        forced_dir = Path(get_param('sistema.paths.out_orcamento', str(forced_default)))
        forced_dir.mkdir(parents=True, exist_ok=True)

        # Se margem não informada, usa params; mantém args quando fornecido
        margem_default = float(get_param('orcamento.margem_seguranca_padrao', 0.15))
        margem_cfg = args.margem if hasattr(args, 'margem') and args.margem is not None else margem_default

        cfg = BudgetConfig(
            horizonte_meses=12,
            margem_seguranca_pct=margem_cfg,
            modo_orcamento=args.modo
        )
        aux_default = Path(args.input).parent / get_param('sistema.paths.tab_aux', 'data/TAB_AUX.xlsx')
        aux_path = aux_default

        # Executa sempre (por enquanto, para forçar re-processamento)
        # Futuramente, adicionar lógica de cache inteligente via hash de params.yaml
        excel_path, resumo = run_budget_projection(args.input, forced_dir, aux_path, cfg)
        _logger.info(f"Orçamento 2026 concluído. Saída: {excel_path}")

if __name__ == '__main__':
    main()

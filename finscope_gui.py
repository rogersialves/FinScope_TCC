import copy
import io
import os
import re
import subprocess
import sys
import threading
import tkinter as tk
import unicodedata
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from tkinter import ttk, messagebox
from typing import Any, Dict, Sequence, Tuple

try:
    from ruamel.yaml import YAML  # type: ignore[import]
    from ruamel.yaml.comments import CommentedMap, CommentedSeq  # type: ignore[import]
except ImportError:  # pragma: no cover - dependente de ambiente
    YAML = None  # type: ignore[assignment]
    CommentedMap = dict  # type: ignore[assignment]
    CommentedSeq = list  # type: ignore[assignment]


class _MissingDateEntry:
    """Fallback para informar dependência ausente mantendo interface mínima."""

    def __init__(self, *args, **kwargs) -> None:
        self._raise()

    @staticmethod
    def _raise() -> None:
        raise ImportError(
            "Dependência opcional 'tkcalendar' não encontrada. Instale com 'pip install tkcalendar' "
            "para habilitar os campos de data na interface."
        )

    # Métodos utilizados pelo código; todos apenas informam o erro
    def grid(self, *args, **kwargs) -> None:  # pragma: no cover - fluxo de erro
        self._raise()

    def pack(self, *args, **kwargs) -> None:  # pragma: no cover
        self._raise()

    def place(self, *args, **kwargs) -> None:  # pragma: no cover
        self._raise()

    def set_date(self, *args, **kwargs) -> None:  # pragma: no cover
        self._raise()

    def get_date(self, *args, **kwargs) -> datetime:  # pragma: no cover
        self._raise()
        return datetime.now()


try:
    from tkcalendar import DateEntry  # type: ignore[import]
except ImportError:  # pragma: no cover - dependente de ambiente
    DateEntry = _MissingDateEntry  # type: ignore[misc,assignment]
from core import fin_params
from core.base_historica_builder import build_base_historica
from core.logger import get_logger

_logger = get_logger(__name__)
from Modulo_Relatorios.cbs005 import (
    apply_excel_formatting_FollowUp,
    fetch_data_from_db_FollowUp,
)
from Modulo_Relatorios.excel_utils import (
    apply_consolidated_formatting,
    apply_excel_formatting,
    consolidate_reports,
    fetch_data_from_db_MAN,
)

MAN_OPTIONS = {
    "1": {
        "label": "MAN001 - Data de Liquidacao",
        "procedure": "EXT_MAN001_pedidos_de_compras_dataLiquidacao",
        "needs_dates": True,
        "needs_empresa": False,
    },
    "2": {
        "label": "MAN001 - Data de Pedido",
        "procedure": "EXT_MAN001_Pedidos_de_Compras_dataPedido",
        "needs_dates": True,
        "needs_empresa": False,
    },
    "3": {
        "label": "MAN001 - Data de Ordem de Compra",
        "procedure": "EXT_MAN001_pedidos_de_compras_dataOc",
        "needs_dates": True,
        "needs_empresa": False,
    },
    "4": {
        "label": "MAN002 - Data de Liquidacao",
        "procedure": "EXT_MAN002_valores_liquidos_impostos",
        "needs_dates": True,
        "needs_empresa": True,
    },
    "5": {
        "label": "MAN004 - Data da Reserva",
        "procedure": "EXT_MAN004_Resumo_de_Custos_Rma",
        "needs_dates": True,
        "needs_empresa": True,
    },
    "6": {
        "label": "MAN005 - Data de Lancamentos",
        "procedure": "EXT_MAN005_Lancamentos_Manutencao",
        "needs_dates": True,
        "needs_empresa": True,
    },
    "7": {
        "label": "MAN008 - Saldo Estoque",
        "procedure": "EXT_MAN008_Saldo_Custo_Estoque",
        "needs_dates": False,
        "needs_empresa": True,
    },
    "8": {
        "label": "CBS005 - Follow UP Compras",
        "procedure": "FOLLOWUP",
        "needs_dates": False,
        "needs_empresa": False,
        "is_followup": True,
    },
    "9": {
        "label": "MAN Consolidado",
        "procedure": "CONSOLIDADO",
        "needs_dates": True,
        "needs_empresa": True,
        "is_consolidated": True,
    },
}

MAN_OPTION_MAP = {
    "man_liquidacao": "1",
    "man_pedido": "2",
    "man_oc": "3",
    "man_002": "4",
    "man_004": "5",
    "man_005": "6",
    "man_estoque": "7",
    "man_followup": "8",
    "man_consolidado": "9",
}

CUSTOM_FACTORY_LABEL = "Personalizado (params.yaml)"
_pisco_cfg = fin_params.get_param("piscofins", {}) or {}
_pisco_cod = _pisco_cfg.get("cod_tipo_despesa_default", [])
_pisco_cst = _pisco_cfg.get("cst_pis_default", "")
_pisco_anos = _pisco_cfg.get("anos_default", [])
PISCOFINS_DEFAULT_COD_TIPO_DESPESA = ",".join(str(x) for x in _pisco_cod) if _pisco_cod else ""
PISCOFINS_DEFAULT_CST_PIS = str(_pisco_cst) if _pisco_cst is not None else ""
PISCOFINS_DEFAULT_ANOS = ",".join(str(a) for a in _pisco_anos) if _pisco_anos else ""

EXTERNAL_OPTIONS = {
    "ml_orcamento": {"needs_dates": False, "needs_empresa": False},
    "piscofins": {"needs_dates": False, "needs_empresa": False},
    "analise_estrategias": {"needs_dates": False, "needs_empresa": False},
}


class FinScopeGUI:
    params_path: Path
    export_group_map: Dict[str, str]
    selected_option: tk.StringVar
    is_processing: bool
    _ml_progress_markers: list[Tuple[str, int]]

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("FinScope - Sistema de Orcamento e Relatorios")
        self.root.geometry("900x800")

        self.base_dir = Path(__file__).resolve().parent
        export_root_cfg = fin_params.get_param("exportacao.raiz", "exportacoes") or "exportacoes"
        export_root_path = Path(export_root_cfg)
        if not export_root_path.is_absolute():
            export_root_path = self.base_dir / export_root_path
        self.export_root = export_root_path
        self.export_root.mkdir(parents=True, exist_ok=True)

        self.analysis_dir = self.base_dir / "Modulo_Analise"
        self.analysis_dir.mkdir(parents=True, exist_ok=True)

        self.params_path = self.base_dir / "config" / "params.yaml"

        groups_cfg = fin_params.get_param("exportacao.grupos", {}) or {}
        self.export_group_map: dict[str, str] = {}
        for category, options in groups_cfg.items():
            normalized_category = self._normalize_name(str(category))
            for opt in options or []:
                self.export_group_map[str(opt)] = normalized_category

        self.selected_option = tk.StringVar()
        self.is_processing = False
        self._ml_progress_markers = [
            ("executando pipeline ml", 55),
            ("pipeline ml concluido", 65),
            ("carregando dados historicos", 72),
            ("integrando previsoes ml", 82),
            ("calculando projecao orcamentaria", 90),
            ("validacao do orcamento gerado", 93),
            ("orcamento exportado", 97),
            ("resumo json", 98),
            ("abas de compatibilidade", 99),
        ]
        self._factory_presets: dict[str, dict[str, Any]] = {}
        self._factory_order: list[str] = []
        self.piscofins_cod_var = tk.StringVar(value=PISCOFINS_DEFAULT_COD_TIPO_DESPESA)
        self.piscofins_cst_var = tk.StringVar(value=PISCOFINS_DEFAULT_CST_PIS)
        self.piscofins_fonte_var = tk.StringVar(value="debito")

        self._init_ml_variables()
        self.configure_styles()
        self.build_interface()

    def configure_styles(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        self.bg_color = "#f0f0f0"
        self.accent_color = "#2196F3"
        self.text_color = "#333333"

        style.configure("Custom.TFrame", background=self.bg_color)
        style.configure("Custom.TLabelframe", background=self.bg_color, padding=10)
        style.configure(
            "Custom.TLabelframe.Label",
            background=self.bg_color,
            foreground=self.text_color,
            font=("Segoe UI", 10, "bold"),
        )
        style.configure(
            "Custom.TButton",
            background=self.accent_color,
            foreground="white",
            padding=(20, 10),
            font=("Segoe UI", 10),
        )
        style.map("Custom.TButton", background=[("active", "#1976D2")])
        style.configure(
            "Icon.TButton",
            background=self.bg_color,
            foreground=self.text_color,
            padding=(6, 4),
            font=("Segoe UI Symbol", 12),
        )
        style.map("Icon.TButton", background=[("active", "#d9d9d9")])
        style.configure(
            "Custom.TRadiobutton",
            background=self.bg_color,
            foreground=self.text_color,
            font=("Segoe UI", 9),
        )
        style.configure(
            "Custom.TLabel",
            background=self.bg_color,
            foreground=self.text_color,
            font=("Segoe UI", 9),
        )
        style.configure(
            "Custom.TEntry",
            fieldbackground="white",
            foreground=self.text_color,
            background="white",
        )
        style.configure(
            "Custom.Horizontal.TProgressbar",
            troughcolor=self.bg_color,
            background=self.accent_color,
        )
        style.configure(
            "Comment.TLabel",
            background=self.bg_color,
            foreground="#666666",
            font=("Segoe UI", 8, "italic"),
        )
        style.configure(
            "my.DateEntry",
            fieldbackground="white",
            background="white",
            foreground=self.text_color,
            arrowcolor=self.accent_color,
            font=("Segoe UI", 9),
        )

        self.root.configure(bg=self.bg_color)

    def _reload_factory_presets(self, filtros: dict[str, Any]) -> None:
        presets_cfg = filtros.get("fabricas") if isinstance(filtros, dict) else None
        presets: dict[str, dict[str, Any]] = {}
        order: list[str] = []

        if isinstance(presets_cfg, dict):
            for key, value in presets_cfg.items():
                if not isinstance(value, dict):
                    continue
                key_norm = str(key).strip().lower()
                if not key_norm:
                    continue
                label = str(value.get("label", key)).strip() or key_norm
                cc_start = value.get("cc_start", "")
                cc_end = value.get("cc_end", "")
                cc_excluir_raw = value.get("cc_excluir", [])
                if isinstance(cc_excluir_raw, (list, tuple)):
                    cc_exclude = [str(item).strip() for item in cc_excluir_raw if str(item).strip()]
                elif isinstance(cc_excluir_raw, str):
                    cc_exclude = [token.strip() for token in re.split(r"[,\n;]+", cc_excluir_raw) if token.strip()]
                else:
                    cc_exclude = []
                presets[key_norm] = {
                    "label": label,
                    "cc_start": str(cc_start).strip() if cc_start not in (None, "") else "",
                    "cc_end": str(cc_end).strip() if cc_end not in (None, "") else "",
                    "cc_exclude": cc_exclude,
                }
                order.append(key_norm)

        if "custom" not in presets:
            presets["custom"] = {
                "label": CUSTOM_FACTORY_LABEL,
                "cc_start": "",
                "cc_end": "",
                "cc_exclude": [],
            }
        else:
            custom = presets["custom"]
            custom_label = str(custom.get("label", CUSTOM_FACTORY_LABEL)).strip() or CUSTOM_FACTORY_LABEL
            custom["label"] = custom_label
            custom["cc_start"] = str(custom.get("cc_start", "") or "").strip()
            custom["cc_end"] = str(custom.get("cc_end", "") or "").strip()
            cc_ex = custom.get("cc_exclude", [])
            if isinstance(cc_ex, (list, tuple)):
                custom["cc_exclude"] = [str(item).strip() for item in cc_ex if str(item).strip()]
            elif isinstance(cc_ex, str):
                custom["cc_exclude"] = [token.strip() for token in re.split(r"[,\n;]+", cc_ex) if token.strip()]
            else:
                custom["cc_exclude"] = []

        self._factory_presets = presets
        order_without_custom = [key for key in order if key in presets and key != "custom"]
        for key in presets:
            if key not in order_without_custom and key != "custom":
                order_without_custom.append(key)
        self._factory_order = order_without_custom

    def _load_ml_defaults(self) -> Dict[str, Any]:
        try:
            params = fin_params.load_params()
        except Exception as exc:
            _logger.warning("Falha ao carregar params.yaml, usando valores padrão: %s", exc)
            params = {}

        filtros = params.get("filtros", {}) if isinstance(params, dict) else {}
        orcamento = params.get("orcamento", {}) if isinstance(params, dict) else {}

        self._reload_factory_presets(filtros)

        contas_default = filtros.get("conta_contabil_permitidas", [])
        if isinstance(contas_default, (list, tuple)):
            contas_list = [str(item).strip() for item in contas_default if str(item).strip()]
        elif isinstance(contas_default, str):
            contas_list = [c.strip() for c in re.split(r"[,\n]+", contas_default) if c.strip()]
        else:
            contas_list = []

        cc_range_default = filtros.get("centro_custo_range", [])
        if isinstance(cc_range_default, (list, tuple)) and len(cc_range_default) >= 2:
            cc_start_default = str(cc_range_default[0])
            cc_end_default = str(cc_range_default[1])
        else:
            cc_start_default = ""
            cc_end_default = ""

        cc_excluir_default = filtros.get("centro_custo_excluir", [])
        if isinstance(cc_excluir_default, (list, tuple)):
            cc_excluir_list = [str(item).strip() for item in cc_excluir_default if str(item).strip()]
        elif isinstance(cc_excluir_default, str):
            cc_excluir_list = [c.strip() for c in re.split(r"[,\n;]+", cc_excluir_default) if c.strip()]
        else:
            cc_excluir_list = []

        apply_cc_default = bool(filtros.get("aplicar_filtro_centro_custo", True))

        factory_config = filtros.get("fabrica_selecionada")
        factory_key = str(factory_config).strip().lower() if isinstance(factory_config, str) else ""
        if factory_key not in self._factory_presets:
            factory_key = self._infer_factory_from_defaults(cc_start_default, cc_end_default, cc_excluir_list)

        if factory_key != "custom":
            preset = self._factory_presets[factory_key]
            cc_start_default = preset["cc_start"]
            cc_end_default = preset["cc_end"]
            cc_excluir_list = list(preset["cc_exclude"])
            apply_cc_default = True
        else:
            cc_excluir_list = [str(item).strip() for item in cc_excluir_list if str(item).strip()]

        ano_min_default = filtros.get("ano_minimo_dados", "")
        palavra_default = filtros.get("palavra_parada", "PARADA")

        return {
            "mode": str(orcamento.get("modo_padrao", "inteligente")),
            "apply_conta": bool(filtros.get("aplicar_filtro_conta_contabil", True)),
            "apply_cc": apply_cc_default,
            "apply_ano": bool(filtros.get("aplicar_filtro_ano_minimo", False)),
            "remove_parada": bool(filtros.get("remover_itens_palavra_parada", True)),
            "contas": contas_list,
            "cc_start": cc_start_default,
            "cc_end": cc_end_default,
            "cc_excluir": cc_excluir_list,
            "ano_min": str(ano_min_default) if ano_min_default not in (None, "") else "",
            "palavra": str(palavra_default) if palavra_default is not None else "PARADA",
            "factory": factory_key,
        }

    @staticmethod
    def _infer_factory_from_defaults(
        cc_start: str,
        cc_end: str,
        cc_excluir: Sequence[str],
    ) -> str:
        def _to_int(value: Any) -> int | None:
            try:
                text = str(value).strip()
                if not text:
                    return None
                return int(float(text))
            except (TypeError, ValueError):
                return None

        start_int = _to_int(cc_start)
        end_int = _to_int(cc_end)
        exclude_set = {num for num in (_to_int(item) for item in cc_excluir) if num is not None}

        # Usa valores do params.yaml para identificar a fábrica
        fabricas = fin_params.get_param("filtros.fabricas", {})
        for fab_key, fab_data in fabricas.items():
            fab_start = fab_data.get("cc_start")
            fab_end = fab_data.get("cc_end")
            if start_int == fab_start and end_int == fab_end:
                return fab_key
        return "custom"

    def _init_ml_variables(self) -> None:
        self._ml_defaults = self._load_ml_defaults()

        self.ml_mode_var = tk.StringVar(value=self._ml_defaults["mode"])
        requested_factory = str(self._ml_defaults.get("factory", "")).strip().lower()
        factory_key, factory_data = self._resolve_factory_data(requested_factory)
        self.ml_factory_var = tk.StringVar(value=factory_key)
        self.ml_factory_label_var = tk.StringVar(value=factory_data["label"])
        self.ml_margin_var = tk.StringVar(value="0.0")
        self.ml_apply_conta_var = tk.BooleanVar(value=self._ml_defaults["apply_conta"])
        self.ml_apply_cc_var = tk.BooleanVar(value=self._ml_defaults["apply_cc"])
        self.ml_apply_ano_var = tk.BooleanVar(value=self._ml_defaults["apply_ano"])
        self.ml_remove_parada_var = tk.BooleanVar(value=self._ml_defaults["remove_parada"])
        self.ml_contas_var = tk.StringVar(value=", ".join(self._ml_defaults["contas"]))
        self.ml_cc_start_var = tk.StringVar(value=self._ml_defaults["cc_start"])
        self.ml_cc_end_var = tk.StringVar(value=self._ml_defaults["cc_end"])
        self.ml_cc_exclude_var = tk.StringVar(value=", ".join(self._ml_defaults["cc_excluir"]))
        self.ml_ano_min_var = tk.StringVar(value=self._ml_defaults["ano_min"])
        self.ml_palavra_var = tk.StringVar(value=self._ml_defaults["palavra"])

    def _refresh_ml_variables_from_defaults(self) -> None:
        defaults = self._load_ml_defaults()
        self._ml_defaults = defaults

        if hasattr(self, "ml_mode_var"):
            self.ml_mode_var.set(defaults["mode"])
        requested_factory = str(defaults.get("factory", "")).strip().lower()
        factory_key, factory_data = self._resolve_factory_data(requested_factory)
        if hasattr(self, "ml_factory_var"):
            self.ml_factory_var.set(factory_key)
        if hasattr(self, "ml_factory_label_var"):
            self.ml_factory_label_var.set(factory_data["label"])
        if hasattr(self, "ml_apply_conta_var"):
            self.ml_apply_conta_var.set(defaults["apply_conta"])
        if hasattr(self, "ml_apply_cc_var"):
            self.ml_apply_cc_var.set(defaults["apply_cc"])
        if hasattr(self, "ml_apply_ano_var"):
            self.ml_apply_ano_var.set(defaults["apply_ano"])
        if hasattr(self, "ml_remove_parada_var"):
            self.ml_remove_parada_var.set(defaults["remove_parada"])
        if hasattr(self, "ml_contas_var"):
            self.ml_contas_var.set(", ".join(defaults["contas"]))
        if hasattr(self, "ml_cc_start_var"):
            self.ml_cc_start_var.set(defaults["cc_start"])
        if hasattr(self, "ml_cc_end_var"):
            self.ml_cc_end_var.set(defaults["cc_end"])
        if hasattr(self, "ml_cc_exclude_var"):
            self.ml_cc_exclude_var.set(", ".join(defaults["cc_excluir"]))
        if hasattr(self, "ml_ano_min_var"):
            self.ml_ano_min_var.set(defaults["ano_min"])
        if hasattr(self, "ml_palavra_var"):
            self.ml_palavra_var.set(defaults["palavra"])

        if getattr(self, "_ml_widgets_ready", False):
            self._apply_factory_preset(factory_key)
            self._update_ml_filter_states()
            self._on_ml_mode_selected()

    def _factory_keys_for_ui(self) -> list[str]:
        current = self.ml_factory_var.get() if hasattr(self, "ml_factory_var") else ""
        keys = list(self._factory_order)
        for key in self._factory_presets:
            if key not in keys and key != "custom":
                keys.append(key)
        if current and current not in keys:
            keys.append(current)
        if "custom" in keys and current != "custom":
            # Remove custom from default list unless explicitly needed
            keys = [key for key in keys if key != "custom"]
        if (current == "custom" or current not in keys) and "custom" in self._factory_presets and "custom" not in keys:
            keys.append("custom")
        return keys

    def _resolve_factory_data(self, factory_key: str) -> tuple[str, dict[str, Any]]:
        preset = self._factory_presets.get(factory_key)
        if preset is not None:
            return factory_key, preset

        for key in self._factory_order:
            preset = self._factory_presets.get(key)
            if preset is not None:
                return key, preset

        for key, preset in self._factory_presets.items():
            if key != "custom":
                return key, preset

        custom = self._factory_presets.get("custom")
        if custom is None:
            custom = {"label": CUSTOM_FACTORY_LABEL, "cc_start": "", "cc_end": "", "cc_exclude": []}
            self._factory_presets["custom"] = custom
        return "custom", custom

    def _apply_factory_preset(self, factory_key: str, update_label: bool = True) -> None:
        factory_key, preset = self._resolve_factory_data(factory_key)

        if hasattr(self, "ml_factory_var"):
            self.ml_factory_var.set(factory_key)

        if hasattr(self, "ml_factory_combo"):
            keys_for_ui = self._factory_keys_for_ui()
            labels: list[str] = []
            for key in keys_for_ui:
                data = self._factory_presets.get(key)
                if data is not None:
                    labels.append(data["label"])
                elif key == "custom":
                    labels.append(CUSTOM_FACTORY_LABEL)
            self.ml_factory_combo.configure(values=labels)

        if update_label and hasattr(self, "ml_factory_label_var"):
            label = preset["label"]
            self.ml_factory_label_var.set(label)
            if hasattr(self, "ml_factory_combo"):
                self.ml_factory_combo.set(label)

        if factory_key == "custom":
            if hasattr(self, "ml_apply_cc_var"):
                self.ml_apply_cc_var.set(self._ml_defaults.get("apply_cc", True))
            if getattr(self, "_ml_widgets_ready", False):
                self._update_ml_filter_states()
            return

        if hasattr(self, "ml_apply_cc_var"):
            self.ml_apply_cc_var.set(True)
        if hasattr(self, "ml_cc_start_var"):
            self.ml_cc_start_var.set(str(preset["cc_start"]).strip())
        if hasattr(self, "ml_cc_end_var"):
            self.ml_cc_end_var.set(str(preset["cc_end"]).strip())
        if hasattr(self, "ml_cc_exclude_var"):
            self.ml_cc_exclude_var.set(", ".join(str(item).strip() for item in preset.get("cc_exclude", [])))

        if getattr(self, "_ml_widgets_ready", False):
            self._update_ml_filter_states()

    def _on_factory_selected(self, _event: tk.Event | None = None) -> None:
        if not hasattr(self, "ml_factory_label_var"):
            return
        label = self.ml_factory_label_var.get()
        key = next(
            (factory_key for factory_key, data in self._factory_presets.items() if data["label"] == label),
            None,
        )
        if key is None:
            return
        self._apply_factory_preset(key, update_label=False)

    def build_interface(self) -> None:
        main_frame = ttk.Frame(self.root, padding="10", style="Custom.TFrame")
        main_frame.pack(fill="both", expand=True)

        relatorios_frame = ttk.LabelFrame(
            main_frame,
            text="Relatorios",
            style="Custom.TLabelframe",
        )
        relatorios_frame.pack(padx=10, pady=10, fill="x")

        report_columns = [
            [
                ("ml_orcamento", "Gerar Orcamento (Machine Learning)"),
                ("piscofins", "Relatorio de Creditos PIS/COFINS"),
                ("analise_estrategias", "Analise Estrategias Consumo"),
                ("man_consolidado", MAN_OPTIONS["9"]["label"]),
            ],
            [
                ("man_liquidacao", MAN_OPTIONS["1"]["label"]),
                ("man_pedido", MAN_OPTIONS["2"]["label"]),
                ("man_oc", MAN_OPTIONS["3"]["label"]),
                ("man_followup", MAN_OPTIONS["8"]["label"]),
            ],
            [
                ("man_004", MAN_OPTIONS["5"]["label"]),
                ("man_estoque", MAN_OPTIONS["7"]["label"]),
                ("man_002", MAN_OPTIONS["4"]["label"]),
                ("man_005", MAN_OPTIONS["6"]["label"]),
            ],
        ]

        for col_index, column_options in enumerate(report_columns):
            for row_index, (value, texto) in enumerate(column_options):
                ttk.Radiobutton(
                    relatorios_frame,
                    text=texto,
                    variable=self.selected_option,
                    value=value,
                    command=self.update_fields,
                    style="Custom.TRadiobutton",
                ).grid(row=row_index, column=col_index, sticky="w", padx=10, pady=5)

        params_frame = ttk.LabelFrame(
            main_frame,
            text="Parametros de Relatorio",
            style="Custom.TLabelframe",
        )
        params_frame.pack(padx=10, pady=10, fill="x")

        self.inputs_frame = ttk.Frame(params_frame, style="Custom.TFrame")
        self.inputs_frame.pack(fill="x", padx=5, pady=5)
        for idx in range(10):
            self.inputs_frame.columnconfigure(idx, weight=1 if idx % 2 == 1 else 0)

        self.label_inicio = ttk.Label(
            self.inputs_frame,
            text="Data de Inicio:",
            style="Custom.TLabel",
        )
        self.entry_inicio = DateEntry(
            self.inputs_frame,
            width=18,
            background="darkblue",
            foreground="white",
            borderwidth=2,
            style="my.DateEntry",
            date_pattern="dd/mm/yyyy",
            locale="pt_BR",
        )
        self.label_fim = ttk.Label(
            self.inputs_frame,
            text="Data de Fim:",
            style="Custom.TLabel",
        )
        self.entry_fim = DateEntry(
            self.inputs_frame,
            width=18,
            background="darkblue",
            foreground="white",
            borderwidth=2,
            style="my.DateEntry",
            date_pattern="dd/mm/yyyy",
            locale="pt_BR",
        )
        self.label_empresa = ttk.Label(
            self.inputs_frame,
            text="Empresas (ex: 02,30):",
            style="Custom.TLabel",
        )
        self.entry_empresa = ttk.Entry(self.inputs_frame, width=20, style="Custom.TEntry")
        self.label_piscofins_fonte = ttk.Label(
            self.inputs_frame,
            text="Fonte PIS/COFINS:",
            style="Custom.TLabel",
        )
        self.radio_piscofins_debito = ttk.Radiobutton(
            self.inputs_frame,
            text="Débito Direto",
            variable=self.piscofins_fonte_var,
            value="debito",
            style="Custom.TRadiobutton",
            command=self.update_fields,
        )
        self.radio_piscofins_rma = ttk.Radiobutton(
            self.inputs_frame,
            text="Reservas Almox",
            variable=self.piscofins_fonte_var,
            value="rma",
            style="Custom.TRadiobutton",
            command=self.update_fields,
        )
        self.label_piscofins_cod = ttk.Label(
            self.inputs_frame,
            text="cod_tipo_despesa:",
            style="Custom.TLabel",
        )
        self.entry_piscofins_cod = ttk.Entry(
            self.inputs_frame,
            width=50,
            textvariable=self.piscofins_cod_var,
            style="Custom.TEntry",
        )
        self.label_piscofins_cst = ttk.Label(
            self.inputs_frame,
            text="cst_pis:",
            style="Custom.TLabel",
        )
        self.entry_piscofins_cst = ttk.Entry(
            self.inputs_frame,
            width=10,
            textvariable=self.piscofins_cst_var,
            style="Custom.TEntry",
        )

        history_frame = ttk.LabelFrame(
            main_frame,
            text="Historico de Execucao",
            style="Custom.TLabelframe",
        )
        history_frame.pack(padx=10, pady=10, fill="both", expand=True)

        scroll = ttk.Scrollbar(history_frame)
        scroll.pack(side="right", fill="y")

        self.text_history = tk.Text(
            history_frame,
            height=10,
            state="disabled",
            font=("Consolas", 9),
            background="white",
            yscrollcommand=scroll.set,
        )
        self.text_history.pack(fill="both", expand=True, padx=5, pady=5)
        scroll.config(command=self.text_history.yview)

        bottom_frame = ttk.Frame(main_frame, style="Custom.TFrame")
        bottom_frame.pack(fill="x", padx=10, pady=10)

        self.progress = ttk.Progressbar(
            bottom_frame,
            orient="horizontal",
            mode="determinate",
            maximum=100,
            style="Custom.Horizontal.TProgressbar",
        )
        self.progress.pack(side="left", fill="x", expand=True, padx=(0, 10))
        self.progress.configure(value=0)

        self.btn_processar = ttk.Button(
            bottom_frame,
            text="Processar",
            command=self.processar,
            style="Custom.TButton",
        )
        self.btn_processar.pack(side="right")

        self._create_ml_widgets()

    def _create_ml_widgets(self) -> None:
        if getattr(self, "_ml_widgets_ready", False):
            return
        self._ml_widgets_ready = True

        self.ml_mode_label = ttk.Label(
            self.inputs_frame,
            text="Modo de Orcamento:",
            style="Custom.TLabel",
        )
        self.ml_mode_combo = ttk.Combobox(
            self.inputs_frame,
            values=["inteligente", "essencial", "fidelidade"],
            textvariable=self.ml_mode_var,
            state="readonly",
        )
        self.ml_mode_combo.bind("<<ComboboxSelected>>", self._on_ml_mode_selected)

        factory_keys = self._factory_keys_for_ui()
        factory_labels: list[str] = []
        for key in factory_keys:
            data = self._factory_presets.get(key)
            if data is not None:
                factory_labels.append(data["label"])
            elif key == "custom":
                factory_labels.append(CUSTOM_FACTORY_LABEL)
        self.ml_factory_label = ttk.Label(
            self.inputs_frame,
            text="Fabrica:",
            style="Custom.TLabel",
        )
        self.ml_factory_combo = ttk.Combobox(
            self.inputs_frame,
            values=factory_labels,
            textvariable=self.ml_factory_label_var,
            state="readonly",
        )
        self.ml_factory_combo.bind("<<ComboboxSelected>>", self._on_factory_selected)

        self.ml_margin_label = ttk.Label(
            self.inputs_frame,
            text="Margem (%):",
            style="Custom.TLabel",
        )
        self.ml_margin_entry = ttk.Entry(
            self.inputs_frame,
            width=10,
            textvariable=self.ml_margin_var,
            style="Custom.TEntry",
        )

        self.ml_params_btn = ttk.Button(
            self.inputs_frame,
            text="⚙",
            width=3,
            style="Icon.TButton",
            command=lambda: self._open_params_popup("ml"),
        )
        self.orc_params_btn = ttk.Button(
            self.inputs_frame,
            text="📈",
            width=3,
            style="Icon.TButton",
            command=lambda: self._open_params_popup("orcamento"),
        )

        self.ml_filters_frame = ttk.LabelFrame(
            self.inputs_frame,
            text="Filtros (params.yaml)",
            style="Custom.TLabelframe",
        )
        for idx in range(3):
            self.ml_filters_frame.columnconfigure(idx, weight=1 if idx >= 2 else 0)

        self.ml_chk_conta = ttk.Checkbutton(
            self.ml_filters_frame,
            text="Aplicar filtro Conta Contabil",
            variable=self.ml_apply_conta_var,
            command=self._update_ml_filter_states,
        )
        self.ml_contas_label = ttk.Label(
            self.ml_filters_frame,
            text="Contas permitidas:",
            style="Custom.TLabel",
        )
        self.ml_contas_entry = ttk.Entry(
            self.ml_filters_frame,
            textvariable=self.ml_contas_var,
            width=50,
            style="Custom.TEntry",
        )
        self.ml_contas_hint = ttk.Label(
            self.ml_filters_frame,
            text="Separe por virgula",
            style="Custom.TLabel",
        )

        self.ml_chk_ano = ttk.Checkbutton(
            self.ml_filters_frame,
            text="Aplicar corte por ano minimo",
            variable=self.ml_apply_ano_var,
            command=self._update_ml_filter_states,
        )
        self.ml_ano_label = ttk.Label(
            self.ml_filters_frame,
            text="Ano minimo:",
            style="Custom.TLabel",
        )
        self.ml_ano_entry = ttk.Entry(
            self.ml_filters_frame,
            textvariable=self.ml_ano_min_var,
            width=8,
            style="Custom.TEntry",
        )

        self._apply_factory_preset(self.ml_factory_var.get())
        self._update_ml_filter_states()
        self._on_ml_mode_selected()
    def _show_ml_inputs(self) -> None:
        self._create_ml_widgets()

        self.ml_mode_label.grid(row=0, column=0, sticky="w", padx=5, pady=(0, 5))
        self.ml_mode_combo.grid(row=0, column=1, sticky="ew", padx=5, pady=(0, 5))
        self.ml_factory_label.grid(row=0, column=2, sticky="e", padx=5, pady=(0, 5))
        self.ml_factory_combo.grid(row=0, column=3, sticky="ew", padx=5, pady=(0, 5))
        self.ml_margin_label.grid(row=0, column=4, sticky="e", padx=5, pady=(0, 5))
        self.ml_margin_entry.grid(row=0, column=5, sticky="w", padx=(0, 5), pady=(0, 5))
        self.ml_params_btn.grid(row=0, column=6, sticky="ew", padx=(5, 0), pady=(0, 5))
        self.orc_params_btn.grid(row=0, column=7, sticky="ew", padx=(5, 0), pady=(0, 5))

        self.ml_filters_frame.grid(row=1, column=0, columnspan=8, sticky="nsew", padx=5)

        # Inside filters frame
        self.ml_chk_conta.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.ml_contas_label.grid(row=0, column=1, sticky="e", padx=(10, 5), pady=5)
        self.ml_contas_entry.grid(row=0, column=2, columnspan=1, sticky="ew", padx=(0, 5), pady=5)
        self.ml_contas_hint.grid(row=1, column=2, sticky="w", padx=(0, 5), pady=(0, 5))

        self.ml_chk_ano.grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.ml_ano_label.grid(row=2, column=1, sticky="e", padx=(10, 5), pady=5)
        self.ml_ano_entry.grid(row=2, column=2, sticky="w", padx=(0, 5), pady=5)

        self._update_ml_filter_states()

    def _set_progress_value(self, value: int) -> None:
        if self.progress["mode"] != "determinate":
            self.progress.stop()
            self.progress.configure(mode="determinate", maximum=100)
        self.progress.configure(value=value)

    def _start_progress_pulse(self) -> None:
        self.progress.stop()
        self.progress.configure(mode="indeterminate")
        self.progress.start(10)

    def _stop_progress_pulse(self) -> None:
        self.progress.stop()
        if self.progress["mode"] != "determinate":
            self.progress.configure(mode="determinate", maximum=100)

    @staticmethod
    def _normalize_name(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = normalized.encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", normalized)
        normalized = normalized.strip("_")
        return normalized or "outros"

    def _normalize_export_key(self, option: str) -> str:
        return self._normalize_name(option)

    def _get_output_directory(self, option: str) -> Path:
        category = self.export_group_map.get(option, "Outros")
        category_dir = self.export_root / self._normalize_name(category)
        category_dir.mkdir(parents=True, exist_ok=True)
        export_dir = category_dir / self._normalize_export_key(option)
        export_dir.mkdir(parents=True, exist_ok=True)
        return export_dir

    def _handle_ml_stream_line(self, line: str) -> None:
        trimmed = line.strip()
        if not trimmed:
            return
        self.append_history(trimmed)
        self._update_ml_progress_from_log(trimmed)

    def _update_ml_progress_from_log(self, line: str) -> None:
        normalized = unicodedata.normalize("NFKD", line)
        normalized = normalized.encode("ascii", "ignore").decode("ascii").lower()
        for marker, value in self._ml_progress_markers:
            if marker in normalized:
                try:
                    current = float(self.progress["value"])
                except (TypeError, tk.TclError):
                    current = 0.0
                if value > current:
                    self._stop_progress_pulse()
                    self._set_progress_value(value)
                break

    def _hide_ml_inputs(self) -> None:
        if not getattr(self, "_ml_widgets_ready", False):
            return
        widgets = [
            self.ml_mode_label,
            self.ml_mode_combo,
            self.ml_factory_label,
            self.ml_factory_combo,
            self.ml_margin_label,
            self.ml_margin_entry,
            self.ml_params_btn,
            self.orc_params_btn,
            self.ml_filters_frame,
        ]
        for widget in widgets:
            widget.grid_remove()

    def _on_ml_mode_selected(self, _event: tk.Event | None = None) -> None:
        has_mode = bool(self.ml_mode_var.get())
        state = "normal" if has_mode else "disabled"
        if hasattr(self, "ml_margin_entry"):
            self.ml_margin_entry.configure(state=state)
        if has_mode and not self.ml_margin_var.get():
            self.ml_margin_var.set("0.0")

    def _update_ml_filter_states(self) -> None:
        if not getattr(self, "_ml_widgets_ready", False):
            return
        state_conta = "normal" if self.ml_apply_conta_var.get() else "disabled"
        if hasattr(self, "ml_contas_entry"):
            self.ml_contas_entry.configure(state=state_conta)

        state_ano = "normal" if self.ml_apply_ano_var.get() else "disabled"
        if hasattr(self, "ml_ano_entry"):
            self.ml_ano_entry.configure(state=state_ano)

    def _create_yaml_loader(self) -> Any:
        if YAML is None:
            raise RuntimeError("ruamel.yaml não está disponível nesta instalação.")
        yaml_loader = YAML()
        yaml_loader.preserve_quotes = True
        yaml_loader.default_flow_style = False
        yaml_loader.width = 120
        yaml_loader.indent(mapping=2, sequence=4, offset=2)
        return yaml_loader

    def _extract_yaml_comment(self, parent: Any, key: Any) -> str:
        if not hasattr(parent, "ca") or not getattr(parent, "ca", None):
            return ""
        comment_entry = parent.ca.items.get(key) if hasattr(parent.ca, "items") else None
        if not comment_entry:
            return ""
        parts: list[str] = []
        for token in comment_entry:
            if token is None:
                continue
            value = getattr(token, "value", "")
            if not value:
                continue
            cleaned = value.strip()
            cleaned = cleaned.lstrip("# ")
            cleaned = cleaned.rstrip("# ")
            cleaned = cleaned.strip()
            if cleaned:
                parts.append(cleaned)
        return "\n".join(parts)

    def _dump_yaml_fragment(self, value: Any) -> str:
        fragment_yaml = self._create_yaml_loader()
        buffer = io.StringIO()
        fragment_yaml.dump(value, buffer)
        return buffer.getvalue().strip()

    @staticmethod
    def _format_scalar(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @staticmethod
    def _coerce_scalar_value(raw: str, original_type: type) -> Any:
        text = raw.strip()
        if text == "":
            return "" if original_type is str else None
        if original_type in {bool}:
            normalized = text.lower()
            return normalized in {"1", "true", "yes", "sim", "on"}
        if original_type in {int}:
            return int(float(text))
        if original_type in {float}:
            return float(text.replace(",", "."))
        return text

    def _build_params_form(
        self,
        container: Any,
        data: Any,
        fields: list[Dict[str, Any]],
        path: Tuple[str, ...],
    ) -> None:
        if isinstance(data, (dict, CommentedMap)):
            for key, value in data.items():
                current_path = path + (str(key),)
                comment_text = self._extract_yaml_comment(data, key)

                if isinstance(value, (dict, CommentedMap)):
                    group_frame = ttk.LabelFrame(
                        container,
                        text=str(key),
                        style="Custom.TLabelframe",
                    )
                    group_frame.pack(fill="x", padx=10, pady=(5, 10))
                    if comment_text:
                        ttk.Label(
                            group_frame,
                            text=comment_text,
                            style="Comment.TLabel",
                            wraplength=460,
                            justify="left",
                        ).pack(anchor="w", padx=5, pady=(0, 5))
                    self._build_params_form(group_frame, value, fields, current_path)
                    continue

                row_frame = ttk.Frame(container, style="Custom.TFrame")
                row_frame.pack(fill="x", padx=10, pady=(5, 10))

                ttk.Label(row_frame, text=str(key), style="Custom.TLabel").pack(anchor="w")
                if comment_text:
                    ttk.Label(
                        row_frame,
                        text=comment_text,
                        style="Comment.TLabel",
                        wraplength=460,
                        justify="left",
                    ).pack(anchor="w", pady=(0, 4))

                if isinstance(value, (list, tuple, CommentedSeq)):
                    text_widget = tk.Text(row_frame, height=max(3, min(12, len(value) + 1)), font=("Consolas", 9))
                    text_widget.pack(fill="x", expand=True)
                    text_widget.insert("1.0", self._dump_yaml_fragment(value))
                    fields.append(
                        {
                            "path": current_path,
                            "widget": text_widget,
                            "kind": "yaml",
                            "value_type": "sequence",
                        }
                    )
                else:
                    entry = ttk.Entry(row_frame, width=40, style="Custom.TEntry")
                    entry.pack(fill="x")
                    entry.insert(0, self._format_scalar(value))
                    fields.append(
                        {
                            "path": current_path,
                            "widget": entry,
                            "kind": "scalar",
                            "original_type": type(value),
                        }
                    )
        else:
            # Non dict root fallback: present as YAML chunk
            row_frame = ttk.Frame(container, style="Custom.TFrame")
            row_frame.pack(fill="both", expand=True, padx=10, pady=10)
            text_widget = tk.Text(row_frame, height=10, font=("Consolas", 9))
            text_widget.pack(fill="both", expand=True)
            text_widget.insert("1.0", self._dump_yaml_fragment(data))
            fields.append(
                {
                    "path": path,
                    "widget": text_widget,
                    "kind": "yaml",
                    "value_type": "mapping",
                }
            )

    def _apply_yaml_field_updates(
        self,
        root: Any,
        fields: Sequence[Dict[str, Any]],
    ) -> None:
        fragment_loader = self._create_yaml_loader()

        for field in fields:
            path = field["path"]
            if not path:
                continue

            parent = root
            for key in path[:-1]:
                parent = parent[key]

            key = path[-1]
            kind = field.get("kind")

            if kind == "scalar":
                entry: ttk.Entry = field["widget"]
                text = entry.get()
                original_type: type = field.get("original_type", str)
                try:
                    new_value = self._coerce_scalar_value(text, original_type)
                except ValueError as exc:
                    raise ValueError(f"Valor inválido para '{' > '.join(path)}': {exc}") from exc
                parent[key] = new_value
            elif kind == "yaml":
                text_widget: tk.Text = field["widget"]
                raw_text = text_widget.get("1.0", "end").strip()
                value_type = field.get("value_type")
                if not raw_text:
                    if value_type == "sequence":
                        parent[key] = CommentedSeq()
                    else:
                        parent[key] = CommentedMap()
                    continue
                try:
                    new_value = fragment_loader.load(raw_text)
                except Exception as exc:
                    raise ValueError(
                        f"Nao foi possivel interpretar o conteudo de '{' > '.join(path)}' como YAML: {exc}"
                    ) from exc
                parent[key] = new_value

    def _open_params_popup(self, section: str) -> None:
        if YAML is None:
            messagebox.showerror(
                "Dependência ausente",
                "Instale o pacote 'ruamel.yaml' para editar os parâmetros diretamente pela interface.",
            )
            return

        params_file = self.params_path
        if not params_file.exists():
            messagebox.showerror(
                "Arquivo não encontrado",
                f"Não foi possível localizar o arquivo de configuração esperado em '{params_file}'.",
            )
            return

        yaml_loader = self._create_yaml_loader()
        try:
            with params_file.open("r", encoding="utf-8") as stream:
                config = yaml_loader.load(stream) or CommentedMap()
        except Exception as exc:
            messagebox.showerror(
                "Erro ao ler parâmetros",
                f"Falha ao carregar '{params_file.name}': {exc}",
            )
            return

        if not isinstance(config, CommentedMap):
            config = CommentedMap(config)

        section_data = config.get(section)
        if section_data is None:
            messagebox.showinfo(
                "Seção não encontrada",
                f"A seção '{section}' não foi localizada em '{params_file.name}'.",
            )
            return

        title_map = {
            "ml": "Parâmetros de Machine Learning",
            "orcamento": "Parâmetros de Orçamento",
        }

        popup = tk.Toplevel(self.root)
        popup.title(title_map.get(section, f"Parâmetros: {section}"))
        popup.geometry("640x560")
        popup.transient(self.root)
        popup.grab_set()

        if section == "ml":
            info_text = (
                f"Modo atual: {self.ml_mode_var.get().upper()} | Ajuste os campos e clique em Salvar para atualizar o params.yaml."
            )
        else:
            info_text = (
                "Atualize os parâmetros de orçamento e clique em Salvar para gravar as alterações no params.yaml."
            )

        ttk.Label(popup, text=info_text, style="Custom.TLabel", wraplength=580, justify="left").pack(
            anchor="w", padx=10, pady=(10, 5)
        )

        canvas = tk.Canvas(popup, borderwidth=0, highlightthickness=0, background=self.bg_color)
        canvas.pack(side="left", fill="both", expand=True, padx=(10, 0), pady=(0, 10))

        scrollbar = ttk.Scrollbar(popup, orient="vertical", command=canvas.yview)
        scrollbar.pack(side="right", fill="y", padx=(0, 10), pady=(0, 10))

        canvas.configure(yscrollcommand=scrollbar.set)

        form_frame = ttk.Frame(canvas, style="Custom.TFrame")
        window_id = canvas.create_window((0, 0), window=form_frame, anchor="nw")

        def _configure_scroll_region(_event: tk.Event) -> None:
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _stretch_inner_width(event: tk.Event) -> None:
            canvas.itemconfigure(window_id, width=event.width)

        def _on_mousewheel(event: tk.Event) -> str:
            delta = getattr(event, "delta", 0)
            if delta:
                canvas.yview_scroll(-int(delta / 120), "units")
            else:
                num = getattr(event, "num", None)
                if num == 4:
                    canvas.yview_scroll(-1, "units")
                elif num == 5:
                    canvas.yview_scroll(1, "units")
            return "break"

        def _bind_scroll_events(_event: tk.Event | None = None) -> None:
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
            canvas.bind_all("<Button-4>", _on_mousewheel)
            canvas.bind_all("<Button-5>", _on_mousewheel)

        def _unbind_scroll_events(_event: tk.Event | None = None) -> None:
            canvas.unbind_all("<MouseWheel>")
            canvas.unbind_all("<Button-4>")
            canvas.unbind_all("<Button-5>")

        form_frame.bind("<Configure>", _configure_scroll_region)
        form_frame.bind("<Enter>", _bind_scroll_events)
        form_frame.bind("<Leave>", _unbind_scroll_events)
        canvas.bind("<Configure>", _stretch_inner_width)
        popup.bind("<Destroy>", _unbind_scroll_events)

        _bind_scroll_events()

        fields: list[Dict[str, Any]] = []
        self._build_params_form(form_frame, section_data, fields, (section,))

        button_frame = ttk.Frame(form_frame, style="Custom.TFrame")
        button_frame.pack(fill="x", padx=10, pady=(10, 10))

        def _save_and_close() -> None:
            try:
                self._apply_yaml_field_updates(config, fields)
            except ValueError as exc:
                messagebox.showerror("Valor inválido", str(exc))
                return

            try:
                with params_file.open("w", encoding="utf-8") as stream:
                    yaml_loader.dump(config, stream)
            except Exception as exc:
                messagebox.showerror(
                    "Erro ao salvar",
                    f"Não foi possível gravar as alterações em '{params_file.name}': {exc}",
                )
                return

            fin_params.load_params(refresh=True)
            self._refresh_ml_variables_from_defaults()
            messagebox.showinfo("Parâmetros atualizados", "Arquivo params.yaml atualizado com sucesso.")
            popup.destroy()

        ttk.Button(button_frame, text="Cancelar", command=popup.destroy, style="Custom.TButton").pack(
            side="right", padx=(0, 10)
        )
        ttk.Button(button_frame, text="Salvar", command=_save_and_close, style="Custom.TButton").pack(
            side="right"
        )

    def _collect_ml_parameters(self) -> dict:
        mode = self.ml_mode_var.get().strip().lower()
        if mode not in {"inteligente", "essencial", "fidelidade"}:
            raise ValueError("Selecione o modo de orcamento.")

        margin_text = self.ml_margin_var.get().strip().replace(",", ".")
        try:
            margin = float(margin_text) if margin_text else 0.0
        except ValueError as exc:
            raise ValueError("Margem invalida. Utilize formato decimal (ex: 0.15).") from exc
        if margin > 1.0:
            margin = margin / 100.0
        if margin < 0:
            raise ValueError("Margem nao pode ser negativa.")

        contas_text = self.ml_contas_var.get()
        contas = [
            item.strip().upper()
            for item in re.split(r"[,\n;]+", contas_text)
            if item.strip()
        ]

        cc_start = self.ml_cc_start_var.get().strip()
        cc_end = self.ml_cc_end_var.get().strip()
        if self.ml_apply_cc_var.get():
            if not cc_start or not cc_end:
                raise ValueError("Informe o intervalo completo de centro de custo.")
            try:
                cc_start_int = int(cc_start)
                cc_end_int = int(cc_end)
            except ValueError as exc:
                raise ValueError("Centro de custo deve ser numerico.") from exc
            if cc_start_int > cc_end_int:
                raise ValueError("Intervalo de centro de custo invalido (inicio > fim).")
            cc_range = [cc_start_int, cc_end_int]

            cc_excluir_text = self.ml_cc_exclude_var.get()
            cc_excluir: list[int] = []
            for item in re.split(r"[,\n;]+", cc_excluir_text):
                token = item.strip()
                if not token:
                    continue
                try:
                    cc_excluir.append(int(token))
                except ValueError as exc:
                    raise ValueError(f"Centro de custo invalido para exclusao: '{token}'.") from exc
        else:
            cc_range = [cc_start, cc_end] if cc_start and cc_end else []
            cc_excluir = []

        if self.ml_apply_ano_var.get():
            ano_text = self.ml_ano_min_var.get().strip()
            if not ano_text:
                raise ValueError("Informe o ano minimo para aplicar o filtro.")
            try:
                ano_min = int(float(ano_text))
            except ValueError as exc:
                raise ValueError("Ano minimo deve ser numerico.") from exc
        else:
            ano_min = self.ml_ano_min_var.get().strip()

        palavra = self.ml_palavra_var.get().strip().upper() or "PARADA"

        return {
            "modo": mode,
            "margem": margin,
            "filtros": {
                "aplicar_filtro_conta_contabil": bool(self.ml_apply_conta_var.get()),
                "conta_contabil_permitidas": contas,
                "aplicar_filtro_centro_custo": bool(self.ml_apply_cc_var.get()),
                "centro_custo_range": cc_range,
                "centro_custo_excluir": cc_excluir,
                "aplicar_filtro_ano_minimo": bool(self.ml_apply_ano_var.get()),
                "ano_minimo_dados": ano_min,
                "remover_itens_palavra_parada": bool(self.ml_remove_parada_var.get()),
                "palavra_parada": palavra,
                "fabrica_selecionada": self.ml_factory_var.get(),
            },
        }

    def _run_ml_orcamento(self) -> None:
        params = self._collect_ml_parameters()

        self.root.after(0, lambda: self.append_history("[INFO] Preparando pipeline de orcamento..."))
        self.root.after(0, lambda: self._set_progress_value(20))

        base_cache = copy.deepcopy(fin_params.load_params(refresh=True) or {})
        overrides = copy.deepcopy(base_cache)

        filtros_cfg = overrides.setdefault("filtros", {})
        filtros_cfg["aplicar_filtro_conta_contabil"] = params["filtros"]["aplicar_filtro_conta_contabil"]
        if params["filtros"]["conta_contabil_permitidas"]:
            filtros_cfg["conta_contabil_permitidas"] = params["filtros"]["conta_contabil_permitidas"]
        filtros_cfg["aplicar_filtro_centro_custo"] = params["filtros"]["aplicar_filtro_centro_custo"]
        if params["filtros"]["centro_custo_range"]:
            filtros_cfg["centro_custo_range"] = params["filtros"]["centro_custo_range"]
        if params["filtros"]["centro_custo_excluir"]:
            filtros_cfg["centro_custo_excluir"] = params["filtros"]["centro_custo_excluir"]
        else:
            filtros_cfg.pop("centro_custo_excluir", None)
        filtros_cfg["aplicar_filtro_ano_minimo"] = params["filtros"]["aplicar_filtro_ano_minimo"]
        filtros_cfg["ano_minimo_dados"] = params["filtros"]["ano_minimo_dados"]
        filtros_cfg["remover_itens_palavra_parada"] = params["filtros"]["remover_itens_palavra_parada"]
        filtros_cfg["palavra_parada"] = params["filtros"]["palavra_parada"]

        orc_cfg = overrides.setdefault("orcamento", {})
        orc_cfg["modo_padrao"] = params["modo"]
        orc_cfg["margem_seguranca_padrao"] = params["margem"]

        fin_params._CACHE = overrides

        root_dir = self.base_dir
        input_path_cfg = fin_params.get_param("sistema.paths.historico", "data/BASE_HISTORICA.xlsx")
        input_path = Path(input_path_cfg)
        if not input_path.is_absolute():
            input_path = root_dir / input_path

        if not input_path.exists():
            self.root.after(0, lambda: self.append_history("[INFO] BASE_HISTORICA.xlsx não encontrada. Gerando a partir dos consolidados..."))
            source_cfg = fin_params.get_param(
                "exportacao.relatorios.man_consolidado",
                "exportacoes/Relatorios_MAN/man_consolidado",
            )
            source_dir = Path(source_cfg)
            if not source_dir.is_absolute():
                source_dir = root_dir / source_dir
            if not source_dir.exists():
                self.root.after(
                    0,
                    lambda: self._report_processing_error(
                        f"Pasta de consolidados não encontrada: {source_dir}"
                    ),
                )
                return
            try:
                build_base_historica(source_dir=source_dir, target_file=input_path)
                self.root.after(0, lambda: self.append_history("[OK] BASE_HISTORICA.xlsx gerada com sucesso."))
            except Exception as exc:  # pragma: no cover - fluxo GUI
                self.root.after(
                    0,
                    lambda msg=str(exc): self._report_processing_error(
                        f"Falha ao gerar BASE_HISTORICA.xlsx automaticamente: {msg}"
                    ),
                )
                return

        default_output_dir = self._get_output_directory("ml_orcamento")
        output_dir_cfg = fin_params.get_param("sistema.paths.out_orcamento", str(default_output_dir))
        output_dir = Path(output_dir_cfg)
        if not output_dir.is_absolute():
            output_dir = root_dir / output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        aux_path_cfg = fin_params.get_param("sistema.paths.tab_aux", "data/TAB_AUX.xlsx")
        aux_path = Path(aux_path_cfg)
        if not aux_path.is_absolute():
            aux_path = root_dir / aux_path

        try:
            from Modulo_Solidos_ML.budget_projection import BudgetConfig, run_budget_projection

            self.root.after(0, lambda: self.append_history(f"[INFO] Modo selecionado: {params['modo']}"))
            self.root.after(0, lambda: self.append_history(f"[INFO] Margem aplicada: {params['margem']:.2%}"))
            self.root.after(0, lambda: self._set_progress_value(45))
            self.root.after(0, self._start_progress_pulse)

            horizon = fin_params.get_param("ml.horizon_meses_default", 12)
            try:
                horizon_int = int(horizon)
            except (TypeError, ValueError):
                horizon_int = 12

            ano_orcamento = fin_params.get_param("orcamento.ano_orcamento", 2026)
            try:
                ano_int = int(ano_orcamento)
            except (TypeError, ValueError):
                ano_int = 2026

            inflacao = fin_params.get_param("orcamento.ajuste_inflacao_anual", 0.035)
            try:
                inflacao_float = float(inflacao)
            except (TypeError, ValueError):
                inflacao_float = 0.035

            budget_cfg = BudgetConfig(
                ano_orcamento=ano_int,
                margem_seguranca_pct=params["margem"],
                ajuste_inflacao_anual=inflacao_float,
                horizonte_meses=horizon_int,
                modo_orcamento=params["modo"],
            )

            class _HistoryStream(io.StringIO):
                def __init__(self, gui: "FinScopeGUI") -> None:
                    super().__init__()
                    self.gui = gui
                    self._buffer = ""

                def write(self, text: str) -> int:
                    if not text:
                        return 0
                    self._buffer += text
                    while "\n" in self._buffer:
                        raw_line, self._buffer = self._buffer.split("\n", 1)
                        line = raw_line.rstrip("\r")
                        if line.strip():
                            self.gui.root.after(0, lambda l=line: self.gui._handle_ml_stream_line(l))
                    return len(text)

                def flush(self) -> None:
                    if self._buffer.strip():
                        line = self._buffer.rstrip("\r")
                        self.gui.root.after(0, lambda l=line: self.gui._handle_ml_stream_line(l))
                    self._buffer = ""

            stream = _HistoryStream(self)
            excel_path: Path | None = None
            resumo: dict | None = None
            success = False
            try:
                with redirect_stdout(stream), redirect_stderr(stream):
                    excel_path, resumo = run_budget_projection(
                        input_path,
                        output_dir,
                        aux_path,
                        budget_cfg,
                    )
                success = True
            finally:
                stream.flush()

            if success and excel_path is not None:
                self.root.after(
                    0,
                    lambda path=str(excel_path): self.append_history(f"[SUCESSO] Orcamento salvo em: {path}"),
                )
                if resumo:
                    total_itens = resumo.get("total_itens")
                    orc_total = resumo.get("orcamento_total_2026")
                    if total_itens is not None:
                        self.root.after(
                            0,
                            lambda total=total_itens: self.append_history(f"Itens processados: {total}"),
                        )
                    if orc_total is not None:
                        self.root.after(
                            0,
                            lambda valor=orc_total: self.append_history(f"Orcamento total 2026: R$ {valor:,.2f}"),
                        )

            self.root.after(0, self._stop_progress_pulse)
            self.root.after(0, lambda: self._set_progress_value(100))
        finally:
            fin_params._CACHE = base_cache

    def _place_piscofins_filters(self, start_col: int) -> int:
        # Fonte
        self.label_piscofins_fonte.grid(row=0, column=start_col, sticky="w", padx=5)
        self.radio_piscofins_debito.grid(row=1, column=start_col, sticky="w", padx=5)
        start_col += 1
        self.radio_piscofins_rma.grid(row=1, column=start_col, sticky="w", padx=5)
        start_col += 1
        # Filtros
        self.label_piscofins_cod.grid(row=0, column=start_col, sticky="w", padx=5)
        self.entry_piscofins_cod.grid(row=1, column=start_col, padx=5, sticky="we", columnspan=2)
        start_col += 2
        if self.piscofins_fonte_var.get() != "rma":
            self.label_piscofins_cst.grid(row=0, column=start_col, sticky="w", padx=5)
            self.entry_piscofins_cst.grid(row=1, column=start_col, padx=5, sticky="w")
            start_col += 1
        return start_col

    def update_fields(self) -> None:
        for widget in self.inputs_frame.winfo_children():
            if hasattr(widget, 'grid_forget'):
                widget.grid_forget()  # type: ignore[union-attr]

        self.entry_empresa.delete(0, tk.END)
        self._hide_ml_inputs()

        option = self.selected_option.get()

        if option == "ml_orcamento":
            # Reinicia valores principais para novo ciclo
            if not self.ml_mode_var.get():
                self.ml_mode_var.set(self._ml_defaults["mode"])
            self.ml_margin_var.set("0.0")
            self._show_ml_inputs()
            self._on_ml_mode_selected()
            return

        if option in MAN_OPTION_MAP:
            man_key = MAN_OPTION_MAP[option]
            config = {
                "needs_dates": MAN_OPTIONS[man_key]["needs_dates"],
                "needs_empresa": MAN_OPTIONS[man_key]["needs_empresa"],
            }
        else:
            config = EXTERNAL_OPTIONS.get(option)

        if not config:
            return

        col = 0
        if config["needs_dates"]:
            self.label_inicio.grid(row=0, column=col, sticky="w", padx=5)
            self.entry_inicio.grid(row=1, column=col, padx=5)
            col += 1

            self.label_fim.grid(row=0, column=col, sticky="w", padx=5)
            self.entry_fim.grid(row=1, column=col, padx=5)
            col += 1

            self.entry_inicio.set_date(datetime.now())
            self.entry_fim.set_date(datetime.now())

        if config["needs_empresa"]:
            self.label_empresa.grid(row=0, column=col, sticky="w", padx=5)
            self.entry_empresa.grid(row=1, column=col, padx=5)

        if option == "piscofins":
            if not self.piscofins_cod_var.get().strip():
                self.piscofins_cod_var.set(PISCOFINS_DEFAULT_COD_TIPO_DESPESA)
            self._place_piscofins_filters(col)

    def append_history(self, text: str) -> None:
        self.text_history.config(state="normal")
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.text_history.insert(tk.END, f"{timestamp} - {text}\n")
        self.text_history.see(tk.END)
        self.text_history.config(state="disabled")

    def processar(self) -> None:
        if self.is_processing:
            messagebox.showinfo(
                "Processando",
                "Ja existe um processamento em andamento. Por favor, aguarde.",
            )
            return

        option = self.selected_option.get()
        if not option:
            messagebox.showwarning(
                "Atencao",
                "Selecione um tipo de relatorio.",
            )
            return

        self.is_processing = True
        self.btn_processar.config(state="disabled")

        if option in MAN_OPTION_MAP or option == "ml_orcamento":
            self.progress.stop()
            self.progress.configure(mode="determinate", value=0, maximum=100)
        else:
            self.progress.configure(mode="indeterminate")
            self.progress.start(10)

        thread = threading.Thread(target=self._processar_background, daemon=True)
        thread.start()

    def _processar_background(self) -> None:
        option = self.selected_option.get()
        label = self._get_option_label(option)
        self.root.after(0, lambda: self.append_history(f"Iniciando: {label}"))

        try:
            if option in MAN_OPTION_MAP:
                self._run_man_option(option)
            else:
                self._run_external_option(option)
        except Exception as exc:
            self.root.after(0, lambda msg=str(exc): self._report_processing_error(msg))
        finally:
            self.root.after(0, self._finalizar_processamento)

    def _get_piscofins_filters(self) -> dict[str, str]:
        cod_tipos = self.piscofins_cod_var.get().strip() or PISCOFINS_DEFAULT_COD_TIPO_DESPESA
        fonte = self.piscofins_fonte_var.get().strip() or "debito"
        cst_pis = ""
        if fonte != "rma":
            cst_pis = self.piscofins_cst_var.get().strip() or PISCOFINS_DEFAULT_CST_PIS
        return {"cod_tipos": cod_tipos, "cst_pis": cst_pis, "fonte": fonte}

    def _run_external_option(self, option: str) -> None:
        env = os.environ.copy()

        if option == "ml_orcamento":
            self._run_ml_orcamento()
            return
        elif option == "piscofins":
            export_dir = self._get_output_directory(option)
            env["FINSCOPE_EXPORT_DIR"] = str(export_dir)
            filtros = self._get_piscofins_filters()
            cmd = [
                sys.executable,
                "-m",
                "Modulo_PisCofins.main",
                "--cod-tipo-despesa",
                filtros["cod_tipos"],
                "--fonte",
                filtros["fonte"],
                "--anos",
                PISCOFINS_DEFAULT_ANOS or "2020,2021",
            ]
            if filtros["fonte"] != "rma" and filtros["cst_pis"]:
                cmd.extend(["--cst-pis", filtros["cst_pis"]])
            self.root.after(0, lambda: self.append_history("Processando PIS/COFINS..."))
            self.root.after(
                0,
                lambda: self.append_history(
                    f"Filtros aplicados: fonte={filtros['fonte']} | cod_tipo_despesa={filtros['cod_tipos']}"
                    + (f" | cst_pis={filtros['cst_pis']}" if filtros["fonte"] != "rma" else "")
                ),
            )
        elif option == "analise_estrategias":
            export_dir = self._get_output_directory(option)
            env["FINSCOPE_EXPORT_DIR"] = str(export_dir)
            ml_script = self.analysis_dir / "test_integration_ml.py"
            self.root.after(
                0,
                lambda: self.append_history("Atualizando dados de integracao ML..."),
            )
            ml_retry = False
            while True:
                ml_result = subprocess.run(
                    [sys.executable, str(ml_script), "--output-dir", str(export_dir)],
                    capture_output=True,
                    text=True,
                    cwd=str(Path(__file__).parent),
                )
                ml_stdout = ml_result.stdout or ""
                if ml_stdout:
                    for line in ml_stdout.splitlines():
                        if line.strip():
                            self.root.after(0, lambda l=line: self.append_history(l))
                if ml_result.returncode == 0:
                    break

                combined_output = ml_stdout + "\n" + (ml_result.stderr or "")
                if not ml_retry and "Arquivo ML nao encontrado" in combined_output:
                    ml_retry = True
                    self.root.after(
                        0,
                        lambda: self.append_history(
                            "Arquivo ML nao encontrado. Gerando orcamento ML automaticamente..."
                        ),
                    )
                    self._run_ml_orcamento()
                    self.root.after(0, self._start_progress_pulse)
                    continue
                break

            if ml_result.returncode != 0:
                stderr_text = ml_result.stderr.strip() or "Falha ao executar test_integration_ml.py."
                self.root.after(
                    0,
                    lambda text=stderr_text: self.append_history(f"[ERRO] {text}"),
                )
                raise RuntimeError("Atualizacao de integracao ML falhou.")

            script_path = self.analysis_dir / "analise_estrategias_detalhada.py"
            analysis_input = export_dir / "teste_integracao_ml.xlsx"
            cmd = [
                sys.executable,
                str(script_path),
                "--output-dir",
                str(export_dir),
                "--input-file",
                str(analysis_input),
            ]
            self.root.after(
                0,
                lambda: self.append_history("Gerando analise detalhada de estrategias..."),
            )
        else:
            raise ValueError(f"Opcao externa nao suportada: {option}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(Path(__file__).parent),
            env=env,
        )

        if result.stdout:
            for line in result.stdout.splitlines():
                if line.strip():
                    self.root.after(0, lambda l=line: self.append_history(l))

        if result.returncode != 0:
            stderr_text = result.stderr.strip() or "Processo retornou codigo diferente de zero."
            self.root.after(
                0,
                lambda text=stderr_text: self.append_history(f"[ERRO] {text}"),
            )

    def _run_man_option(self, option: str) -> None:
        man_key = MAN_OPTION_MAP.get(option)
        if man_key is None:
            self.root.after(
                0,
                lambda: self._report_processing_error(f"Opcao MAN desconhecida: {option}"),
            )
            return

        opt = MAN_OPTIONS[man_key]
        data_inicio = self.entry_inicio.get_date().strftime("%d/%m/%Y")
        data_fim = self.entry_fim.get_date().strftime("%d/%m/%Y")
        empresa = self.entry_empresa.get().strip() if opt["needs_empresa"] else "0"

        if opt["needs_dates"] and (not data_inicio or not data_fim):
            self.root.after(0, lambda: self._report_input_error("Informe as datas de inicio e fim."))
            return

        if opt["needs_empresa"] and not empresa:
            self.root.after(0, lambda: self._report_input_error("Informe os numeros das empresas."))
            return

        export_dir = self._get_output_directory(option)
        output_path: Path | str = ""

        try:
            self.root.after(0, lambda: self.append_history("[INFO] Processando dados..."))
            self.root.after(0, lambda: self.progress.configure(value=33))

            if opt.get("is_consolidated"):
                output_path = self._process_consolidated_report(data_inicio, data_fim, empresa, option)
            elif opt.get("is_followup"):
                df = fetch_data_from_db_FollowUp(use_sqlalchemy=False)
                self.root.after(0, lambda: self.append_history("[INFO] Formatando dados..."))
                self.root.after(0, lambda: self.progress.configure(value=66))

                filename = "CBS005_FOLLOW_UP.xlsx"
                output_path = export_dir / filename
                apply_excel_formatting_FollowUp(df, str(output_path))
            else:
                df = fetch_data_from_db_MAN(
                    data_inicio,
                    data_fim,
                    empresa,
                    opt["procedure"],
                    use_sqlalchemy=False,
                )
                self.root.after(0, lambda: self.append_history("[INFO] Formatando dados..."))
                self.root.after(0, lambda: self.progress.configure(value=66))

                filename = f"MAN_{opt['procedure'].split('_')[-1]}.xlsx"
                output_path = export_dir / filename
                apply_excel_formatting(df, str(output_path))

            self.root.after(
                0,
                lambda path=str(output_path): self.append_history(f"[SUCESSO] Arquivo salvo em: {path}"),
            )
            self.root.after(0, lambda: self.progress.configure(value=100))
        except Exception as exc:
            self.root.after(0, lambda msg=str(exc): self._report_processing_error(msg))

    def _process_consolidated_report(self, data_inicio: str, data_fim: str, empresa: str, option: str) -> str:
        import pandas as pd

        dfs_dict = {}

        procedures = [
            ("EXT_MAN001_pedidos_de_compras_dataLiquidacao", "MAN001_LIQUIDACAO"),
            ("EXT_MAN001_pedidos_de_compras_dataOc", "MAN001_OC"),
            ("EXT_MAN004_Resumo_de_Custos_Rma", "MAN004_RMA"),
        ]

        for proc, sheet_name in procedures:
            self.root.after(0, lambda name=sheet_name: self.append_history(f"[INFO] Processando {name}..."))
            emp = empresa if "MAN004" in proc else "0"
            df = fetch_data_from_db_MAN(data_inicio, data_fim, emp, proc, use_sqlalchemy=False)
            dfs_dict[sheet_name] = df

        self.root.after(0, lambda: self.append_history("[INFO] Processando Follow UP Compras..."))
        df_followup = fetch_data_from_db_FollowUp(use_sqlalchemy=False)
        dfs_dict["Follow_UP"] = df_followup

        self.root.after(0, lambda: self.append_history("[INFO] Consolidando relatorios..."))
        df_consolidated, resumo_por_despesa, total_geral = consolidate_reports(dfs_dict)

        filename = f"MAN_Consolidado_{data_inicio.replace('/', '')}.xlsx"
        export_dir = self._get_output_directory(option)
        output_path = export_dir / filename

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            for sheet_name, df in dfs_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                apply_consolidated_formatting(df, writer, sheet_name)

            df_consolidated.to_excel(writer, sheet_name="CONSOLIDADO", index=False)
            apply_consolidated_formatting(df_consolidated, writer, "CONSOLIDADO")

            df_pend = getattr(df_consolidated, "_tag_pendencias", None)
            if df_pend is not None and hasattr(df_pend, "empty") and not df_pend.empty:
                df_pend.to_excel(writer, sheet_name="TAG_PENDENTES", index=False)
                apply_consolidated_formatting(df_pend, writer, "TAG_PENDENTES")

        self.root.after(
            0,
            lambda path=str(output_path): self.append_history(f"[SUCESSO] Arquivo salvo em: {path}"),
        )
        self.root.after(0, lambda: self.append_history("\nResumo da Consolidacao:"))
        self.root.after(0, lambda: self.append_history("=" * 50))

        if "MAN001_LIQUIDACAO" in dfs_dict:
            total = len(dfs_dict["MAN001_LIQUIDACAO"])
            filtrado = len(
                [
                    registro
                    for registro in df_consolidated.to_dict("records")
                    if registro.get("FONTE") == "MAN001_LIQUIDACAO"
                ]
            )
            self.root.after(
                0,
                lambda t=total, f=filtrado: self.append_history(
                    f"\nMAN001_LIQUIDACAO:\n  - Registros iniciais: {t}\n  - Apos filtros: {f} registros"
                ),
            )

        if "MAN001_OC" in dfs_dict:
            total = len(dfs_dict["MAN001_OC"])
            filtrado = len(
                [
                    registro
                    for registro in df_consolidated.to_dict("records")
                    if registro.get("FONTE") == "MAN001_OC"
                ]
            )
            self.root.after(
                0,
                lambda t=total, f=filtrado: self.append_history(
                    f"\nMAN001_OC:\n  - Registros iniciais: {t}\n  - Apos filtros: {f} registros"
                ),
            )

        if "MAN004_RMA" in dfs_dict:
            total = len(dfs_dict["MAN004_RMA"])
            filtrado = len(
                [
                    registro
                    for registro in df_consolidated.to_dict("records")
                    if registro.get("FONTE") == "MAN004"
                ]
            )
            self.root.after(
                0,
                lambda t=total, f=filtrado: self.append_history(
                    f"\nMAN004_RMA:\n  - Registros iniciais: {t}\n  - Apos filtros: {f} registros"
                ),
            )

        if "Follow_UP" in dfs_dict:
            total = len(dfs_dict["Follow_UP"])
            filtrado = len(
                [
                    registro
                    for registro in df_consolidated.to_dict("records")
                    if registro.get("FONTE") == "CBS005_FOLLOW"
                ]
            )
            self.root.after(
                0,
                lambda t=total, f=filtrado: self.append_history(
                    f"\nFollow_UP:\n  - Registros iniciais: {t}\n  - Apos filtros: {f} registros"
                ),
            )

        self.root.after(
            0,
            lambda: self.append_history(f"\n\nTotal consolidado: {len(df_consolidated)} registros"),
        )
        self.root.after(0, lambda: self.append_history("=" * 50))
        self.root.after(0, lambda: self.append_history("\nResumo Financeiro por Tipo de Despesa:"))
        self.root.after(0, lambda: self.append_history("=" * 50))

        if resumo_por_despesa is not None:
            for _, row in resumo_por_despesa.iterrows():
                self.root.after(
                    0,
                    lambda tipo=row["TIPO_DESPESA"],
                    valor=row["VALOR"],
                    qtd=row["QUANTIDADE"]: self.append_history(
                        f"\nTipo Despesa: {tipo}\n  Valor Total: R$ {valor:,.2f}\n  Quantidade Total: {qtd:,.0f}"
                    ),
                )

        self.root.after(0, lambda: self.append_history("\n" + "=" * 50))
        self.root.after(0, lambda: self.append_history("\nTotais Gerais:"))
        self.root.after(
            0,
            lambda: self.append_history(f"  Valor Total: R$ {total_geral['valor_total']:,.2f}"),
        )
        self.root.after(
            0,
            lambda: self.append_history(f"  Quantidade Total: {total_geral['qtd_total']:,.0f}"),
        )
        self.root.after(0, lambda: self.append_history("=" * 50))
        self.root.after(0, lambda: self.progress.configure(value=100))

        return str(output_path)

    def _report_input_error(self, message: str) -> None:
        self.progress.configure(value=0)
        self.append_history(f"[ERRO] {message}")

    def _report_processing_error(self, message: str) -> None:
        self.progress.configure(value=0)
        self.append_history(f"[ERRO] {message}")

    def _finalizar_processamento(self) -> None:
        self.progress.stop()
        if self.progress["mode"] == "indeterminate":
            self.progress.configure(mode="determinate", value=0, maximum=100)
        self.is_processing = False
        self.btn_processar.config(state="normal")

    def _get_option_label(self, option: str) -> str:
        if option in MAN_OPTION_MAP:
            man_key = MAN_OPTION_MAP[option]
            return MAN_OPTIONS[man_key]["label"]
        if option == "ml_orcamento":
            return "Gerar Orcamento"
        if option == "piscofins":
            return "Relatorio PIS/COFINS"
        return option

def main() -> None:
    root = tk.Tk()
    app = FinScopeGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()


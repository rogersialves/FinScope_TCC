"""
Pacote GUI do FinScope.

Este pacote organiza os componentes da interface gráfica:
- main_window: Janela principal e layout base
- styles: Estilos e temas
- ml_panel: Componentes do painel ML Orçamento  
- reports_panel: Componentes do painel de relatórios
- piscofins_panel: Componentes do painel PIS/COFINS
- widgets: Widgets e helpers reutilizáveis

Para manter compatibilidade, a classe principal FinScopeGUI
continua em finscope_gui.py na raiz.
"""

from gui.styles import COLORS, configure_styles
from gui.widgets import (
    create_scrollable_frame,
    create_labeled_entry,
    create_checkbox_group,
)

__all__ = [
    "COLORS",
    "configure_styles",
    "create_scrollable_frame",
    "create_labeled_entry",
    "create_checkbox_group",
]

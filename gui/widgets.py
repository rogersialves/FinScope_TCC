"""
Widgets e helpers reutilizáveis para a GUI.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Dict, List, Optional, Tuple

from gui.styles import COLORS


def create_scrollable_frame(parent: tk.Widget) -> Tuple[ttk.Frame, tk.Canvas]:
    """
    Cria um frame com scroll vertical.
    
    Args:
        parent: Widget pai
        
    Returns:
        Tuple (frame_interno, canvas) para adicionar conteúdo
    """
    canvas = tk.Canvas(
        parent,
        bg=COLORS["bg"],
        highlightthickness=0,
    )
    scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
    
    inner_frame = ttk.Frame(canvas, style="Custom.TFrame")
    
    inner_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )
    
    canvas.create_window((0, 0), window=inner_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)
    
    canvas.pack(side="left", fill="both", expand=True)
    scrollbar.pack(side="right", fill="y")
    
    # Bind mouse wheel
    def _on_mousewheel(event: tk.Event) -> None:
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    
    canvas.bind_all("<MouseWheel>", _on_mousewheel)
    
    return inner_frame, canvas


def create_labeled_entry(
    parent: tk.Widget,
    label_text: str,
    variable: tk.StringVar,
    width: int = 20,
    row: int = 0,
    column: int = 0,
    tooltip: Optional[str] = None,
) -> ttk.Entry:
    """
    Cria um par label + entry.
    
    Args:
        parent: Widget pai
        label_text: Texto do label
        variable: StringVar para o entry
        width: Largura do entry
        row: Linha no grid
        column: Coluna no grid (label fica em column, entry em column+1)
        tooltip: Texto de tooltip opcional
        
    Returns:
        Widget Entry criado
    """
    label = ttk.Label(
        parent,
        text=label_text,
        style="Custom.TLabel",
    )
    label.grid(row=row, column=column, sticky="e", padx=(5, 2), pady=2)
    
    entry = ttk.Entry(
        parent,
        textvariable=variable,
        width=width,
        style="Custom.TEntry",
    )
    entry.grid(row=row, column=column + 1, sticky="w", padx=(2, 5), pady=2)
    
    if tooltip:
        _add_tooltip(entry, tooltip)
    
    return entry


def create_checkbox_group(
    parent: tk.Widget,
    title: str,
    options: List[Tuple[str, tk.BooleanVar]],
    columns: int = 2,
) -> ttk.LabelFrame:
    """
    Cria um grupo de checkboxes em um LabelFrame.
    
    Args:
        parent: Widget pai
        title: Título do grupo
        options: Lista de (texto, variável) para cada checkbox
        columns: Número de colunas no grid
        
    Returns:
        LabelFrame contendo os checkboxes
    """
    frame = ttk.LabelFrame(
        parent,
        text=title,
        style="Custom.TLabelframe",
    )
    
    for idx, (text, var) in enumerate(options):
        row = idx // columns
        col = idx % columns
        
        cb = ttk.Checkbutton(
            frame,
            text=text,
            variable=var,
            style="Custom.TCheckbutton",
        )
        cb.grid(row=row, column=col, sticky="w", padx=5, pady=2)
    
    return frame


def create_combobox(
    parent: tk.Widget,
    values: List[str],
    variable: tk.StringVar,
    width: int = 20,
    state: str = "readonly",
) -> ttk.Combobox:
    """
    Cria um combobox estilizado.
    
    Args:
        parent: Widget pai
        values: Lista de valores para o dropdown
        variable: StringVar para o valor selecionado
        width: Largura do combobox
        state: Estado do widget (readonly, normal, disabled)
        
    Returns:
        Widget Combobox criado
    """
    combo = ttk.Combobox(
        parent,
        textvariable=variable,
        values=values,
        width=width,
        state=state,
        style="Custom.TCombobox",
    )
    return combo


def create_text_area(
    parent: tk.Widget,
    height: int = 10,
    width: int = 50,
    readonly: bool = False,
) -> tk.Text:
    """
    Cria uma área de texto estilizada.
    
    Args:
        parent: Widget pai
        height: Altura em linhas
        width: Largura em caracteres
        readonly: Se True, texto não pode ser editado
        
    Returns:
        Widget Text criado
    """
    text = tk.Text(
        parent,
        height=height,
        width=width,
        bg=COLORS["bg_light"],
        fg=COLORS["text"],
        insertbackground=COLORS["text"],
        font=("Consolas", 9),
        wrap="word",
    )
    
    if readonly:
        text.configure(state="disabled")
    
    return text


def _add_tooltip(widget: tk.Widget, text: str) -> None:
    """
    Adiciona tooltip simples a um widget.
    
    Args:
        widget: Widget alvo
        text: Texto do tooltip
    """
    tooltip_window: Optional[tk.Toplevel] = None
    
    def show_tooltip(event: tk.Event) -> None:
        nonlocal tooltip_window
        if tooltip_window:
            return
            
        x = widget.winfo_rootx() + 20
        y = widget.winfo_rooty() + widget.winfo_height() + 5
        
        tooltip_window = tk.Toplevel(widget)
        tooltip_window.wm_overrideredirect(True)
        tooltip_window.wm_geometry(f"+{x}+{y}")
        
        label = tk.Label(
            tooltip_window,
            text=text,
            bg=COLORS["bg_light"],
            fg=COLORS["text"],
            font=("Segoe UI", 8),
            padx=5,
            pady=2,
            relief="solid",
            borderwidth=1,
        )
        label.pack()
    
    def hide_tooltip(event: tk.Event) -> None:
        nonlocal tooltip_window
        if tooltip_window:
            tooltip_window.destroy()
            tooltip_window = None
    
    widget.bind("<Enter>", show_tooltip)
    widget.bind("<Leave>", hide_tooltip)

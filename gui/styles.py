"""
Estilos e temas para a interface gráfica do FinScope.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict, Any


# Paleta de cores do FinScope
COLORS: Dict[str, str] = {
    "bg": "#1a1a2e",           # Fundo principal (azul escuro)
    "bg_light": "#16213e",     # Fundo secundário
    "accent": "#0f3460",       # Destaque
    "primary": "#e94560",      # Cor primária (vermelho)
    "text": "#eaeaea",         # Texto principal
    "text_dim": "#a0a0a0",     # Texto secundário
    "success": "#4ecca3",      # Verde sucesso
    "warning": "#f39c12",      # Amarelo aviso
    "error": "#e74c3c",        # Vermelho erro
    "border": "#333355",       # Bordas
}


def configure_styles(root: tk.Tk) -> ttk.Style:
    """
    Configura estilos ttk para a aplicação.
    
    Args:
        root: Janela principal do Tk
        
    Returns:
        Objeto Style configurado
    """
    style = ttk.Style(root)
    
    # Configuração base
    root.configure(bg=COLORS["bg"])
    
    # Frame customizado
    style.configure(
        "Custom.TFrame",
        background=COLORS["bg"],
    )
    
    # LabelFrame customizado
    style.configure(
        "Custom.TLabelframe",
        background=COLORS["bg"],
        foreground=COLORS["text"],
    )
    style.configure(
        "Custom.TLabelframe.Label",
        background=COLORS["bg"],
        foreground=COLORS["text"],
        font=("Segoe UI", 10, "bold"),
    )
    
    # Label customizado
    style.configure(
        "Custom.TLabel",
        background=COLORS["bg"],
        foreground=COLORS["text"],
        font=("Segoe UI", 9),
    )
    
    # Label de título
    style.configure(
        "Title.TLabel",
        background=COLORS["bg"],
        foreground=COLORS["primary"],
        font=("Segoe UI", 14, "bold"),
    )
    
    # Label de subtítulo
    style.configure(
        "Subtitle.TLabel",
        background=COLORS["bg"],
        foreground=COLORS["text_dim"],
        font=("Segoe UI", 9),
    )
    
    # Button customizado
    style.configure(
        "Custom.TButton",
        background=COLORS["accent"],
        foreground=COLORS["text"],
        font=("Segoe UI", 9, "bold"),
        padding=(10, 5),
    )
    style.map(
        "Custom.TButton",
        background=[("active", COLORS["primary"])],
    )
    
    # Button de ação primária
    style.configure(
        "Primary.TButton",
        background=COLORS["primary"],
        foreground=COLORS["text"],
        font=("Segoe UI", 10, "bold"),
        padding=(15, 8),
    )
    
    # Entry customizado
    style.configure(
        "Custom.TEntry",
        fieldbackground=COLORS["bg_light"],
        foreground=COLORS["text"],
        insertcolor=COLORS["text"],
    )
    
    # Combobox customizado
    style.configure(
        "Custom.TCombobox",
        fieldbackground=COLORS["bg_light"],
        background=COLORS["bg_light"],
        foreground=COLORS["text"],
        arrowcolor=COLORS["text"],
    )
    style.map(
        "Custom.TCombobox",
        fieldbackground=[("readonly", COLORS["bg_light"])],
        selectbackground=[("readonly", COLORS["accent"])],
    )
    
    # Checkbutton customizado
    style.configure(
        "Custom.TCheckbutton",
        background=COLORS["bg"],
        foreground=COLORS["text"],
        font=("Segoe UI", 9),
    )
    
    # Radiobutton customizado
    style.configure(
        "Custom.TRadiobutton",
        background=COLORS["bg"],
        foreground=COLORS["text"],
        font=("Segoe UI", 9),
    )
    
    # Progressbar
    style.configure(
        "Custom.Horizontal.TProgressbar",
        background=COLORS["success"],
        troughcolor=COLORS["bg_light"],
    )
    
    # Notebook (tabs)
    style.configure(
        "Custom.TNotebook",
        background=COLORS["bg"],
        tabmargins=[2, 5, 2, 0],
    )
    style.configure(
        "Custom.TNotebook.Tab",
        background=COLORS["bg_light"],
        foreground=COLORS["text"],
        padding=[10, 5],
        font=("Segoe UI", 9),
    )
    style.map(
        "Custom.TNotebook.Tab",
        background=[("selected", COLORS["accent"])],
        foreground=[("selected", COLORS["text"])],
    )
    
    return style

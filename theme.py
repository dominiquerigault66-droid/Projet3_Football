# theme.py

class Theme:
    def __init__(self):
        # 🎨 Cores principais
        self.primary = "#1E90FF"
        self.secondary = "#FF6347"
        self.background = "#F5F5F5"
        self.text = "#333333"

        # Extras
        self.success = "#28a745"
        self.warning = "#ffc107"
        self.error = "#dc3545"


# Instância global
_T = Theme()


def get_theme():
    """Retorna o tema atual"""
    return _T


def generate_css(theme):
    """Gera CSS para o Streamlit"""
    return f"""
    <style>
    body {{
        background-color: {theme.background};
        color: {theme.text};
    }}

    .stButton > button {{
        background-color: {theme.primary};
        color: white;
        border-radius: 8px;
    }}

    .stButton > button:hover {{
        background-color: {theme.secondary};
    }}
    </style>
    """
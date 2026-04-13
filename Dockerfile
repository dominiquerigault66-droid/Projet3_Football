# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — Application Streamlit Football Analytics
# ─────────────────────────────────────────────────────────────────────────────
# Image de base : Python 3.13 slim (légère, sans X11 ni GUI inutile)
FROM python:3.13-slim

# Métadonnées
LABEL maintainer="Projet3 Football"
LABEL description="Application Streamlit d'analyse de données football"

# ── Variables d'environnement ─────────────────────────────────────────────────
# Désactive le buffering Python (logs visibles en temps réel dans Docker)
ENV PYTHONUNBUFFERED=1
# Force l'encodage UTF-8 (caractères accentués, emojis)
ENV PYTHONUTF8=1
# Répertoire de travail dans le conteneur
WORKDIR /app

# ── Installation des dépendances système ──────────────────────────────────────
# gcc et default-libmysqlclient-dev sont nécessaires pour PyMySQL / SQLAlchemy
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Copie et installation des dépendances Python ─────────────────────────────
# On copie d'abord requirements.txt seul → Docker met en cache cette couche
# tant que requirements.txt ne change pas (optimisation des rebuilds)
COPY requirements_streamlit.txt .
RUN pip install --no-cache-dir -r requirements_streamlit.txt

# ── Copie du code de l'application ───────────────────────────────────────────
# On copie uniquement ce qui est nécessaire à Streamlit (pas les scripts de collecte)
COPY streamlit_app/ ./streamlit_app/
COPY theme.py .
COPY utils.py .

# ── Port exposé ───────────────────────────────────────────────────────────────
# Streamlit écoute sur 8501 par défaut
EXPOSE 8501

# ── Healthcheck ───────────────────────────────────────────────────────────────
# Docker vérifie que l'app répond toutes les 30 secondes
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# ── Commande de démarrage ─────────────────────────────────────────────────────
# --server.address=0.0.0.0 : écoute sur toutes les interfaces (obligatoire en conteneur)
# --server.headless=true   : désactive l'ouverture automatique du navigateur
CMD ["python", "-m", "streamlit", "run", "streamlit_app/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]

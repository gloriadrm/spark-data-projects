"""Configuracion compartida: rutas base del repo."""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = REPO_ROOT / "datasets"
MINI_PROJECTS_DIR = REPO_ROOT / "mini-projects"

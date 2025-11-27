from pathlib import Path
import yaml

# Ruta raíz del proyecto
BASE_DIR = Path(__file__).resolve().parents[2]

# Cargar config.yaml
CONFIG_PATH = BASE_DIR / "config.yaml"
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

# ============================
# Rutas globales
# ============================

BRONZE_DIR = BASE_DIR / CFG["paths"]["bronze_dir"]
SILVER_DIR = BASE_DIR / CFG["paths"]["silver_dir"]
SEEDS_FILE = BASE_DIR / CFG["paths"]["seeds_file"]

BRONZE_GASTO = BRONZE_DIR / "gasto"
BRONZE_UNIVERSO = BRONZE_DIR / "universo"
BRONZE_BRECHAS = BRONZE_DIR / "brechas"

SILVER_GASTO = SILVER_DIR / "gasto"
SILVER_GASTO.mkdir(parents=True, exist_ok=True)

"""
===============================================================
EXPORT GOLD — Convertir CSVs finales a Excel para MEF
===============================================================
Convierte automáticamente los CSV clave generados en /silver
a archivos Excel en /gold, donde MEF los puede usar.
===============================================================
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

from .paths import SILVER_DIR, BASE_DIR

GOLD_DIR = BASE_DIR / "data/gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


# ===============================================================
# LISTA DE ARCHIVOS A EXPORTAR
# ===============================================================

EXPORT_LIST = [
    "proyectos_final.csv",
    "proyectos_expandido_FF.csv",
    "proyectos_terna_final.csv",
    "proyectos_final_enriquecido_brechas.csv",
    "proyectos_final_clasificado.csv",  # si existe
]


# ===============================================================
# FUNCIÓN PRINCIPAL DE EXPORTACIÓN
# ===============================================================

def export_to_excel():
    print("\n📦 Exportando CSVs a Excel (carpeta GOLD)...")

    fecha = datetime.now().strftime("%Y%m%d")

    for fname in EXPORT_LIST:
        src = SILVER_DIR / fname
        if not src.exists():
            print(f"   ⚠️ Saltando: {fname} (no existe aún)")
            continue

        print(f"   ⏳ Exportando: {fname}")

        df = pd.read_csv(src, dtype=str)

        out_path = GOLD_DIR / f"{fname.replace('.csv', '')}_{fecha}.xlsx"
        df.to_excel(out_path, index=False)

        print(f"   ✅ Guardado en: {out_path.name}")

    print("\n🏁 Export GOLD completado.\n")


# ===============================================================
# ENTRYPOINT
# ===============================================================

if __name__ == "__main__":
    export_to_excel()

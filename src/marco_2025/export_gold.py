"""
===============================================================
EXPORT GOLD — Convertir CSVs finales a Excel para MEF
===============================================================
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from .paths import SILVER_DIR, BASE_DIR


GOLD_DIR = BASE_DIR / "data/gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)


# ===============================================================
# Columnas que deben ser numéricas (si existen)
# ===============================================================
NUMERIC_COLS = [
    "CODIGO_UNICO","FUENTE",
    "DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025",
    "MONTO_VIABLE", "COSTO_ACTUALIZADO",
    "UBIGEO","BENEFICIARIO","N_BRECHAS","CODIGO_SNIP","SALDO_EJECUTAR",
]

EXPORT_LIST = [
    "proyectos_final.csv",
    "proyectos_expandido_FF.csv",
    "proyectos_terna_final.csv",
    "proyectos_final_enriquecido_brechas.csv",
    "proyectos_final_clasificado.csv",
]


def clean_to_numeric(df):
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue

        df[col] = (
            df[col].astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("PEN", "", regex=False)
            .str.replace("S/.", "", regex=False)
            .str.strip()
        )

        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def export_to_excel():
    print("\n📦 Exportando CSVs a Excel (carpeta GOLD)...")

    fecha = datetime.now().strftime("%Y%m%d")

    for fname in EXPORT_LIST:
        src = SILVER_DIR / fname
        if not src.exists():
            print(f"   ⚠️ Saltando: {fname} (no existe)")
            continue

        print(f"   ⏳ Procesando: {fname}")

        df = pd.read_csv(src)
        df = clean_to_numeric(df)

        out = GOLD_DIR / f"{fname.replace('.csv','')}_{fecha}.xlsx"

        # 👉 CLAVE: usar ExcelWriter + número como número
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

            # aplicar formato numérico (sin apóstrofe)
            ws = writer.sheets["Sheet1"]
            for col_idx, col in enumerate(df.columns, start=1):
                if col in NUMERIC_COLS:
                    for cell in ws.iter_cols(min_col=col_idx, max_col=col_idx,
                                             min_row=2, max_row=len(df) + 1):
                        for c in cell:
                            c.number_format = "0"  # número entero sin símbolo

        print(f"   ✅ Guardado: {out.name}")

    print("\n🏁 Export GOLD completado.\n")


if __name__ == "__main__":
    export_to_excel()

"""
===============================================================
RUN PIPELINE — Ejecución completa con medidores por etapa
===============================================================
"""

import os, sys

# Agregar carpeta src al PYTHONPATH automáticamente
ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.append(SRC)
import time
import subprocess
from pathlib import Path


# ================================================================
# Helper: formateo de tiempo
# ================================================================
def t(sec):
    m = sec // 60
    s = sec % 60
    return f"{int(m)}m {int(s)}s" if m > 0 else f"{int(s)}s"


# ================================================================
# 1) Crear directorios necesarios
# ================================================================
def ensure_directories():
    dirs = [
        Path("data/bronze/gasto"),
        Path("data/bronze/universo"),
        Path("data/bronze/brechas"),
        Path("data/silver"),
        Path("data/seeds"),
        Path("data/gold"),
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    print("📁 Directorios verificados.")


# ================================================================
# 2) Validar insumos crudos
# ================================================================
def validate_inputs():
    required = {
        "data/bronze/gasto/2022-Gasto.csv",
        "data/bronze/gasto/2023-Gasto.csv",
        "data/bronze/gasto/2024-Gasto-Diario.csv",
        "data/bronze/gasto/2025-Gasto-Diario.csv",

        "data/bronze/universo/DETALLE_INVERSIONES.csv",
        "data/bronze/universo/CIERRE_INVERSIONES.csv",
        "data/bronze/universo/INVERSIONES_DESACTIVADAS.csv",

        "data/bronze/brechas/INVERSIONES_BRECHAS.csv",
    }

    missing = [f for f in required if not Path(f).exists()]
    if missing:
        print("\n❌ FALTAN ARCHIVOS CRUCIALES:")
        for m in missing:
            print("   -", m)
        print("\n➡ Pon los archivos y corre de nuevo.")
        sys.exit(1)

    print("🟢 Archivos crudos OK.")


# ================================================================
# 3) Ejecutar módulo y medir tiempo
# ================================================================
def run_step(name, module_name):
    print(f"\n▶️  {name}...")
    start = time.time()

    res = subprocess.run([sys.executable, "-m", module_name])

    if res.returncode != 0:
        print(f"❌ Error al ejecutar {name}")
        sys.exit(1)

    elapsed = int(time.time() - start)
    print(f"   ✅ {name} completado en {t(elapsed)}")
    return elapsed


# ================================================================
# MAIN
# ================================================================
def main():

    print("\n=====================================================")
    print("         PIPELINE MARCO 2025 — EJECUCIÓN TOTAL       ")
    print("=====================================================\n")

    total_start = time.time()

    ensure_directories()
    validate_inputs()

    tiempos = {}

    # -------------------------
    # Ejecutar cada módulo
    # -------------------------

    tiempos["1) Pipeline Gasto"] = run_step(
        "Pipeline del gasto 2022–2025",
        "marco_2025.gasto_pipeline"
    )

    if Path("data/seeds/Semillas_Indicadores_Actualizados.xlsx").exists():
        tiempos["2) Clasificación NLP"] = run_step(
            "Clasificación Marco 2025 (NLP)",
            "marco_2025.nlp_marco2025"
        )
    else:
        print("⚠️ No hay archivo de seeds → Saltando NLP")

    tiempos["3) Exportación GOLD"] = run_step(
        "Exportar Excel finales",
        "marco_2025.export_gold"
    )

    # -------------------------
    # Resumen final
    # -------------------------

    total_elapsed = int(time.time() - total_start)

    print("\n=====================================================")
    print("                     RESUMEN FINAL                  ")
    print("=====================================================")

    for k, v in tiempos.items():
        print(f"   ⏱ {k}: {t(v)}")

    print("-----------------------------------------------------")
    print(f"   🟣 TIEMPO TOTAL: {t(total_elapsed)}")
    print("-----------------------------------------------------")

    print("\n🎉 Pipeline ejecutado exitosamente.\n")
    print("📂 Excel finales disponibles en: data/gold/\n")


if __name__ == "__main__":
    main()

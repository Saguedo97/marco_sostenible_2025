"""
===============================================================
RUN PIPELINE — Ejecución automática completa (1 solo comando)
===============================================================

Este script:
1. Crea la estructura data/ si no existe
2. Verifica archivos esenciales
3. Ejecuta el pipeline MEF completo
4. Ejecuta la clasificación NLP (opcional si seeds presentes)
5. Exporta a GOLD los Excel finales

Uso:
    python run_pipeline.py
===============================================================
"""

import os
from pathlib import Path
import sys
import subprocess

# ================================================================
# 1) Crear estructura de carpetas automáticamente
# ================================================================

def ensure_directories():
    base = Path("data")
    dirs = [
        base / "bronze" / "gasto",
        base / "bronze" / "universo",
        base / "bronze" / "brechas",
        base / "silver",
        base / "seeds",
        base / "gold",
    ]

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    print("📁 Directorios verificados / creados.")


# ================================================================
# 2) Validar presencia de archivos clave
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
        print("\n❌ FALTAN ARCHIVOS CRUCIALES:\n")
        for f in missing:
            print("   -", f)
        print("\n➡ Por favor colócalos antes de continuar.\n")
        sys.exit(1)

    print("🟢 Todos los archivos crudos están presentes.")


# ================================================================
# 3) Ejecutar módulo Python dentro del paquete
# ================================================================

def run_module(module_name):
    print(f"\n🚀 Ejecutando módulo: {module_name}")
    result = subprocess.run([sys.executable, "-m", module_name])
    if result.returncode != 0:
        print(f"\n❌ ERROR ejecutando {module_name}")
        sys.exit(1)
    print(f"✅ {module_name} completado.\n")


# ================================================================
# MAIN
# ================================================================

def main():
    print("\n==============================")
    print("   PIPELINE MARCO 2025")
    print("==============================\n")

    ensure_directories()
    validate_inputs()

    # 1) Pipeline completo de gasto
    run_module("marco_2025.gasto_pipeline")

    # 2) Clasificación (si hay seeds)
    if Path("data/seeds/Semillas_Indicadores_Actualizados.xlsx").exists():
        run_module("marco_2025.nlp_marco2025")
    else:
        print("⚠️ No se encontró archivo de seeds. Saltando NLP.")

    # 3) Exportación a Excel (GOLD)
    run_module("marco_2025.export_gold")

    print("\n🎉 Pipeline completo ejecutado sin errores.")
    print("📂 Resultados finales disponibles en: data/gold/\n")


if __name__ == "__main__":
    main()

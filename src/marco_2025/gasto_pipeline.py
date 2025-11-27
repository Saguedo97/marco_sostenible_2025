"""
===============================================================
PIPELINE MEF — GASTO 2022–2025 (VERSIÓN PRO - CONSOLA)
===============================================================
Estructura limpia, modular, y basada en carpetas bronze/silver.

Nota:
Todo este archivo depende de que hayas colocado tus datos crudos en:
data/bronze/gasto/
data/bronze/universo/
data/bronze/brechas/

Y que config.yaml tenga las rutas correctas.
===============================================================
"""

import pandas as pd
import numpy as np
import duckdb
import re
from pathlib import Path

from .paths import (
    BRONZE_GASTO,
    BRONZE_UNIVERSO,
    BRONZE_BRECHAS,
    SILVER_DIR,
    SILVER_GASTO,
)

CHUNK = 500_000


# ================================================================
# UTILIDADES
# ================================================================

def normalize_key(s: pd.Series) -> pd.Series:
    s = s.astype("string").fillna("")
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    return s


def sum_by_project(df, monto_col="MONTO_DEVENGADO", key="PRODUCTO_PROYECTO"):
    return (
        df.groupby(key)[monto_col]
        .sum(min_count=1)
        .reset_index()
    )


def clean_devengados(df):
    dev_cols = ["DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025"]
    for c in dev_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


# ================================================================
# CELDA 1 — CONSOLIDADO DEVENGADO 2022–2025
# ================================================================

def build_devengado_2022_2025():
    print("📌 Celda 1 — Consolidando devengado 2022–2025...")

    files = {
        "2022": BRONZE_GASTO / "2022-Gasto.csv",
        "2023": BRONZE_GASTO / "2023-Gasto.csv",
        "2024": BRONZE_GASTO / "2024-Gasto-Diario.csv",
        "2025": BRONZE_GASTO / "2025-Gasto-Diario.csv",
    }

    df_agg = None

    for year, path in files.items():
        print(f"   ⏳ Procesando {year}...")
        df = pd.read_csv(path, dtype=str)

        # Normalizar
        df["PRODUCTO_PROYECTO"] = normalize_key(df["PRODUCTO_PROYECTO"])
        df["MONTO_DEVENGADO"] = pd.to_numeric(df["MONTO_DEVENGADO"], errors="coerce").fillna(0)

        df_year = sum_by_project(df, "MONTO_DEVENGADO", "PRODUCTO_PROYECTO")
        df_year.rename(columns={"MONTO_DEVENGADO": f"DEV_{year}"}, inplace=True)

        df_agg = df_year if df_agg is None else df_agg.merge(df_year, how="outer")

    # Guardar
    out_outer = SILVER_GASTO / "consolidado_devengado_2022_2025_outer.csv"
    df_agg.to_csv(out_outer, index=False, encoding="utf-8-sig")

    df_pos = df_agg[df_agg[[c for c in df_agg.columns if c.startswith("DEV_")]].sum(axis=1) > 0]
    out_pos = SILVER_GASTO / "consolidado_devengado_2022_2025_positive.csv"
    df_pos.to_csv(out_pos, index=False, encoding="utf-8-sig")

    print("   ✅ Celda 1 completada.")


# ================================================================
# CELDA 2 — CSV → PARQUET
# ================================================================

def csv_to_parquet_by_year():
    print("📌 Celda 2 — Generando Parquets...")

    # --- reader blindado ---
    duckdb.sql("""
        CREATE OR REPLACE MACRO safe_read(path) AS 
            read_csv_auto(
                path,
                types={
                    'SECTOR': 'VARCHAR',
                    'DIVISION_FUNCIONAL': 'VARCHAR',
                    'GRUPO_FUNCIONAL': 'VARCHAR',
                    'PLIEGO': 'VARCHAR',
                    'DEPARTAMENTO_EJECUTORA': 'VARCHAR',
                    'PROVINCIA_EJECUTORA': 'VARCHAR',
                    'DISTRITO_EJECUTORA': 'VARCHAR',
                    'UBIGEO': 'VARCHAR'
                },
                sample_size=-1,
                nullstr=''
            );
    """)

    SILVER_GASTO_TMP = SILVER_GASTO / "_tmp_parquet"
    SILVER_GASTO_TMP.mkdir(exist_ok=True)

    files = {
        "2022": BRONZE_GASTO / "2022-Gasto.csv",
        "2023": BRONZE_GASTO / "2023-Gasto.csv",
        "2024": BRONZE_GASTO / "2024-Gasto-Diario.csv",
        "2025": BRONZE_GASTO / "2025-Gasto-Diario.csv",
    }

    for year, file in files.items():
        out = SILVER_GASTO_TMP / f"{year}.parquet"
        print(f"   ⏳ {year} → parquet...")
        duckdb.sql(f"""
            COPY (SELECT * FROM safe_read('{file}'))
            TO '{out}' (FORMAT PARQUET);
        """)

    print("   ✅ Celda 2 completada.")

# ================================================================
# CELDA 3 — UNIFICAR PARQUETS
# ================================================================

def build_parquet_unificado():
    print("📌 Celda 3 — Uniendo Parquets...")

    SILVER_GASTO_TMP = SILVER_GASTO / "_tmp_parquet"
    files = sorted(SILVER_GASTO_TMP.glob("*.parquet"))

    out_final = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT * FROM (
                {' UNION ALL '.join([f"SELECT * FROM parquet_scan('{f}')" for f in files])}
            )
        ) TO '{out_final}' (FORMAT 'PARQUET');
    """)

    print("   ✅ Celda 3 completada.")


# ================================================================
# CELDA 4 — INCONSISTENCIAS
# ================================================================

def extraer_inconsistencias_gasto():

    print("📌 Celda 4 — Detectando inconsistencias...")

    f_parquet = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    VAR_CHECK = [
        "FUNCION","FUNCION_NOMBRE",
        "DIVISION_FUNCIONAL","DIVISION_FUNCIONAL_NOMBRE",
        "GRUPO_FUNCIONAL","GRUPO_FUNCIONAL_NOMBRE",
        "NIVEL_GOBIERNO","NIVEL_GOBIERNO_NOMBRE",
        "SECTOR","SECTOR_NOMBRE",
        "PLIEGO","PLIEGO_NOMBRE",
        "DEPARTAMENTO_EJECUTORA_NOMBRE",
        "PROVINCIA_EJECUTORA_NOMBRE",
        "DISTRITO_EJECUTORA_NOMBRE",
    ]

    q = f"""
        SELECT PRODUCTO_PROYECTO AS CODIGO_UNICO,
               {",".join([f"LIST_DISTINCT({c}) AS {c}" for c in VAR_CHECK])}
        FROM parquet_scan('{f_parquet}')
        GROUP BY 1
        HAVING { " OR ".join([f"LIST_LENGTH({c}) > 1" for c in VAR_CHECK]) }
    """

    df_inc = duckdb.sql(q).df()
    out = SILVER_GASTO / "inconsistencias_variables_gasto_apiladas.csv"
    df_inc.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ⚠️ Inconsistencias detectadas:", len(df_inc))
    print("   ✅ Celda 4 completada.")


# ================================================================
# CELDA 5 — UNIVERSO BASE
# ================================================================

def build_universo_base():
    print("📌 Celda 5 — Construyendo universo base...")

    activos = pd.read_csv(BRONZE_UNIVERSO / "DETALLE_INVERSIONES.csv", dtype=str)
    cierre = pd.read_csv(BRONZE_UNIVERSO / "CIERRE_INVERSIONES.csv", dtype=str)
    desact = pd.read_csv(BRONZE_UNIVERSO / "INVERSIONES_DESACTIVADAS.csv", dtype=str)

    activos["FUENTE"] = "ACTIVO"
    cierre["FUENTE"] = "CERRADO"
    desact["FUENTE"] = "DESACTIVADO"

    # Homologar clave
    for df in [activos, cierre, desact]:
        if "CODIGO_SNIP" in df.columns:
            df["CODIGO_UNICO"] = df["CODIGO_SNIP"].fillna(df.get("CODIGO_UNICO", ""))

    base = pd.concat([activos, cierre, desact], ignore_index=True)
    base = base.drop_duplicates(subset="CODIGO_UNICO")

    out = SILVER_DIR / "proyectos_base_total.csv"
    base.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ✅ Celda 5 completada.")


# ================================================================
# CELDA 6 — TERNA FINAL
# ================================================================

def filtrar_terna_final():
    print("📌 Celda 6 — Filtrando terna final...")

    base = pd.read_csv(SILVER_DIR / "proyectos_base_total.csv", dtype=str)
    dev = pd.read_csv(SILVER_GASTO / "consolidado_devengado_2022_2025_outer.csv", dtype=str)

    df = base.merge(dev, how="left", left_on="CODIGO_UNICO", right_on="PRODUCTO_PROYECTO")

    df = clean_devengados(df)
    df["TOTAL_DEV_22_25"] = df[["DEV_2022","DEV_2023","DEV_2024","DEV_2025"]].sum(axis=1)
    df["TIENE_GASTO"] = df["TOTAL_DEV_22_25"] > 0

    df["EMPEZO"] = df["ANIO_PRIMER_DEVENGADO"].notna()

    g1 = df[df["TIENE_GASTO"] == True]
    g2 = df[(df["TIENE_GASTO"] == False) & (df["EMPEZO"] == False)]

    final = pd.concat([g1, g2], ignore_index=True)

    out = SILVER_DIR / "proyectos_terna_final.csv"
    final.to_csv(out, index=False, encoding="utf-8-sig")

    print("   🟢 Proyectos activos considerados:", len(final))
    print("   ✅ Celda 6 completada.")


# ================================================================
# CELDA 7 — VARIABLES MEF
# ================================================================

def enriquecer_con_variables_mef():
    print("📌 Celda 7 — Enriqueciendo variables MEF...")

    df = pd.read_csv(SILVER_DIR / "proyectos_terna_final.csv", dtype=str)
    f_parquet = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    q = f"""
        SELECT DISTINCT
            PRODUCTO_PROYECTO AS CODIGO_UNICO,
            FUNCION, FUNCION_NOMBRE,
            DIVISION_FUNCIONAL, DIVISION_FUNCIONAL_NOMBRE,
            GRUPO_FUNCIONAL, GRUPO_FUNCIONAL_NOMBRE,
            NIVEL_GOBIERNO, NIVEL_GOBIERNO_NOMBRE,
            SECTOR, SECTOR_NOMBRE,
            PLIEGO, PLIEGO_NOMBRE,
            DEPARTAMENTO_EJECUTORA_NOMBRE,
            PROVINCIA_EJECUTORA_NOMBRE,
            DISTRITO_EJECUTORA_NOMBRE
        FROM parquet_scan('{f_parquet}')
    """

    vars_df = duckdb.sql(q).df()
    vars_df = vars_df.groupby("CODIGO_UNICO").first().reset_index()

    df = df.merge(vars_df, on="CODIGO_UNICO", how="left")

    out = SILVER_DIR / "proyectos_final_enriquecido.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ✅ Celda 7 completada.")


# ================================================================
# CELDA 8 — BRECHAS
# ================================================================

def anexar_brechas():
    print("📌 Celda 8 — Agregando brechas...")

    df = pd.read_csv(SILVER_DIR / "proyectos_final_enriquecido.csv", dtype=str)
    brechas = pd.read_csv(BRONZE_BRECHAS / "INVERSIONES_BRECHAS.csv", dtype=str)

    brechas = brechas[brechas["CODIGO_UNICO"].isin(df["CODIGO_UNICO"])]

    long = brechas[["CODIGO_UNICO","BRECHA"]].dropna()
    long.to_csv(SILVER_DIR / "proyectos_final_brechas_long.csv", index=False)

    resumen = (
        long.groupby("CODIGO_UNICO")["BRECHA"]
        .agg(["count", lambda x: "; ".join(x)])
        .reset_index()
    )
    resumen.columns = ["CODIGO_UNICO","N_BRECHAS","BRECHAS_TODAS"]

    df2 = df.merge(resumen, on="CODIGO_UNICO", how="left")
    df2.to_csv(SILVER_DIR / "proyectos_final_enriquecido_brechas.csv", index=False)

    print("   ✅ Celda 8 completada.")


# ================================================================
# CELDA 9 — EXPANDIR POR FUENTE
# ================================================================

def expandir_por_fuente():
    print("📌 Celda 9 — Expandiendo por fuente de financiamiento...")

    df = pd.read_csv(SILVER_DIR / "proyectos_final_enriquecido_brechas.csv", dtype=str)
    f_parquet = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    q = f"""
        SELECT 
            PRODUCTO_PROYECTO AS CODIGO_UNICO,
            FUENTE_FINANCIAMIENTO AS FUENTE,
            FUENTE_FINANCIAMIENTO_NOMBRE AS FUENTE_NOMBRE,
            ANO_EJE,
            MONTO_DEVENGADO
        FROM parquet_scan('{f_parquet}')
    """

    gasto = duckdb.sql(q).df()
    gasto["MONTO_DEVENGADO"] = pd.to_numeric(gasto["MONTO_DEVENGADO"], errors="coerce").fillna(0)

    piv = gasto.pivot_table(
        index=["CODIGO_UNICO","FUENTE","FUENTE_NOMBRE"],
        columns="ANO_EJE",
        values="MONTO_DEVENGADO",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    piv.columns = [str(c) for c in piv.columns]
    piv.rename(columns={"2022": "DEV_2022","2023":"DEV_2023","2024":"DEV_2024","2025":"DEV_2025"}, inplace=True)

    merged = df.merge(piv, on="CODIGO_UNICO", how="left")

    sin_gasto = merged[merged[["DEV_2022","DEV_2023","DEV_2024","DEV_2025"]].sum(axis=1) == 0]
    sin_gasto["FUENTE"] = "0"
    sin_gasto["FUENTE_NOMBRE"] = "SIN GASTO"

    final = pd.concat([merged, sin_gasto], ignore_index=True)
    final.to_csv(SILVER_DIR / "proyectos_expandido_FF.csv", index=False)

    print("   ✅ Celda 9 completada.")


# ================================================================
# CELDA 10 — FILTRAR MUNICIPALIDADES
# ================================================================

def filtrar_final_no_munis():
    print("📌 Celda 10 — Filtrando municipalidades...")

    df = pd.read_csv(SILVER_DIR / "proyectos_expandido_FF.csv", dtype=str)

    mask = ~df["NOMBRE_UEP"].str.contains(r"(?i)municipalidad|mun\.", na=False)
    df2 = df[mask]

    out = SILVER_DIR / "proyectos_final.csv"
    df2.to_csv(out, index=False, encoding="utf-8-sig")

    print("   🟢 Proyectos finales:", len(df2))
    print("   ✅ Celda 10 completada.")


# ================================================================
# ORQUESTADOR
# ================================================================

def run_all():
    build_devengado_2022_2025()
    csv_to_parquet_by_year()
    build_parquet_unificado()
    extraer_inconsistencias_gasto()
    build_universo_base()
    filtrar_terna_final()
    enriquecer_con_variables_mef()
    anexar_brechas()
    expandir_por_fuente()
    filtrar_final_no_munis()

    print("\n🚀 Pipeline completo.\n")


if __name__ == "__main__":
    run_all()

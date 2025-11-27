"""
===============================================================
PIPELINE MEF — GASTO 2022–2025 (VERSIÓN PRO)
===============================================================
Limpio, sin duplicados, sin funciones repetidas,
con imports correctos y listo para ejecutar completo.

Estructura:
- Celda 1: Consolidar devengado 2022–2025
- Celda 2: CSV → Parquet
- Celda 3: Unificar Parquets
- Celda 4: Inconsistencias
- Celda 5: Universo Base
- Celda 6: Terna Final
- Celda 7: Variables MEF
- Celda 8: Brechas
- Celda 9: Expansión FF
- Celda 10: Filtro municipalidades
===============================================================
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from .paths import BRONZE_GASTO, BRONZE_UNIVERSO, BRONZE_BRECHAS, SILVER_DIR, SILVER_GASTO


# ================================================================
# UTILIDADES
# ================================================================

def normalize_key(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )


def clean_devengados(df):
    cols = ["DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025"]
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.replace("'", "", regex=False).str.strip()
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    return df


def sum_by_project(path: Path, year: int, chunk_size=500_000) -> pd.DataFrame:
    sums = {}

    usecols = [
        "ANO_EJE", "MES_EJE", "PROGRAMA_PPTO",
        "PRODUCTO_PROYECTO", "SEC_FUNC", "MONTO_DEVENGADO",
    ]

    dtype = {
        "ANO_EJE": "int64",
        "MES_EJE": "int64",
        "PROGRAMA_PPTO": "Int64",
        "PRODUCTO_PROYECTO": "string",
        "SEC_FUNC": "Int64",
        "MONTO_DEVENGADO": "float64",
    }

    for chunk in pd.read_csv(path, usecols=usecols, chunksize=chunk_size, dtype=dtype):

        chunk = chunk[chunk["ANO_EJE"] == year]

        if year == 2025:
            chunk = chunk[chunk["MES_EJE"] <= 10]

        chunk["PRODUCTO_PROYECTO"] = normalize_key(chunk["PRODUCTO_PROYECTO"])

        chunk = chunk.dropna(subset=["PRODUCTO_PROYECTO", "MONTO_DEVENGADO"])

        chunk["ID_UNICO"] = (
            chunk["ANO_EJE"].astype(str) + "/" +
            chunk["PROGRAMA_PPTO"].astype(str) + "/" +
            chunk["PRODUCTO_PROYECTO"].astype(str) + "/" +
            chunk["SEC_FUNC"].astype(str) + "/" +
            chunk["MONTO_DEVENGADO"].astype(str)
        )

        reps = chunk["ID_UNICO"].value_counts()
        chunk["FACTOR_HPL"] = 1.0 / chunk["ID_UNICO"].map(reps)
        chunk["MONTO_AJUSTADO"] = chunk["MONTO_DEVENGADO"] * chunk["FACTOR_HPL"]

        grp = chunk.groupby("PRODUCTO_PROYECTO")["MONTO_AJUSTADO"].sum()

        for proj, v in grp.items():
            sums[proj] = sums.get(proj, 0.0) + float(v)

    df = pd.Series(sums, name=f"DEVENGADO_{year}").to_frame().reset_index()
    df.columns = ["PRODUCTO_PROYECTO", f"DEVENGADO_{year}"]
    return df


# ================================================================
# CELDA 1 — CONSOLIDADO DEVENGADO 2022–2025
# ================================================================

def build_devengado_2022_2025():
    print("📌 Celda 1 — Consolidando devengado 2022–2025...")

    df22 = sum_by_project(BRONZE_GASTO / "2022-Gasto.csv", 2022)
    df23 = sum_by_project(BRONZE_GASTO / "2023-Gasto.csv", 2023)
    df24 = sum_by_project(BRONZE_GASTO / "2024-Gasto-Diario.csv", 2024)
    df25 = sum_by_project(BRONZE_GASTO / "2025-Gasto-Diario.csv", 2025)

    df_wide = (
        df22.merge(df23, on="PRODUCTO_PROYECTO", how="outer")
            .merge(df24, on="PRODUCTO_PROYECTO", how="outer")
            .merge(df25, on="PRODUCTO_PROYECTO", how="outer")
            .sort_values("PRODUCTO_PROYECTO")
    )

    df_wide = clean_devengados(df_wide)

    df_wide = df_wide.rename(columns={"PRODUCTO_PROYECTO": "CODIGO_UNICO"})

    val_cols = ["DEVENGADO_2022", "DEVENGADO_2023", "DEVENGADO_2024", "DEVENGADO_2025"]

    df_filtered = df_wide[df_wide[val_cols].fillna(0).sum(axis=1) > 0]

    df_wide[val_cols] = df_wide[val_cols].fillna(0)
    df_filtered[val_cols] = df_filtered[val_cols].fillna(0)

    out_outer = SILVER_GASTO / "consolidado_devengado_2022_2025_outer.csv"
    out_pos = SILVER_GASTO / "consolidado_devengado_2022_2025_positive.csv"

    df_wide.to_csv(out_outer, index=False, encoding="utf-8-sig")
    df_filtered.to_csv(out_pos, index=False, encoding="utf-8-sig")

    print("📁 CSV outer:", out_outer)
    print("📁 CSV positive:", out_pos)
    print("🔥 Celda 1 completada")


# ================================================================
# CELDA 2 — CSV → PARQUET
# ================================================================

def csv_to_parquet_by_year():
    print("📌 Celda 2 — Generando Parquets...")

    tmp_dir = SILVER_GASTO / "_tmp_parquet"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    files = {
        2022: BRONZE_GASTO / "2022-Gasto.csv",
        2023: BRONZE_GASTO / "2023-Gasto.csv",
        2024: BRONZE_GASTO / "2024-Gasto-Diario.csv",
        2025: BRONZE_GASTO / "2025-Gasto-Diario.csv",
    }

    for year, file in files.items():
        out = tmp_dir / f"{year}.parquet"

        duckdb.sql(f"""
            COPY (
                SELECT *, '{year}' AS ANIO
                FROM read_csv_auto('{file.as_posix()}', HEADER=TRUE, ALL_VARCHAR=TRUE)
            )
            TO '{out.as_posix()}' (FORMAT PARQUET);
        """)

        print(f"   🟢 Parquet {year} creado → {out}")

    print("🔥 Celda 2 completada.")



# ================================================================
# CELDA 3 — UNIFICAR PARQUETS
# ================================================================

def build_parquet_unificado():
    print("📌 Celda 3 — Uniendo Parquets...")

    tmp_dir = SILVER_GASTO / "_tmp_parquet"
    out_final = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT *
            FROM read_parquet('{(tmp_dir / "*.parquet").as_posix()}')
        )
        TO '{out_final.as_posix()}' (FORMAT PARQUET);
    """)

    print("   🎯 Parquet final creado:", out_final)
    print("   🔥 Celda 3 completada.")



# ================================================================
# CELDA 4 — INCONSISTENCIAS
# ================================================================

def extraer_inconsistencias_gasto():
    print("📌 Celda 4 — Detectando inconsistencias...")

    f = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    VAR_CHECK = [
        "FUNCION","FUNCION_NOMBRE",
        "DIVISION_FUNCIONAL","DIVISION_FUNCIONAL_NOMBRE",
        "GRUPO_FUNCIONAL","GRUPO_FUNCIONAL_NOMBRE",
        "NIVEL_GOBIERNO","NIVEL_GOBIERNO_NOMBRE",
        "PLIEGO","PLIEGO_NOMBRE",
        "DEPARTAMENTO_EJECUTORA_NOMBRE",
        "PROVINCIA_EJECUTORA_NOMBRE",
        "DISTRITO_EJECUTORA_NOMBRE"
    ]

    select_cols = ", ".join([f"COUNT(DISTINCT {c}) AS c_{c}" for c in VAR_CHECK])
    having_clause = " OR ".join([f"COUNT(DISTINCT {c}) > 1" for c in VAR_CHECK])

    q = f"""
        SELECT PRODUCTO_PROYECTO AS CODIGO_UNICO, {select_cols}
        FROM read_parquet('{f.as_posix()}')
        GROUP BY PRODUCTO_PROYECTO
        HAVING {having_clause}
    """

    df = duckdb.sql(q).df()

    out = SILVER_GASTO / "inconsistencias_variables_gasto.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ⚠️ Inconsistencias detectadas:", len(df))
    print("🔥 Celda 4 completada.")



# ================================================================
# CELDA 5 — UNIVERSO BASE
# ================================================================

def build_universo_base():
    print("📌 Celda 5 — Construyendo universo base...")

    activos = pd.read_csv(BRONZE_UNIVERSO / "DETALLE_INVERSIONES.csv", dtype=str)
    cierre  = pd.read_csv(BRONZE_UNIVERSO / "CIERRE_INVERSIONES.csv", dtype=str)
    desact  = pd.read_csv(BRONZE_UNIVERSO / "INVERSIONES_DESACTIVADAS.csv", dtype=str)

    activos = activos.rename(columns={
        "DEVEN_ACUMUL_ANIO_ANT": "DEVEN_ACUMULADO_2024"
    })

    desact = desact.rename(columns={
        "COD_SNIP": "CODIGO_SNIP",
        "NOM_UEP": "NOMBRE_UEP",
        "NOM_OPMI": "NOMBRE_OPMI",
        "NOM_UF": "NOMBRE_UF",
        "NOM_UEI": "NOMBRE_UEI",
        "NUM_HABITANTES_BENEF": "BENEFICIARIO",
        "DEVEN_ACUMULADO": "DEVEN_ACUMULADO_2024",
    })

    cierre = cierre.rename(columns={
        "NOM_UEP": "NOMBRE_UEP",
        "NOM_OPMI": "NOMBRE_OPMI",
        "NOM_UF": "NOMBRE_UF",
        "NOM_UEI": "NOMBRE_UEI",
        "DEVEN_ACUMULADO": "DEVEN_ACUMULADO_2024",
    })

    def fix(df):
        if "CODIGO_UNICO" not in df.columns and "CODIGO_SNIP" in df.columns:
            df["CODIGO_UNICO"] = df["CODIGO_SNIP"]
        df["CODIGO_UNICO"] = df["CODIGO_UNICO"].astype(str).str.strip()
        return df

    activos = fix(activos)
    cierre = fix(cierre)
    desact = fix(desact)

    activos["FUENTE"] = "ACTIVOS"
    cierre["FUENTE"]  = "CERRADOS"
    desact["FUENTE"]  = "DESACTIVADOS"

    base = pd.concat([activos, cierre, desact], ignore_index=True)
    base = base.drop_duplicates(subset=["CODIGO_UNICO"])

    out = SILVER_DIR / "proyectos_base_total.csv"
    base.to_csv(out, index=False, encoding="utf-8-sig")

    print("   📁 Guardado:", out)
    print("🔥 Celda 5 completada.")



# ================================================================
# CELDA 6 — TERNA FINAL
# ================================================================

def filtrar_terna_final():
    print("📌 Celda 6 — Filtrando terna final...")

    base = pd.read_csv(SILVER_DIR / "proyectos_base_total.csv", dtype=str)
    gasto = pd.read_csv(SILVER_GASTO / "consolidado_devengado_2022_2025_outer.csv", dtype=str)

    base["CODIGO_UNICO"] = base["CODIGO_UNICO"].astype(str).str.strip()
    gasto["CODIGO_UNICO"] = gasto["CODIGO_UNICO"].astype(str).str.strip()

    df = base.merge(gasto, on="CODIGO_UNICO", how="left")

    val_cols = ["DEVENGADO_2022", "DEVENGADO_2023", "DEVENGADO_2024", "DEVENGADO_2025"]
    for c in val_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["TOTAL_DEV"] = df[val_cols].fillna(0).sum(axis=1)
    df["TIENE_GASTO"] = df["TOTAL_DEV"] > 0

    df["ESTADO"] = df["ESTADO"].astype(str).str.upper()
    activos = df[df["ESTADO"] == "ACTIVO"].copy()

    activos["EMPEZO"] = activos["ANIO_PRIMER_DEVENGADO"].notna()

    G1 = activos["TIENE_GASTO"]
    G2 = (~activos["TIENE_GASTO"]) & (~activos["EMPEZO"])

    df_final = activos[G1 | G2]

    out = SILVER_DIR / "proyectos_terna_final.csv"
    df_final.to_csv(out, index=False, encoding="utf-8-sig")

    print("🔥 Celda 6 completada.")



# ================================================================
# CELDA 7 — VARIABLES MEF
# ================================================================

def enriquecer_con_variables_mef():
    print("📌 Celda 7 — Enriqueciendo con variables MEF...")

    df_final = pd.read_csv(SILVER_DIR / "proyectos_terna_final.csv", dtype=str)
    df_final["CODIGO_UNICO"] = df_final["CODIGO_UNICO"].astype(str).str.strip()

    parquet = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    lista = df_final["CODIGO_UNICO"].dropna().unique().tolist()
    lista_sql = ",".join([f"'{x}'" for x in lista])

    VAR_CHECK = [
        "DIVISION_FUNCIONAL", "DIVISION_FUNCIONAL_NOMBRE",
        "GRUPO_FUNCIONAL", "GRUPO_FUNCIONAL_NOMBRE",
        "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
        "PLIEGO", "PLIEGO_NOMBRE",
        "DEPARTAMENTO_EJECUTORA_NOMBRE",
        "PROVINCIA_EJECUTORA_NOMBRE",
        "DISTRITO_EJECUTORA_NOMBRE"
    ]

    q = f"""
        SELECT DISTINCT
            PRODUCTO_PROYECTO AS CODIGO_UNICO,
            {", ".join(VAR_CHECK)}
        FROM read_parquet('{parquet.as_posix()}')
        WHERE PRODUCTO_PROYECTO IN ({lista_sql})
    """

    df_vars = duckdb.sql(q).df()

    agg = df_vars.groupby("CODIGO_UNICO", as_index=False).agg({c: "first" for c in VAR_CHECK})

    df_enr = df_final.merge(agg, on="CODIGO_UNICO", how="left")

    out = SILVER_DIR / "proyectos_final_enriquecido.csv"
    df_enr.to_csv(out, index=False, encoding="utf-8-sig")

    print("🔥 Celda 7 completada.")



# ================================================================
# CELDA 8 — BRECHAS
# ================================================================

def anexar_brechas():
    print("📌 Celda 8 — Anexando brechas...")

    final = pd.read_csv(SILVER_DIR / "proyectos_final_enriquecido.csv", dtype=str)
    bre = pd.read_csv(BRONZE_BRECHAS / "INVERSIONES_BRECHAS.csv", dtype=str)

    df_long = bre[bre["CODIGO_UNICO"].isin(final["CODIGO_UNICO"])].copy()

    out_long = SILVER_DIR / "proyectos_final_brechas_long.csv"
    df_long.to_csv(out_long, index=False)

    grp = df_long.groupby("CODIGO_UNICO")

    bre_count = grp.size().reset_index(name="N_BRECHAS")

    bre_all = grp["DES_BRECHA"].apply(
        lambda s: "; ".join(sorted(pd.unique(s.dropna())))
    ).reset_index(name="BRECHAS_TODAS")

    resumen = bre_count.merge(bre_all, on="CODIGO_UNICO")

    df_padre = final.merge(resumen, on="CODIGO_UNICO", how="left")

    out = SILVER_DIR / "proyectos_final_enriquecido_brechas.csv"
    df_padre.to_csv(out, index=False, encoding="utf-8-sig")

    print("🔥 Celda 8 completada.")



# ================================================================
# CELDA 9 — EXPANSIÓN POR FUENTE DE FINANCIAMIENTO
# ================================================================

def expandir_por_fuente():
    print("📌 Celda 9 — Expandiendo por FUENTE...")

    proy = pd.read_csv(SILVER_DIR / "proyectos_final_enriquecido_brechas.csv", dtype=str)
    gasto_path = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    g = pd.read_parquet(gasto_path, columns=[
        "PRODUCTO_PROYECTO", "FUENTE_FINANCIAMIENTO",
        "FUENTE_FINANCIAMIENTO_NOMBRE", "MONTO_DEVENGADO", "ANO_EJE"
    ])

    g["CODIGO_UNICO"] = g["PRODUCTO_PROYECTO"].astype(str).str.strip()
    g["FUENTE"] = g["FUENTE_FINANCIAMIENTO"].astype(str).str.strip()
    g["FUENTE_NOMBRE"] = g["FUENTE_FINANCIAMIENTO_NOMBRE"].astype(str).str.strip()

    g["MONTO_DEVENGADO"] = pd.to_numeric(g["MONTO_DEVENGADO"], errors="coerce").fillna(0)
    g["ANO_EJE"] = pd.to_numeric(g["ANO_EJE"], errors="coerce")

    g = g[g["ANO_EJE"].isin([2022, 2023, 2024, 2025])]

    agg = (
        g.groupby(["CODIGO_UNICO", "FUENTE", "FUENTE_NOMBRE", "ANO_EJE"])["MONTO_DEVENGADO"]
         .sum()
         .reset_index()
    )

    pivot = agg.pivot_table(
        index=["CODIGO_UNICO", "FUENTE", "FUENTE_NOMBRE"],
        columns="ANO_EJE",
        values="MONTO_DEVENGADO",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    pivot = pivot.rename(columns={
        2022: "DEV_2022",
        2023: "DEV_2023",
        2024: "DEV_2024",
        2025: "DEV_2025"
    })

    df_real = proy.merge(pivot, on="CODIGO_UNICO", how="left")

    df_final = df_real.copy()

    df_final.to_csv(SILVER_DIR / "proyectos_expandido_FF.csv", index=False, encoding="utf-8-sig")

    print("🔥 Celda 9 completada.")



# ================================================================
# CELDA 10 — FILTRAR MUNICIPALIDADES
# ================================================================

def filtrar_final_no_munis():
    print("📌 Celda 10 — Filtro final...")

    df = pd.read_csv(SILVER_DIR / "proyectos_expandido_FF.csv", dtype=str)

    df["FUENTE_NOMBRE"] = df["FUENTE_NOMBRE"].astype(str).str.lower()

    df_f = df[df["FUENTE_NOMBRE"].isin(["recursos ordinarios", "sin gasto"])]

    patron = r"(?i)municipalidad|mun\."
    df_f = df_f[~df_f["NOMBRE_UEP"].str.contains(patron, na=False)]

    df_f.to_csv(SILVER_DIR / "proyectos_final.csv", index=False, encoding="utf-8-sig")

    print("🔥 Celda 10 completada.")
    print("📁 proyectos_final.csv generado.")



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

    print("\n🚀 PIPELINE COMPLETO.\n")


if __name__ == "__main__":
    run_all()

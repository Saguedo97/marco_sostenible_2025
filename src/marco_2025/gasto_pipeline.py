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
    s = s.astype("string")
    return (
        s.str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )


def sum_by_project(path: Path, year: int, chunk_size: int = 500_000) -> pd.DataFrame:

    sums = {}

    usecols = [
        "ANO_EJE", "MES_EJE", "PROGRAMA_PPTO",
        "PRODUCTO_PROYECTO", "SEC_FUNC", "MONTO_DEVENGADO",
    ]

    for chunk in pd.read_csv(
        path,
        usecols=usecols,
        chunksize=chunk_size,
        dtype={
            "ANO_EJE": "int64",
            "MES_EJE": "int64",
            "PROGRAMA_PPTO": "Int64",
            "PRODUCTO_PROYECTO": "string",
            "SEC_FUNC": "Int64",
            "MONTO_DEVENGADO": "float64",
        },
    ):
        # Filtrar año
        chunk = chunk[chunk["ANO_EJE"] == year]

        # Corte octubre si es 2025
        if year == 2025:
            chunk = chunk[chunk["MES_EJE"] <= 10]

        # Normalizar clave
        chunk["PRODUCTO_PROYECTO"] = normalize_key(chunk["PRODUCTO_PROYECTO"])

        # Quitar nulos críticos
        chunk = chunk.dropna(subset=["PRODUCTO_PROYECTO", "MONTO_DEVENGADO"])

        # -------------------------
        # FACTOR HPL (de-duplicación inteligente)
        # -------------------------
        chunk["ID_UNICO"] = (
            chunk["ANO_EJE"].astype(str) + "/" +
            chunk["PROGRAMA_PPTO"].astype(str) + "/" +
            chunk["PRODUCTO_PROYECTO"].astype(str) + "/" +
            chunk["SEC_FUNC"].astype(str) + "/" +
            chunk["MONTO_DEVENGADO"].astype(str)
        )

        reps = chunk["ID_UNICO"].value_counts()
        chunk["FACTOR_HPL"] = chunk["ID_UNICO"].map(lambda x: 1.0 / reps[x])
        chunk["MONTO_AJUSTADO"] = chunk["MONTO_DEVENGADO"] * chunk["FACTOR_HPL"]

        # Agregar por proyecto
        grp = chunk.groupby("PRODUCTO_PROYECTO")["MONTO_AJUSTADO"].sum()

        for proj, v in grp.items():
            sums[proj] = sums.get(proj, 0.0) + float(v)

    df = pd.Series(sums, name=f"DEVENGADO_{year}").to_frame().reset_index()
    df.columns = ["PRODUCTO_PROYECTO", f"DEVENGADO_{year}"]

    return df



def clean_devengados(df):
    dev_cols = ["DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025"]
    for c in dev_cols:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                     .str.replace("'", "", regex=False)
                     .str.strip()
            )
            df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
    return df


# ================================================================
# CELDA 1 — CONSOLIDADO DEVENGADO 2022–2025
# ================================================================

def build_devengado_2022_2025():
    print("📌 Celda 1 — Consolidando devengado 2022–2025...")

    CHUNK = 500_000

    # -----------------------------
    # UTIL
    # -----------------------------
    def normalize_key(s: pd.Series) -> pd.Series:
        s = s.astype("string")
        return s.str.strip().str.replace(r"\s+", " ", regex=True)

    # -----------------------------
    # SUMAR DEVENGADO POR PROYECTO (HPL)
    # -----------------------------
    def sum_by_project(path: Path, year: int) -> pd.DataFrame:

        sums = {}

        usecols = [
            "ANO_EJE", "MES_EJE", "PROGRAMA_PPTO",
            "PRODUCTO_PROYECTO", "SEC_FUNC", "MONTO_DEVENGADO",
        ]

        for chunk in pd.read_csv(
            path,
            usecols=usecols,
            chunksize=CHUNK,
            dtype={
                "ANO_EJE": "int64",
                "MES_EJE": "int64",
                "PROGRAMA_PPTO": "Int64",
                "PRODUCTO_PROYECTO": "string",
                "SEC_FUNC": "Int64",
                "MONTO_DEVENGADO": "float64",
            },
        ):
            # Filtrar por año
            chunk = chunk[chunk["ANO_EJE"] == year]

            # Filtro para 2025
            if year == 2025:
                chunk = chunk[chunk["MES_EJE"] <= 10]

            # Normalizar claves
            chunk["PRODUCTO_PROYECTO"] = normalize_key(chunk["PRODUCTO_PROYECTO"])

            # Eliminar nulos
            chunk = chunk.dropna(subset=["PRODUCTO_PROYECTO", "MONTO_DEVENGADO"])

            # -------------------------
            # FACTOR HPL
            # -------------------------
            chunk["ID_UNICO"] = (
                chunk["ANO_EJE"].astype(str) + "/" +
                chunk["PROGRAMA_PPTO"].astype(str) + "/" +
                chunk["PRODUCTO_PROYECTO"].astype(str) + "/" +
                chunk["SEC_FUNC"].astype(str) + "/" +
                chunk["MONTO_DEVENGADO"].astype(str)
            )

            reps = chunk["ID_UNICO"].value_counts()
            chunk["FACTOR_HPL"] = chunk["ID_UNICO"].map(lambda x: 1.0 / reps[x])
            chunk["MONTO_AJUSTADO"] = chunk["MONTO_DEVENGADO"] * chunk["FACTOR_HPL"]

            # Agrupar por proyecto
            grp = chunk.groupby("PRODUCTO_PROYECTO")["MONTO_AJUSTADO"].sum()

            for proj, v in grp.items():
                sums[proj] = sums.get(proj, 0.0) + float(v)

        df = pd.Series(sums, name=f"DEVENGADO_{year}").to_frame().reset_index()
        df.columns = ["PRODUCTO_PROYECTO", f"DEVENGADO_{year}"]

        return df

    # -----------------------------
    # Procesar cada año
    # -----------------------------
    df22 = sum_by_project(BRONZE_GASTO / "2022-Gasto.csv", 2022)
    df23 = sum_by_project(BRONZE_GASTO / "2023-Gasto.csv", 2023)
    df24 = sum_by_project(BRONZE_GASTO / "2024-Gasto-Diario.csv", 2024)
    df25 = sum_by_project(BRONZE_GASTO / "2025-Gasto-Diario.csv", 2025)

    # -----------------------------
    # MERGE WIDE
    # -----------------------------
    df_wide = (
        df22.merge(df23, on="PRODUCTO_PROYECTO", how="outer")
            .merge(df24, on="PRODUCTO_PROYECTO", how="outer")
            .merge(df25, on="PRODUCTO_PROYECTO", how="outer")
            .sort_values("PRODUCTO_PROYECTO")
    )

    # -----------------------------
    # LIMPIEZA DEVENGADOS
    # -----------------------------
    def clean_devengados(df):
        cols = ["DEVENGADO_2022", "DEVENGADO_2023", "DEVENGADO_2024", "DEVENGADO_2025"]
        for c in cols:
            if c in df.columns:
                df[c] = (
                    df[c].astype(str)
                         .str.replace("'", "", regex=False)
                         .str.strip()
                )
                df[c] = pd.to_numeric(df[c], errors="coerce").round(2)
        return df

    df_wide = clean_devengados(df_wide)

    # -----------------------------
    # RENOMBRAR A CODIGO_UNICO
    # -----------------------------
    df_wide = df_wide.rename(columns={"PRODUCTO_PROYECTO": "CODIGO_UNICO"})

    # -----------------------------
    # FILTRAR POSITIVE
    # -----------------------------
    val_cols = ["DEVENGADO_2022", "DEVENGADO_2023", "DEVENGADO_2024", "DEVENGADO_2025"]

    df_filtered = df_wide[df_wide[val_cols].fillna(0).sum(axis=1) > 0].reset_index(drop=True)

    df_wide[val_cols] = df_wide[val_cols].fillna(0)
    df_filtered[val_cols] = df_filtered[val_cols].fillna(0)

    # -----------------------------
    # EXPORTAR CSVs
    # -----------------------------
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

    # Carpeta temporal donde se guardarán los .parquet por año
    SILVER_GASTO_TMP = SILVER_GASTO / "_tmp_parquet"
    SILVER_GASTO_TMP.mkdir(parents=True, exist_ok=True)

    # Mapeo año → archivos CSV
    files = {
        2022: BRONZE_GASTO / "2022-Gasto.csv",
        2023: BRONZE_GASTO / "2023-Gasto.csv",
        2024: BRONZE_GASTO / "2024-Gasto-Diario.csv",
        2025: BRONZE_GASTO / "2025-Gasto-Diario.csv",
    }

    for year, file in files.items():
        out = SILVER_GASTO_TMP / f"{year}.parquet"
        print(f"   ⏳ {year} → parquet...")

        duckdb.sql(f"""
            COPY (
                SELECT *, '{year}' AS ANIO
                FROM read_csv_auto(
                    '{file.as_posix()}',
                    HEADER=TRUE,
                    ALL_VARCHAR=TRUE,
                    SAMPLE_SIZE=-1,
                    IGNORE_ERRORS=TRUE
                )
            ) TO '{out.as_posix()}' (FORMAT PARQUET);
        """)

        print(f"   🟢 Parquet {year} creado → {out}")

    print("🔥 Celda 2 completada.")




# ================================================================
# CELDA 3 — UNIFICAR PARQUETS
# ================================================================

def build_parquet_unificado():
    print("📌 Celda 3 — Uniendo Parquets...")

    # Carpeta temporal con muchos .parquet
    tmp_dir = SILVER_GASTO / "_tmp_parquet"

    # Parquet final
    out_final = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    q = f"""
        COPY (
            SELECT DISTINCT *
            FROM parquet_scan('{(tmp_dir / "*.parquet").as_posix()}')
        ) 
        TO '{out_final.as_posix()}' (FORMAT PARQUET);
    """

    duckdb.sql(q)

    print("   🎯 Parquet final creado:", out_final)
    print("   🔥 Celda 3 completada.")


# ================================================================
# CELDA 4 — INCONSISTENCIAS
# ================================================================

# ================================================================
# CELDA 4 — Detectar inconsistencias (versión correcta)
# ================================================================

def extraer_inconsistencias_gasto():

    print("📌 Celda 4 — Detectando inconsistencias...")

    f_parquet = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

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

    # SELECT dinámico
    select_cols = ", ".join([f"COUNT(DISTINCT {c}) AS c_{c}" for c in VAR_CHECK])

    # HAVING dinámico
    having_clause = " OR ".join([f"COUNT(DISTINCT {c}) > 1" for c in VAR_CHECK])

    q = f"""
        SELECT 
            PRODUCTO_PROYECTO AS CODIGO_UNICO,
            {select_cols}
        FROM parquet_scan('{f_parquet.as_posix()}')
        GROUP BY PRODUCTO_PROYECTO
        HAVING {having_clause}
    """

    df = duckdb.sql(q).df()

    # Exportar
    out = SILVER_GASTO / "inconsistencias_variables_gasto.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ⚠️ Inconsistencias detectadas:", len(df))
    print("   📁 Guardado en:", out)
    print("   ✅ Celda 4 completada")



# ================================================================
# CELDA 5 — UNIVERSO BASE
# ================================================================

def build_universo_base():
    print("📌 Celda 5 — Construyendo universo base...")

    # === Lectura ===
    activos = pd.read_csv(BRONZE_UNIVERSO / "DETALLE_INVERSIONES.csv", dtype=str)
    cierre  = pd.read_csv(BRONZE_UNIVERSO / "CIERRE_INVERSIONES.csv", dtype=str)
    desact  = pd.read_csv(BRONZE_UNIVERSO / "INVERSIONES_DESACTIVADAS.csv", dtype=str)

    # === Homologar columnas ===
    # Activos
    activos = activos.rename(columns={
        "DEVEN_ACUMUL_ANIO_ANT": "DEVEN_ACUMULADO_2024"
    })

    # Desactivados
    desact = desact.rename(columns={
        "COD_SNIP": "CODIGO_SNIP",
        "NOM_UEP": "NOMBRE_UEP",
        "NOM_OPMI": "NOMBRE_OPMI",
        "NOM_UF": "NOMBRE_UF",
        "NOM_UEI": "NOMBRE_UEI",
        "NUM_HABITANTES_BENEF": "BENEFICIARIO",
        "DEVEN_ACUMULADO": "DEVEN_ACUMULADO_2024",
    })

    # Cierre
    cierre = cierre.rename(columns={
        "NOM_UEP": "NOMBRE_UEP",
        "NOM_OPMI": "NOMBRE_OPMI",
        "NOM_UF": "NOMBRE_UF",
        "NOM_UEI": "NOMBRE_UEI",
        "DEVEN_ACUMULADO": "DEVEN_ACUMULADO_2024",
    })

    # === Relleno CODIGO_UNICO ===
    for df in (activos, desact, cierre):
        if "CODIGO_UNICO" in df.columns and "CODIGO_SNIP" in df.columns:
            df["CODIGO_UNICO"] = df["CODIGO_UNICO"].fillna(df["CODIGO_SNIP"])
        if "CODIGO_UNICO" not in df.columns and "CODIGO_SNIP" in df.columns:
            df["CODIGO_UNICO"] = df["CODIGO_SNIP"]

        df["CODIGO_UNICO"] = df["CODIGO_UNICO"].astype(str).str.strip()

    # === Año de devengado inicial / final ===
    def to_year(s: pd.Series):
        return s.astype(str).str[:4]

    for df in (activos, desact, cierre):
        if "PRIMER_DEVENGADO" in df.columns:
            df["ANIO_PRIMER_DEVENGADO"] = to_year(df["PRIMER_DEVENGADO"])
        if "ULTIMO_DEVENGADO" in df.columns:
            df["ANIO_ULTIMO_DEVENGADO"] = to_year(df["ULTIMO_DEVENGADO"])

    # === Añadir columna FUENTE (misma del Word) ===
    activos["FUENTE"] = "ACTIVOS"
    desact["FUENTE"]  = "DESACTIVADOS"
    cierre["FUENTE"]  = "CERRADOS"

    # === Concatenar ===
    base = pd.concat([activos, desact, cierre], ignore_index=True)

    # === Eliminar duplicados ===
    base = base.drop_duplicates(subset=["CODIGO_UNICO"])

    # === Exportar ===
    out = SILVER_DIR / "proyectos_base_total.csv"
    base.to_csv(out, index=False, encoding="utf-8-sig")

    print("   ✅ Celda 5 completada.")
    print("   📁 Guardado en:", out)
    print("\n📊 Conteo por FUENTE:")
    print(base["FUENTE"].value_counts(dropna=False))



# ================================================================
# CELDA 6 — TERNA FINAL
# ================================================================

def filtrar_terna_final():
    print("📌 Celda 6 — Filtrando terna final...")

    # === Paths ===
    base_path = SILVER_DIR / "proyectos_base_total.csv"
    gasto_path = SILVER_GASTO / "consolidado_devengado_2022_2025_outer.csv"

    # === Lectura ===
    proyectos = pd.read_csv(base_path, dtype=str)
    gasto = pd.read_csv(gasto_path, dtype=str)

    # === Limpiar claves ===
    proyectos["CODIGO_UNICO"] = proyectos["CODIGO_UNICO"].astype(str).str.strip()
    gasto["CODIGO_UNICO"] = gasto["CODIGO_UNICO"].astype(str).str.strip()

    # === Merge ===
    df = proyectos.merge(gasto, on="CODIGO_UNICO", how="left")

    # === Numéricos devengado ===
    val_cols = ["DEVENGADO_2022", "DEVENGADO_2023", "DEVENGADO_2024", "DEVENGADO_2025"]
    for col in val_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["TOTAL_DEV_22_25"] = df[val_cols].fillna(0).sum(axis=1)
    df["TIENE_GASTO"] = df["TOTAL_DEV_22_25"] > 0

    # === Solo ACTIVOS ===
    df["ESTADO"] = df["ESTADO"].astype(str).str.upper()
    df_activos = df[df["ESTADO"] == "ACTIVO"].copy()

    # === Flags de inicio ===
    df_activos["EMPEZO"] = df_activos["ANIO_PRIMER_DEVENGADO"].notna()

    # ======================================================
    # Grupo 1 — Con gasto
    # ======================================================
    cond_g1 = df_activos["TIENE_GASTO"]

    # ======================================================
    # Grupo 2 — Sin gasto y NO empezó
    # ======================================================
    cond_g2 = (~df_activos["TIENE_GASTO"]) & (~df_activos["EMPEZO"])

    # ======================================================
    # Grupo 3 — Sin gasto pero sí empezó (DESCARTAR)
    # ======================================================
    cond_descartar = (~df_activos["TIENE_GASTO"]) & (df_activos["EMPEZO"])

    # ======================================================
    # Base final = G1 + G2
    # ======================================================
    df_final = df_activos[cond_g1 | cond_g2].copy()

    # === Exportar ===
    out = SILVER_DIR / "proyectos_terna_final.csv"
    df_final.to_csv(out, index=False)

    print("   🟢 Base final guardada:", out)
    print("   🧩 Filas activas:", df_activos.shape[0])
    print("   🧩 Grupo 1 (con gasto):", df_activos[cond_g1].shape[0])
    print("   🧩 Grupo 2 (sin gasto, no empezado):", df_activos[cond_g2].shape[0])
    print("   🧩 Grupo 3 descartado:", df_activos[cond_descartar].shape[0])
    print("   ✅ Celda 6 completada.")


# ================================================================
# CELDA 7 — VARIABLES MEF
# ================================================================

def enriquecer_con_variables_mef():
    print("📌 Celda 7 — Enriqueciendo variables MEF...")

    # ------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------
    proy_final_path = SILVER_DIR / "proyectos_terna_final.csv"
    parquet_gasto = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"

    # Cargar proyectos activos filtrados (grupo 1 + grupo 2)
    df_final = pd.read_csv(proy_final_path, dtype=str)

    # Limpiar claves
    df_final["CODIGO_UNICO"] = df_final["CODIGO_UNICO"].astype(str).str.strip()

    # ------------------------------------------------------------
    # Variables MEF necesarias (idénticas al Word)
    # ------------------------------------------------------------
    VAR_CHECK = [
        "DIVISION_FUNCIONAL", "DIVISION_FUNCIONAL_NOMBRE",
        "GRUPO_FUNCIONAL", "GRUPO_FUNCIONAL_NOMBRE",
        "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
        "PLIEGO", "PLIEGO_NOMBRE",
        "DEPARTAMENTO_EJECUTORA_NOMBRE",
        "PROVINCIA_EJECUTORA_NOMBRE",
        "DISTRITO_EJECUTORA_NOMBRE"
    ]

    KEY = "CODIGO_UNICO"

    # Lista de proyectos activos filtrados
    lista_activos = df_final[KEY].dropna().unique().tolist()

    # Convertir lista a SQL
    lista_sql = ",".join([f"'{x}'" for x in lista_activos])

    # ------------------------------------------------------------
    # 1) EXTRAER VARIABLES SOLO PARA PROYECTOS ACTIVOS FILTRADOS
    # ------------------------------------------------------------
    q = f"""
        SELECT DISTINCT
            PRODUCTO_PROYECTO AS CODIGO_UNICO,
            {", ".join(VAR_CHECK)}
        FROM read_parquet('{parquet_gasto.as_posix()}')
        WHERE PRODUCTO_PROYECTO IN ({lista_sql})
    """

    df_vars = duckdb.sql(q).df()

    print("📌 Variables extraídas SOLO para activos:", df_vars.shape)

    # Limpieza de IDs
    df_vars["CODIGO_UNICO"] = df_vars["CODIGO_UNICO"].astype(str).str.strip()

    # ------------------------------------------------------------
    # 2) AGRUPAR variables por proyecto (sin inconsistencias)
    # ------------------------------------------------------------
    agg_dict = {col: "first" for col in VAR_CHECK}

    df_vars_grouped = (
        df_vars
        .groupby("CODIGO_UNICO", as_index=False)
        .agg(agg_dict)
    )

    print("📌 Variables agrupadas:", df_vars_grouped.shape)

    # ------------------------------------------------------------
    # 3) MERGE con df_final (grupos 1 + 2)
    # ------------------------------------------------------------
    df_enriquecido = df_final.merge(
        df_vars_grouped,
        on="CODIGO_UNICO",
        how="left"
    )

    print("📌 After merge:", df_enriquecido.shape)

    # ------------------------------------------------------------
    # 4) ELIMINAR COLUMNAS DE APOYO (solo si existen)
    # ------------------------------------------------------------
    DROP_COLS = [
        "FUENTE",
        "TOTAL_DEVENGADO_22_25",
        "TOTAL_DEV_22_25",
        "EMPEZO",
        "TIENE_GASTO",
        "CATEGORIA_FILTRO",
        "INCONSISTENTE",
        "RESPONSABLE_OPMI",
        "RESPONSABLE_UEI",
        "RESP_NOMBRE_UF",
        "OPI",
        "RESPONSABLE_OPI",
        "SUBPROGRAM",
        "DEVENGADO_ACUMULADO",
        "ANIO_PROC",
        "FEC_CIERRE",
        "DES_CIERRE",
        "CULMINADA",
        "INICIO_EJEC_FISICA",
        "CULMINACION_EJEC_FISICA",
        "FEC_INI_OPER",
        "DES_SERVICIO",
        "DES_BRECHA",
        "DES_UM",
        "DES_ESPACIO_GEO",
        "CONTRIB_CIERRE_BRECHA",
        "TOTAL_LIQUIDACION",
        "FEC_LIQUIDACION",
        "UNIDAD_OPE_MANT",
        "FUENTE_FINANCIAMIENTO",
        "FEC_TRANSFERENCIA"
    ]

    df_enriquecido = df_enriquecido.drop(
        columns=[c for c in DROP_COLS if c in df_enriquecido.columns]
    )

    # ------------------------------------------------------------
    # 5) EXPORTAR RESULTADO FINAL
    # ------------------------------------------------------------
    out_path = SILVER_DIR / "proyectos_final_enriquecido.csv"
    df_enriquecido.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("✅ Archivo final enriquecido guardado en:", out_path)
    print("📊 Filas:", df_enriquecido.shape[0], "| Columnas:", df_enriquecido.shape[1])



# ================================================================
# CELDA 8 — BRECHAS
# ================================================================

def anexar_brechas():
    print("📌 Celda 8 — Agregando brechas...")

    # ------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------
    final_path = SILVER_DIR / "proyectos_final_enriquecido.csv"
    brechas_path = BRONZE_BRECHAS / "INVERSIONES_BRECHAS.csv"

    out_long = SILVER_DIR / "proyectos_final_brechas_long.csv"
    out_padre = SILVER_DIR / "proyectos_final_enriquecido_brechas.csv"

    # ------------------------------------------------------------
    # 1) Leer datos
    # ------------------------------------------------------------
    df_final = pd.read_csv(final_path, dtype=str)
    bre = pd.read_csv(brechas_path, dtype=str)

    # ------------------------------------------------------------
    # 2) Filtrar solo brechas de proyectos activos (grupo 1 + 2)
    # ------------------------------------------------------------
    df_long = bre[bre["CODIGO_UNICO"].isin(df_final["CODIGO_UNICO"])].copy()

    # Exportar formato LONG
    df_long.to_csv(out_long, index=False)

    # ------------------------------------------------------------
    # 3) RESUMEN — TODAS LAS BRECHAS POR PROYECTO
    # ------------------------------------------------------------
    grp = df_long.groupby("CODIGO_UNICO", dropna=False)

    # Número de brechas por proyecto
    bre_count = grp.size().reset_index(name="N_BRECHAS")

    # Todas las brechas únicas (orden alfabético, separadas por ;)
    bre_all = grp["DES_BRECHA"].apply(
        lambda s: "; ".join(sorted(pd.unique(s.dropna())))
    ).reset_index(name="BRECHAS_TODAS")

    # Resumen final
    bre_resumen = bre_count.merge(bre_all, on="CODIGO_UNICO")

    # ------------------------------------------------------------
    # 4) Merge con el padre
    # ------------------------------------------------------------
    df_padre_brechas = df_final.merge(bre_resumen, on="CODIGO_UNICO", how="left")
    df_padre_brechas.to_csv(out_padre, index=False)

    # ------------------------------------------------------------
    # 5) Chequeos
    # ------------------------------------------------------------
    print("✅ Exportados:")
    print(" - Brechas LONG:", out_long, "filas:", df_long.shape[0])
    print(" - Padre + resumen:", out_padre, "filas:", df_padre_brechas.shape[0])

    n_total = df_padre_brechas.shape[0]
    n_con_brecha = df_padre_brechas["N_BRECHAS"].fillna(0).gt(0).sum()

    print(f"\n Cobertura: {n_con_brecha}/{n_total} proyectos ({100*n_con_brecha/n_total:.2f}%)")
    print("📊 Promedio brechas/proyecto:", df_padre_brechas["N_BRECHAS"].fillna(0).astype(int).mean())

    print("\n📊 Top 10 brechas más comunes:")
    print(df_long["DES_BRECHA"].value_counts().head(10))

    print("   ✅ Celda 8 completada.")



# ================================================================
# CELDA 9 — EXPANDIR POR FUENTE
# ================================================================

def expandir_por_fuente():
    print("📌 Celda 9 — Expandiendo por Fuente de Financiamiento...")

    # ================================================================
    # PATHS
    # ================================================================
    proy_path  = SILVER_DIR / "proyectos_final_enriquecido_brechas.csv"
    gasto_path = SILVER_GASTO / "Gasto_Consolidado_2022_2025.parquet"
    out_path   = SILVER_DIR / "proyectos_expandido_FF.csv"

    # ================================================================
    # 1) UNIVERSO FINAL (enriquecido con brechas)
    # ================================================================
    proy = pd.read_csv(proy_path, dtype=str)
    proy["CODIGO_UNICO"] = proy["CODIGO_UNICO"].astype(str).str.strip()

    # ================================================================
    # 2) CARGAR GASTO DESDE PARQUET
    # ================================================================
    usecols = [
        "PRODUCTO_PROYECTO",
        "FUENTE_FINANCIAMIENTO",
        "FUENTE_FINANCIAMIENTO_NOMBRE",
        "MONTO_DEVENGADO",
        "ANO_EJE"
    ]

    g = pd.read_parquet(gasto_path, columns=usecols)

    g["CODIGO_UNICO"] = g["PRODUCTO_PROYECTO"].astype(str).str.strip()
    g["FUENTE"] = g["FUENTE_FINANCIAMIENTO"].astype(str).str.strip()
    g["FUENTE_NOMBRE"] = g["FUENTE_FINANCIAMIENTO_NOMBRE"].astype(str).str.strip()

    g["MONTO_DEVENGADO"] = pd.to_numeric(g["MONTO_DEVENGADO"], errors="coerce").fillna(0)
    g["ANO_EJE"] = pd.to_numeric(g["ANO_EJE"], errors="coerce")

    g = g[g["ANO_EJE"].isin([2022, 2023, 2024, 2025])]

    # ================================================================
    # 3) AGREGAR POR PROYECTO × FUENTE × AÑO
    # ================================================================
    agg = (
        g.groupby(["CODIGO_UNICO", "FUENTE", "FUENTE_NOMBRE", "ANO_EJE"])["MONTO_DEVENGADO"]
         .sum()
         .reset_index()
    )

    # ================================================================
    # 4) PIVOTEAR (solo fuentes reales)
    # ================================================================
    pivot = agg.pivot_table(
        index=["CODIGO_UNICO", "FUENTE", "FUENTE_NOMBRE"],
        columns="ANO_EJE",
        values="MONTO_DEVENGADO",
        aggfunc="sum",
        fill_value=0.0
    ).reset_index()

    pivot = pivot.rename(columns={
        2022: "DEV_2022",
        2023: "DEV_2023",
        2024: "DEV_2024",
        2025: "DEV_2025"
    })

    # ================================================================
    # 5) MERGE — EXPANSIÓN POR FF REAL
    # ================================================================
    df_real = proy.merge(pivot, on="CODIGO_UNICO", how="left")

    # ================================================================
    # 6) PROYECTOS SIN GASTO → Fila Única
    # ================================================================
    proy_con_ff = df_real[df_real["FUENTE"].notna()]["CODIGO_UNICO"].unique()
    proy_sin_ff = proy.loc[~proy["CODIGO_UNICO"].isin(proy_con_ff), "CODIGO_UNICO"]

    df_sin = proy[proy["CODIGO_UNICO"].isin(proy_sin_ff)].copy()
    df_sin["FUENTE"] = "0"
    df_sin["FUENTE_NOMBRE"] = "SIN GASTO"
    df_sin["DEV_2022"] = 0.0
    df_sin["DEV_2023"] = 0.0
    df_sin["DEV_2024"] = 0.0
    df_sin["DEV_2025"] = 0.0

    # ================================================================
    # 7) CONCAT FINAL
    # ================================================================
    df_final = pd.concat(
        [df_real[df_real["FUENTE"].notna()], df_sin],
        ignore_index=True
    ).sort_values(["CODIGO_UNICO", "FUENTE"]).reset_index(drop=True)

    # ================================================================
    # 8) LIMPIAR COLUMNAS QUE YA NO VAN
    # ================================================================
    DROP_COLS = [
        "DEVENGADO_2022",
        "DEVENGADO_2023",
        "DEVENGADO_2024",
        "DEVENGADO_2025",
        "TOTAL_DEV_22_25",
        "TIENE_GASTO",
        "EMPEZO",
        "PRODUCTO_PROYECTO",
        "FUENTE_FINANCIAMIENTO",
        "FUENTE_FINANCIAMIENTO_NOMBRE"
    ]

    df_final = df_final.drop(columns=[c for c in DROP_COLS if c in df_final.columns])

    # ================================================================
    # 9) REORDENAR COLUMNAS
    # ================================================================
    ORDER = [
        "CODIGO_UNICO",
        "NOMBRE_INVERSION",
        "FUENTE",
        "FUENTE_NOMBRE",
        "DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025",
        "MONTO_VIABLE",
        "COSTO_ACTUALIZADO",
        "ALTERNATIVA",
        "SECTOR",
        "NOMBRE_UEP",
        "DEPARTAMENTO",
        "PROVINCIA",
        "DISTRITO",
        "UBIGEO",
        "BENEFICIARIO",
        "N_BRECHAS",
        "BRECHAS_TODAS"
    ]

    primero = [c for c in ORDER if c in df_final.columns]
    resto   = [c for c in df_final.columns if c not in ORDER]
    df_final = df_final[primero + resto]

    # ================================================================
    # 9B) LIMPIAR Y CONVERTIR A INT
    # ================================================================
    INT_COLS = [
        "CODIGO_UNICO", "FUENTE", "DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025",
        "MONTO_VIABLE", "COSTO_ACTUALIZADO", "UBIGEO", "BENEFICIARIO",
        "N_BRECHAS", "CODIGO_SNIP", "CTRL_CONCURR", "MONTO_LAUDO", "MONTO_FIANZA",
        "PMI_ANIO_1", "PMI_ANIO_2", "PMI_ANIO_3", "PMI_ANIO_4",
        "PRIMER_DEVENGADO", "ULTIMO_DEVENGADO", "DEVEN_ACUMULADO_2024",
        "DEV_ANIO_ACTUAL",
        "DEV_ENE_ANIO_VIG", "DEV_FEB_ANIO_VIG", "DEV_MAR_ANIO_VIG",
        "DEV_ABR_ANIO_VIG", "DEV_MAY_ANIO_VIG", "DEV_JUN_ANIO_VIG",
        "DEV_JUL_ANIO_VIG", "DEV_AGO_ANIO_VIG", "DEV_SET_ANIO_VIG",
        "DEV_OCT_ANIO_VIG", "DEV_NOV_ANIO_VIG", "DEV_DIC_ANIO_VIG",
        "CERTIF_ANIO_ACTUAL", "COMPROM_ANUAL_ANIO_ACTUAL", "SALDO_EJECUTAR",
        "AVANCE_FISICO", "AVANCE_EJECUCION",
        "PROG_ACTUAL_ANIO_ACTUAL", "MONTO_VALORIZACION", "MONTO_ET_F8",
        "ANIO_PROCESO", "ANIO_PRIMER_DEVENGADO", "ANIO_ULTIMO_DEVENGADO"
    ]

    for c in INT_COLS:
        if c in df_final.columns:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").astype("float64")

    # ================================================================
    # 10) EXPORTAR
    # ================================================================
    df_final.to_csv(out_path, index=False, encoding="utf-8-sig")

    print("   ✅ EXPANDIDO POR FF generado:", out_path)
    print("   📌 Tamaño final:", df_final.shape)



# ================================================================
# CELDA 10 — FILTRAR MUNICIPALIDADES
# ================================================================

def filtrar_final_no_munis():
    print("📌 Celda 10 — Filtrando UEPs y fuentes...")

    # ===== Paths =====
    path_in = SILVER_DIR / "proyectos_expandido_FF.csv"
    path_out = SILVER_DIR / "proyectos_final.csv"

    # ===== 1. Cargar CSV =====
    df = pd.read_csv(path_in, dtype=str, low_memory=False)

    # ===== 2. Filtrar por Recursos Ordinarios o Sin Gasto =====
    df_filtrado = df[df["FUENTE_NOMBRE"].str.lower().isin(
        ["recursos ordinarios", "sin gasto"]
    )]

    # ===== 3. Excluir municipalidades =====
    patron_muni = r"(?i)municipalidad|mun\."
    df_filtrado = df_filtrado[
        ~df_filtrado["NOMBRE_UEP"].str.contains(patron_muni, na=False)
    ]

    # ===== 4. Guardar resultado =====
    df_filtrado.to_csv(path_out, index=False, encoding="utf-8-sig")

    print(f"   🟢 Proyectos finales: {len(df_filtrado)}")
    print(f"   📁 Archivo guardado en: {path_out}")
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

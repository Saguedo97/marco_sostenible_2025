from difflib import SequenceMatcher
from .paths import SEEDS_FILE, SILVER_DIR, CFG


def clasificador_marco2025():
    import re, unicodedata, difflib, math, time
    import pandas as pd
    import numpy as np
    from tqdm import tqdm

    # ======================================================
    # CONFIG (adaptado a tu proyecto)
    # ======================================================
    SEEDS_XLSX = SEEDS_FILE
    BIG_CSV = SILVER_DIR / "proyectos_final.csv"
    OUT_CSV = BIG_CSV.with_name(BIG_CSV.stem + "_clasificado.csv")

    BRECHA_COL = "BRECHAS_TODAS"

    USE_EMBEDDINGS = CFG["nlp"]["use_embeddings"]
    EMB_MODEL = CFG["nlp"]["embedding_model"]

    FUZZY_MIN = CFG["nlp"]["fuzzy_min"]
    EMB_MIN_STRONG = CFG["nlp"]["embedding_min_strong"]
    BATCH_SIZE = CFG["nlp"]["batch_size"]

    # ======================================================
    # NORMALIZACIÓN
    # ======================================================
    def nrm(s: str) -> str:
        s = "" if s is None else str(s)
        s = s.lower().strip()
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        s = re.sub(r"\s+", " ", s)
        return s

    def split_brechas(val: str):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return []
        s = str(val)
        s = re.sub(r"[;\n\r\t/]+", "|", s)
        s = re.sub(r"[•\u2022]+", "|", s)
        parts = re.split(r"\|+|,", s)
        return [nrm(p) for p in parts if p and p.strip()]

    # ======================================================
    # LECTURA DE SEMILLAS
    # ======================================================
    print("📘 Cargando semillas…")
    seeds = pd.read_excel(SEEDS_XLSX)

    COL_NOMBRE = "Nombre del indicador"
    COL_MARCO_CAT = "MARCO_CATEGORIA"
    COL_MARCO_SUB = "MARCO_SUBCATEGORIA"
    COL_SECTOR = "Sector Elegible"

    seeds["seed_norm"] = seeds[COL_NOMBRE].map(nrm)
    seeds_norm_list = seeds["seed_norm"].tolist()

    # ======================================================
    # EMBEDDINGS (si aplica)
    # ======================================================
    seed_vecs = None
    emb_model = None

    if USE_EMBEDDINGS:
        try:
            from sentence_transformers import SentenceTransformer
            emb_model = SentenceTransformer(EMB_MODEL)
            seed_vecs = emb_model.encode(
                seeds_norm_list,
                show_progress_bar=True,
                normalize_embeddings=True
            ).astype("float32")
        except Exception as e:
            print(f"[⚠] Error cargando embeddings: {e} → usando solo fuzzy.")
            USE_EMBEDDINGS = False

    # ======================================================
    # MATCHING
    # ======================================================

    def best_fuzzy(q: str):
        if not q:
            return -1, 0.0

        ratios = [SequenceMatcher(None, q, s).ratio() for s in seeds_norm_list]
        idx = int(np.argmax(ratios))
        best = ratios[idx]

        if best >= FUZZY_MIN:
            return idx, float(best)

        return -1, 0.0


    def best_embedding(q: str):
        if not USE_EMBEDDINGS or not q:
            return -1, 0.0
        qv = emb_model.encode([q], normalize_embeddings=True)[0].astype("float32")
        sims = seed_vecs @ qv
        idx = int(np.argmax(sims))
        return idx, float(sims[idx])

    # ======================================================
    # CLASIFICAR UNA FILA
    # ======================================================
    def classify_row(cell_val: str):
        chunks = split_brechas(cell_val)

        if not chunks:
            return {
                "MARCO_CATEGORIA": None,
                "MARCO_SUBCATEGORIA": None,
                "Sector_Elegible": None,
                "Match_Seed": None,
                "Match_Method": "no_brecha",
                "Match_Score": 0.0,
                "Matched_Chunk": None,
            }

        votes = {}
        best_global = dict(cat=None, sub=None, sector=None,
                           seed=None, method="none", score=0.0, chunk=None)

        for ch in chunks:
            if not ch:
                continue

            idx_fz, sc_fz = best_fuzzy(ch)
            idx_emb, sc_emb = best_embedding(ch) if USE_EMBEDDINGS else (-1, 0.0)

            idx, sc, method = -1, 0.0, "none"

            # ----- Embedding domina -----
            if USE_EMBEDDINGS and sc_emb >= EMB_MIN_STRONG and sc_emb >= sc_fz:
                idx, sc, method = idx_emb, sc_emb, "embedding"
            # ----- Fuzzy válido -----
            elif sc_fz >= FUZZY_MIN:
                idx, sc, method = idx_fz, sc_fz, "fuzzy"
            else:
                continue

            row_seed = seeds.iloc[idx]
            key = (row_seed[COL_MARCO_CAT], row_seed[COL_MARCO_SUB], row_seed[COL_SECTOR])
            votes[key] = votes.get(key, 0.0) + sc

            if sc > best_global["score"]:
                best_global.update({
                    "cat": row_seed[COL_MARCO_CAT],
                    "sub": row_seed[COL_MARCO_SUB],
                    "sector": row_seed[COL_SECTOR],
                    "seed": row_seed[COL_NOMBRE],
                    "method": method,
                    "score": sc,
                    "chunk": ch,
                })

        if not votes:
            return {
                "MARCO_CATEGORIA": None,
                "MARCO_SUBCATEGORIA": None,
                "Sector_Elegible": None,
                "Match_Seed": None,
                "Match_Method": "no_match",
                "Match_Score": 0.0,
                "Matched_Chunk": None,
            }

        cat_best, sub_best, sect_best = max(votes, key=votes.get)

        # Flag de revisar embeddings débiles
        if best_global["method"] == "embedding" and best_global["score"] < 0.50:
            cat_best = "REVISAR"

        return {
            "MARCO_CATEGORIA": cat_best,
            "MARCO_SUBCATEGORIA": sub_best,
            "Sector_Elegible": sect_best,
            "Match_Seed": best_global["seed"],
            "Match_Method": best_global["method"],
            "Match_Score": round(best_global["score"], 3),
            "Matched_Chunk": best_global["chunk"],
        }

    # ======================================================
    # PROCESAMIENTO POR LOTES
    # ======================================================
    print("📂 Cargando universo a clasificar…")
    big = pd.read_csv(BIG_CSV, dtype=str)
    n = len(big)
    print(f"Total filas: {n}")

    results = []
    start = time.time()

    for start_i in tqdm(range(0, n, BATCH_SIZE), desc="🔥 Clasificando"):
        end_i = min(start_i + BATCH_SIZE, n)

        batch = big.iloc[start_i:end_i]
        batch_res = batch[BRECHA_COL].apply(classify_row).apply(pd.Series)
        results.append(batch_res)

        # Guardado incremental
        partial = pd.concat(
            [big.iloc[:end_i].reset_index(drop=True),
             pd.concat(results).reset_index(drop=True)],
            axis=1
        )

        partial_path = OUT_CSV.with_name(
    OUT_CSV.stem + f"_partial_{end_i}.csv"
)

        partial.to_csv(partial_path, index=False, encoding="utf-8-sig")

    # ===== Archivo final =====
    final = pd.concat(
        [big.reset_index(drop=True),
         pd.concat(results).reset_index(drop=True)],
        axis=1
    )

    # ======================================================
    # ORDENAR COLUMNAS
    # ======================================================
    ORDER = [
        "CODIGO_UNICO",
        "NOMBRE_INVERSION",
        "FUENTE",
        "FUENTE_NOMBRE",
        "DEV_2022", "DEV_2023", "DEV_2024", "DEV_2025",
        "MARCO_CATEGORIA", "MARCO_SUBCATEGORIA", "Sector_Elegible",
        "MONTO_VIABLE", "COSTO_ACTUALIZADO", "ALTERNATIVA",
        "NOMBRE_UEP", "SECTOR",
        "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
        "UBIGEO", "BENEFICIARIO",
        "N_BRECHAS", "BRECHAS_TODAS",
        "Match_Seed", "Match_Method", "Match_Score", "Matched_Chunk"
    ]

    cols_ordered = [c for c in ORDER if c in final.columns]
    cols_extra   = [c for c in final.columns if c not in ORDER]
    final = final[cols_ordered + cols_extra]

    # ======================================================
    # GUARDAR CSV FINAL
    # ======================================================
    final.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    mins = round((time.time() - start) / 60, 2)
    print("📐 Columnas ordenadas.")
    print("✅ Guardado final CSV:", OUT_CSV)
    print(f"⏱ Tiempo total: {mins} min")

if __name__ == "__main__":
    clasificador_marco2025()

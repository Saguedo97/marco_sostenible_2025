"""
CLASIFICADOR NLP — MARCO 2025
"""

import pandas as pd
import numpy as np
import re, unicodedata, math, time
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from difflib import SequenceMatcher

from .paths import SEEDS_FILE, SILVER_DIR, CFG


USE_EMB = CFG["nlp"]["use_embeddings"]
EMB_MODEL = CFG["nlp"]["embedding_model"]
FUZZY_MIN = CFG["nlp"]["fuzzy_min"]
EMB_STRONG = CFG["nlp"]["embedding_min_strong"]
BATCH_SIZE = CFG["nlp"]["batch_size"]

BRECHA_COL = "BRECHAS_TODAS"


# ====================================================
# NORMALIZAR TEXTO
# ====================================================
def nrm(s):
    if s is None:
        return ""
    s = str(s).lower().strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s)
    return s

def split_brechas(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return []
    s = re.sub(r"[;\n\r\t/]+", "|", str(val))
    parts = [nrm(p) for p in re.split(r"[|,]+", s)]
    return [p for p in parts if p.strip()]


# ====================================================
# PRINCIPAL
# ====================================================
def clasificar_marco2025():
    print("📘 Cargando semillas…")
    seeds = pd.read_excel(SEEDS_FILE)
    seeds["seed_norm"] = seeds["Nombre del indicador"].map(nrm)
    seed_norm_list = seeds["seed_norm"].tolist()

    print("🧠 Cargando embeddings…")
    model = SentenceTransformer(EMB_MODEL) if USE_EMB else None
    seed_vecs = (
        model.encode(seed_norm_list, normalize_embeddings=True).astype("float32")
        if USE_EMB
        else None
    )

    def best_fuzzy(q):
        ratios = [SequenceMatcher(None, q, s).ratio() for s in seed_norm_list]
        idx = int(np.argmax(ratios))
        score = ratios[idx]
        return (idx, score) if score >= FUZZY_MIN else (-1, 0.0)

    def best_emb(q):
        if not USE_EMB:
            return (-1, 0.0)
        qv = model.encode([q], normalize_embeddings=True)[0]
        sims = seed_vecs @ qv
        idx = int(np.argmax(sims))
        score = sims[idx]
        return (idx, score)

    # ============================
    # CARGAR UNIVERSE
    # ============================
    big_path = SILVER_DIR / "proyectos_terna_final.csv"
    big = pd.read_csv(big_path, dtype=str)
    N = len(big)

    results = []
    start = time.time()

    for start_i in tqdm(range(0, N, BATCH_SIZE)):
        batch = big.iloc[start_i : start_i + BATCH_SIZE]
        batch_res = batch[BRECHA_COL].apply(lambda x: clasificar_brecha(x, seeds, best_fuzzy, best_emb))
        results.append(batch_res.apply(pd.Series))

    final = pd.concat([big.reset_index(drop=True), pd.concat(results).reset_index(drop=True)], axis=1)
    out_path = SILVER_DIR / "proyectos_terna_final_clasificado.csv"
    final.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"✅ Archivo final guardado en {out_path}")
    print(f"⏱ Tiempo total: {(time.time() - start)/60:.2f} minutos")


def clasificar_brecha(brechas, seeds, best_fuzzy, best_emb):
    chunks = split_brechas(brechas)
    if not chunks:
        return {"MARCO_CATEGORIA": None, "MARCO_SUBCATEGORIA": None, "Sector_Elegible": None}

    votos = {}
    best_global = ("", 0, None)  # (seed_name, score, category)

    for ch in chunks:
        idx_f, sc_f = best_fuzzy(ch)
        idx_e, sc_e = best_emb(ch)

        if sc_e >= EMB_STRONG and sc_e >= sc_f:
            idx, sc = idx_e, sc_e
        elif sc_f >= FUZZY_MIN:
            idx, sc = idx_f, sc_f
        else:
            continue

        seed = seeds.iloc[idx]
        cat = seed["MARCO_CATEGORIA"]
        sub = seed["MARCO_SUBCATEGORIA"]
        sect = seed["Sector Elegible"]

        key = (cat, sub, sect)
        votos[key] = votos.get(key, 0) + sc

        if sc > best_global[1]:
            best_global = (seed["Nombre del indicador"], sc, key)

    if not votos:
        return {"MARCO_CATEGORIA": None, "MARCO_SUBCATEGORIA": None, "Sector_Elegible": None}

    final_cat, final_sub, final_sect = max(votos, key=votos.get)

    return {
        "MARCO_CATEGORIA": final_cat,
        "MARCO_SUBCATEGORIA": final_sub,
        "Sector_Elegible": final_sect,
        "Match_Seed": best_global[0],
        "Match_Score": round(best_global[1], 3),
    }


if __name__ == "__main__":
    clasificar_marco2025()

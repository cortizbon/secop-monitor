from __future__ import annotations

import importlib
from functools import lru_cache

import pandas as pd

from pipeline.config import SEMANTIC_MODEL_NAME


@lru_cache(maxsize=1)
def load_model():
    try:
        module = importlib.import_module("sentence_transformers")
    except Exception:
        return None
    return module.SentenceTransformer(SEMANTIC_MODEL_NAME)


def fallback_search(dataframe: pd.DataFrame, query: str, top_k: int = 50) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    tokens = [token.strip().lower() for token in query.split() if token.strip()]
    if not tokens:
        return dataframe.head(0).copy()

    text = dataframe["contract_object"].fillna("").astype(str).str.lower()
    scored = dataframe.copy()
    scored["semantic_score"] = sum(text.str.count(token) for token in tokens)
    scored = scored[scored["semantic_score"] > 0].sort_values(
        ["semantic_score", "amount_value", "reference_date"],
        ascending=[False, False, False],
    )
    return scored.head(top_k).reset_index(drop=True)


def build_embeddings(dataframe: pd.DataFrame, text_column: str = "contract_object"):
    model = load_model()
    if model is None or dataframe.empty:
        return None

    texts = dataframe[text_column].fillna("").astype(str).tolist()
    return model.encode(
        texts,
        convert_to_tensor=True,
        batch_size=64,
        show_progress_bar=False,
    )



def semantic_search_dataframe(
    dataframe: pd.DataFrame,
    query: str,
    *,
    top_k: int,
    similarity_min: float,
) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    model = load_model()
    embeddings = build_embeddings(dataframe)
    if model is None or embeddings is None:
        return fallback_search(dataframe, query, top_k=top_k)

    try:
        cos_sim = importlib.import_module("sentence_transformers.util").cos_sim
    except Exception:
        return fallback_search(dataframe, query, top_k=top_k)

    query_embedding = model.encode(query, convert_to_tensor=True)
    similarities = cos_sim(query_embedding, embeddings)[0].cpu().numpy()

    result = dataframe.copy()
    result["semantic_score"] = similarities
    result = result[result["semantic_score"] >= similarity_min]
    result = result.sort_values(
        ["semantic_score", "amount_value", "reference_date"],
        ascending=[False, False, False],
    )
    return result.head(top_k).reset_index(drop=True)

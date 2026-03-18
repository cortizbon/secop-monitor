from __future__ import annotations

import importlib
from collections.abc import Iterable
from functools import lru_cache

import pandas as pd

from pipeline.config import SEMANTIC_MODEL_NAME, SEMANTIC_SEARCH_MODULE, SEMANTIC_SIMILARITY_MIN, SEMANTIC_TOP_K


def _load_external_searcher():
    if not SEMANTIC_SEARCH_MODULE:
        return None
    module = importlib.import_module(SEMANTIC_SEARCH_MODULE)
    return getattr(module, "search_contracts", None)


def _fallback_search(dataframe: pd.DataFrame, query: str, top_k: int = 50) -> pd.DataFrame:
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
    return scored.head(top_k)


@lru_cache(maxsize=1)
def load_model():
    try:
        module = importlib.import_module("sentence_transformers")
    except Exception:
        return None
    return module.SentenceTransformer(SEMANTIC_MODEL_NAME)


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


def _semantic_from_embeddings(
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
        return _fallback_search(dataframe, query, top_k=top_k)

    try:
        cos_sim = importlib.import_module("sentence_transformers.util").cos_sim
    except Exception:
        return _fallback_search(dataframe, query, top_k=top_k)

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


def _coerce_external_result(dataframe: pd.DataFrame, result) -> pd.DataFrame:
    if isinstance(result, pd.DataFrame):
        return result.copy()

    if isinstance(result, Iterable):
        result_list = list(result)
        if not result_list:
            return dataframe.head(0).copy()

        if isinstance(result_list[0], dict):
            result_df = pd.DataFrame(result_list)
            if "contract_uid" in result_df.columns:
                return dataframe.merge(result_df, on="contract_uid", how="inner")
            return result_df

        candidate_ids = {str(item) for item in result_list}
        return dataframe[dataframe["contract_uid"].astype(str).isin(candidate_ids)].copy()

    return dataframe.head(0).copy()


def search_contracts(
    dataframe: pd.DataFrame,
    query: str,
    top_k: int | None = None,
    similarity_min: float | None = None,
) -> pd.DataFrame:
    top_k = top_k or SEMANTIC_TOP_K
    similarity_min = SEMANTIC_SIMILARITY_MIN if similarity_min is None else similarity_min

    external_search = _load_external_searcher()
    if external_search is not None:
        result = external_search(query=query, records=dataframe.to_dict(orient="records"), top_k=top_k)
        coerced = _coerce_external_result(dataframe, result)
        if not coerced.empty:
            return coerced

    return _semantic_from_embeddings(
        dataframe,
        query,
        top_k=top_k,
        similarity_min=similarity_min,
    )

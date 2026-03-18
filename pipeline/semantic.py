from __future__ import annotations

import importlib
from collections.abc import Iterable

import pandas as pd

from pipeline.config import SEMANTIC_SEARCH_MODULE, SEMANTIC_SIMILARITY_MIN, SEMANTIC_TOP_K
from pipeline.semantic_engine import semantic_search_dataframe


def _load_external_searcher():
    if not SEMANTIC_SEARCH_MODULE:
        return None
    module = importlib.import_module(SEMANTIC_SEARCH_MODULE)
    return getattr(module, "search_contracts", None)


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

    return semantic_search_dataframe(
        dataframe,
        query,
        top_k=top_k,
        similarity_min=similarity_min,
    )

from __future__ import annotations

import importlib
from collections.abc import Iterable

import pandas as pd

from pipeline.config import SEMANTIC_SEARCH_MODULE



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



def search_contracts(dataframe: pd.DataFrame, query: str, top_k: int = 50) -> pd.DataFrame:
    external_search = _load_external_searcher()
    if external_search is None:
        return _fallback_search(dataframe, query, top_k=top_k)

    result = external_search(query=query, records=dataframe.to_dict(orient="records"), top_k=top_k)

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

"""
extract.py — Extração de dados da World Bank API v2.

Implementa paginação completa e retry com backoff exponencial.
"""

import time
import logging
import requests

from src.config import (
    WB_BASE_URL,
    INDICATORS,
    COUNTRIES_PER_PAGE,
    INDICATORS_PER_PAGE,
    MRV,
    MAX_RETRIES,
    BACKOFF_FACTOR,
)

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────

def _request_with_retry(url: str, params: dict) -> dict | None:
    """
    Executa GET com retry e backoff exponencial.
    Retorna o JSON da resposta ou None em caso de falha permanente.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            wait = BACKOFF_FACTOR ** attempt
            logger.warning(
                "Tentativa %d/%d falhou para %s — erro: %s. "
                "Aguardando %ds antes do retry.",
                attempt, MAX_RETRIES, url, exc, wait,
            )
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    logger.error("Falha permanente após %d tentativas: %s", MAX_RETRIES, url)
    return None


# ── Extração de países ─────────────────────────────────────────

def extract_countries() -> list[dict]:
    """
    Retorna a lista completa de países/entidades da API.
    Lida com paginação caso haja mais de COUNTRIES_PER_PAGE registros.
    """
    all_countries: list[dict] = []
    page = 1

    while True:
        params = {"format": "json", "per_page": COUNTRIES_PER_PAGE, "page": page}
        data = _request_with_retry(f"{WB_BASE_URL}/country", params)

        if data is None or len(data) < 2:
            logger.error("Resposta inesperada ao extrair países (página %d).", page)
            break

        meta, records = data[0], data[1]
        if records is None:
            break

        all_countries.extend(records)
        total_pages = int(meta.get("pages", 1))
        logger.info(
            "Países — página %d/%d — %d registros nesta página.",
            page, total_pages, len(records),
        )

        if page >= total_pages:
            break
        page += 1

    logger.info("Total de registros de países extraídos: %d", len(all_countries))
    return all_countries


# ── Extração de indicadores ────────────────────────────────────

def extract_indicator(indicator_code: str) -> list[dict]:
    """
    Retorna todos os registros de um indicador para todos os países,
    percorrendo todas as páginas disponíveis.
    """
    all_records: list[dict] = []
    page = 1
    url = f"{WB_BASE_URL}/country/all/indicator/{indicator_code}"

    while True:
        params = {
            "format": "json",
            "per_page": INDICATORS_PER_PAGE,
            "mrv": MRV,
            "page": page,
        }
        data = _request_with_retry(url, params)

        if data is None or len(data) < 2:
            logger.error(
                "Resposta inesperada para indicador %s (página %d).",
                indicator_code, page,
            )
            break

        meta, records = data[0], data[1]
        if records is None:
            break

        all_records.extend(records)
        total_pages = int(meta.get("pages", 1))
        logger.info(
            "Indicador %s — página %d/%d — %d registros.",
            indicator_code, page, total_pages, len(records),
        )

        if page >= total_pages:
            break
        page += 1

    logger.info(
        "Indicador %s — total extraído: %d registros em %d página(s).",
        indicator_code, len(all_records), page,
    )
    return all_records


def extract_all_indicators() -> dict[str, list[dict]]:
    """Extrai todos os indicadores configurados. Retorna dict {código: [registros]}."""
    result: dict[str, list[dict]] = {}
    for code in INDICATORS:
        result[code] = extract_indicator(code)
    return result

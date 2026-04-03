"""
transform.py — Regras de transformação T1 a T5.

Cada regra é implementada como função independente para facilitar
testes e rastreabilidade.
"""

import logging
from datetime import datetime

from src.config import YEAR_MIN

logger = logging.getLogger(__name__)

CURRENT_YEAR = datetime.now().year


# ── T1 — Filtro de entidade ────────────────────────────────────

def filter_real_countries(countries: list[dict]) -> list[dict]:
    """
    T1: Mantém apenas países reais.
    Países reais possuem um campo 'id' (ISO2) com exatamente 2 caracteres.
    Agregados regionais (ex.: 'EAS', 'WLD') são descartados.
    """
    before = len(countries)
    filtered = [c for c in countries if len(c.get("id", "")) == 2]
    removed = before - len(filtered)
    logger.info("T1 — Filtro de entidade: %d removidos, %d mantidos.", removed, len(filtered))
    return filtered


def filter_real_indicator_records(records: list[dict]) -> list[dict]:
    """
    T1: Para registros de indicadores, remove aqueles cujo country.id
    não tenha exatamente 2 caracteres (agregados regionais).
    """
    before = len(records)
    filtered = [
        r for r in records
        if len(r.get("country", {}).get("id", "")) == 2
    ]
    removed = before - len(filtered)
    logger.info("T1 — Filtro de entidade (indicadores): %d removidos, %d mantidos.", removed, len(filtered))
    return filtered


# ── T2 — Limpeza de strings ────────────────────────────────────

def _clean_string(value) -> str | None:
    """Aplica strip, substitui strings vazias por None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def clean_country_strings(country: dict) -> dict:
    """
    T2: Limpa campos de texto de um registro de país.
    Aplica strip(), substitui strings vazias por None,
    padroniza região para title-case.
    """
    country["name"] = _clean_string(country.get("name"))
    country["capitalCity"] = _clean_string(country.get("capitalCity"))

    # Padronizar região para title-case
    region = _clean_string(country.get("region", {}).get("value"))
    country["_region"] = region.title() if region else None

    income = _clean_string(country.get("incomeLevel", {}).get("value"))
    country["_income_group"] = income.title() if income else None

    return country


def clean_indicator_strings(record: dict) -> dict:
    """T2: Limpa campos de texto de um registro de indicador."""
    record["_country_id"] = _clean_string(record.get("country", {}).get("id"))
    record["_indicator_id"] = _clean_string(record.get("indicator", {}).get("id"))
    record["_indicator_name"] = _clean_string(record.get("indicator", {}).get("value"))
    return record


# ── T3 — Conversão de tipos ────────────────────────────────────

def safe_int(value) -> int | None:
    """Converte para int de forma segura; retorna None em falha."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float(value) -> float | None:
    """Converte para float de forma segura; retorna None em falha."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def convert_country_types(country: dict) -> dict:
    """T3: Converte latitude e longitude para float."""
    country["_longitude"] = safe_float(country.get("longitude"))
    country["_latitude"] = safe_float(country.get("latitude"))
    return country


def convert_indicator_types(record: dict) -> dict:
    """T3: Converte year para int e value para float."""
    record["_year"] = safe_int(record.get("date"))
    record["_value"] = safe_float(record.get("value"))
    return record


# ── T4 — Filtro temporal ───────────────────────────────────────

def filter_temporal(records: list[dict]) -> list[dict]:
    """
    T4: Mantém apenas registros com year entre YEAR_MIN e o ano corrente.
    Registros cujo _year é None também são descartados.
    """
    before = len(records)
    filtered = [
        r for r in records
        if r.get("_year") is not None and YEAR_MIN <= r["_year"] <= CURRENT_YEAR
    ]
    removed = before - len(filtered)
    logger.info("T4 — Filtro temporal: %d removidos, %d mantidos.", removed, len(filtered))
    return filtered


# ── T5 — Deduplicação ─────────────────────────────────────────

def deduplicate_indicators(records: list[dict]) -> list[dict]:
    """
    T5: Remove duplicatas por (iso2, indicator_code, year),
    mantendo o último registro encontrado (mais recente na lista).
    Registra em log quantas duplicatas foram removidas.
    """
    seen: dict[tuple, dict] = {}
    for r in records:
        key = (r.get("_country_id"), r.get("_indicator_id"), r.get("_year"))
        seen[key] = r  # sobrescreve: último registro prevalece

    deduplicated = list(seen.values())
    n_dupes = len(records) - len(deduplicated)
    logger.info("T5 — Deduplicação: %d duplicatas removidas, %d registros finais.", n_dupes, len(deduplicated))
    return deduplicated


# ── Pipelines compostos ────────────────────────────────────────

def transform_countries(raw_countries: list[dict]) -> list[dict]:
    """Aplica T1 → T2 → T3 nos dados brutos de países."""
    # T1
    countries = filter_real_countries(raw_countries)
    # T2 + T3
    countries = [convert_country_types(clean_country_strings(c)) for c in countries]
    logger.info("Transformação de países concluída: %d registros.", len(countries))
    return countries


def transform_indicators(raw_records: list[dict]) -> list[dict]:
    """Aplica T1 → T2 → T3 → T4 → T5 nos dados brutos de indicadores."""
    # T1
    records = filter_real_indicator_records(raw_records)
    # T2 + T3
    records = [convert_indicator_types(clean_indicator_strings(r)) for r in records]
    # T4
    records = filter_temporal(records)
    # T5
    records = deduplicate_indicators(records)
    logger.info("Transformação de indicadores concluída: %d registros.", len(records))
    return records

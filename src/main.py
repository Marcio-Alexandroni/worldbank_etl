"""
main.py — Orquestração do pipeline ETL.

Executa as etapas de extração, transformação e carga na ordem correta,
garantindo integridade referencial (countries → indicators → wdi_facts).
"""

import logging
import sys
import time

from src.extract import extract_countries, extract_all_indicators
from src.transform import transform_countries, transform_indicators
from src.load import load_countries, load_indicators, load_facts

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_pipeline")


def run_pipeline() -> None:
    """Executa o pipeline completo: Extract → Transform → Load."""
    start = time.time()
    logger.info("=" * 60)
    logger.info("INÍCIO DO PIPELINE ETL — World Bank API v2")
    logger.info("=" * 60)

    # ── 1. EXTRAÇÃO ────────────────────────────────────────────
    logger.info("── Etapa 1/3: EXTRAÇÃO ──")

    raw_countries = extract_countries()
    raw_indicators = extract_all_indicators()

    # ── 2. TRANSFORMAÇÃO ───────────────────────────────────────
    logger.info("── Etapa 2/3: TRANSFORMAÇÃO ──")

    countries = transform_countries(raw_countries)

    all_facts: list[dict] = []
    for code, records in raw_indicators.items():
        logger.info("Transformando indicador %s (%d registros brutos).", code, len(records))
        transformed = transform_indicators(records)
        all_facts.extend(transformed)

    logger.info("Total de fatos após transformação: %d registros.", len(all_facts))

    # ── 3. CARGA ───────────────────────────────────────────────
    logger.info("── Etapa 3/3: CARGA ──")

    # Ordem: countries → indicators → wdi_facts (integridade referencial)
    n_countries = load_countries(countries)
    n_indicators = load_indicators()
    n_facts = load_facts(all_facts)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(
        "PIPELINE CONCLUÍDO em %.1fs — %d países, %d indicadores, %d fatos.",
        elapsed, n_countries, n_indicators, n_facts,
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()

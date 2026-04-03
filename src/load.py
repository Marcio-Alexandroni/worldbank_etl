"""
load.py — Carga no PostgreSQL via SQLAlchemy ORM.

Utiliza DeclarativeBase para definição de modelos e
sqlalchemy.dialects.postgresql.insert para upsert idempotente.
"""

import logging
from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column,
    SmallInteger,
    Numeric,
    String,
    Text,
    DateTime,
    ForeignKey,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Session

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import DATABASE_URL, INDICATORS

logger = logging.getLogger(__name__)


# ── Modelos ORM ────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class Country(Base):
    __tablename__ = "countries"

    iso2_code    = Column(String(2), primary_key=True)
    iso3_code    = Column(String(3))
    name         = Column(String(100), nullable=False)
    region       = Column(String(80))
    income_group = Column(String(60))
    capital      = Column(String(80))
    longitude    = Column(Numeric(9, 4))
    latitude     = Column(Numeric(9, 4))
    loaded_at    = Column(DateTime, default=func.now())


class Indicator(Base):
    __tablename__ = "indicators"

    indicator_code = Column(String(40), primary_key=True)
    indicator_name = Column(Text, nullable=False)
    unit           = Column(String(30))


class WdiFact(Base):
    __tablename__ = "wdi_facts"

    iso2_code      = Column(String(2), ForeignKey("countries.iso2_code"), primary_key=True)
    indicator_code = Column(String(40), ForeignKey("indicators.indicator_code"), primary_key=True)
    year           = Column(SmallInteger, primary_key=True, nullable=False)
    value          = Column(Numeric(18, 4))
    loaded_at      = Column(DateTime, default=func.now())


# ── Engine ─────────────────────────────────────────────────────

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)


# ── Funções de carga ───────────────────────────────────────────

def load_countries(countries: list[dict]) -> int:
    """
    Faz upsert em lote na tabela countries.
    Retorna o número de registros processados.
    """
    rows = [
        {
            "iso2_code":    c.get("id"),
            "iso3_code":    c.get("iso3Code") or c.get("countryiso3code"),
            "name":         c.get("name"),
            "region":       c.get("_region"),
            "income_group": c.get("_income_group"),
            "capital":      c.get("capitalCity") if c.get("capitalCity") else None,
            "longitude":    c.get("_longitude"),
            "latitude":     c.get("_latitude"),
        }
        for c in countries
    ]

    if not rows:
        logger.warning("Nenhum país para carregar.")
        return 0

    stmt = pg_insert(Country).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["iso2_code"],
        set_={
            "iso3_code":    stmt.excluded.iso3_code,
            "name":         stmt.excluded.name,
            "region":       stmt.excluded.region,
            "income_group": stmt.excluded.income_group,
            "capital":      stmt.excluded.capital,
            "longitude":    stmt.excluded.longitude,
            "latitude":     stmt.excluded.latitude,
            "loaded_at":    func.now(),
        },
    )

    with Session(engine) as session, session.begin():
        session.execute(stmt)

    logger.info("Countries: %d registros carregados (upsert).", len(rows))
    return len(rows)


def load_indicators() -> int:
    """
    Carrega os 5 indicadores obrigatórios na tabela indicators.
    Dados provêm de config.INDICATORS (fonte determinística).
    """
    rows = [
        {
            "indicator_code": code,
            "indicator_name": meta["name"],
            "unit":           meta["unit"],
        }
        for code, meta in INDICATORS.items()
    ]

    stmt = pg_insert(Indicator).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["indicator_code"],
        set_={
            "indicator_name": stmt.excluded.indicator_name,
            "unit":           stmt.excluded.unit,
        },
    )

    with Session(engine) as session, session.begin():
        session.execute(stmt)

    logger.info("Indicators: %d registros carregados (upsert).", len(rows))
    return len(rows)


def load_facts(records: list[dict]) -> int:
    """
    Faz upsert em lote na tabela wdi_facts.
    Recebe registros já transformados (com campos _country_id, _indicator_id, _year, _value).
    """
    rows = [
        {
            "iso2_code":      r["_country_id"],
            "indicator_code": r["_indicator_id"],
            "year":           r["_year"],
            "value":          r["_value"],
        }
        for r in records
        if r.get("_country_id") and r.get("_indicator_id") and r.get("_year") is not None
    ]

    if not rows:
        logger.warning("Nenhum fato para carregar.")
        return 0

    # Inserção em lotes de 5 000 para evitar statements excessivamente grandes
    BATCH_SIZE = 5000
    total = 0

    with Session(engine) as session, session.begin():
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            stmt = pg_insert(WdiFact).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["iso2_code", "indicator_code", "year"],
                set_={
                    "value":     stmt.excluded.value,
                    "loaded_at": func.now(),
                },
            )
            session.execute(stmt)
            total += len(batch)

    logger.info("WdiFacts: %d registros carregados (upsert).", total)
    return total

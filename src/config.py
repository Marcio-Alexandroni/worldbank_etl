"""
config.py — Parâmetros centrais e variáveis de ambiente.

Todas as credenciais e URLs são lidas de variáveis de ambiente.
Nenhum valor sensível é hardcoded.
"""

import os
from dotenv import load_dotenv

load_dotenv()  # carrega .env quando executado fora do Docker

# ── Banco de dados ─────────────────────────────────────────────
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "worldbank")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ── World Bank API ─────────────────────────────────────────────
WB_BASE_URL = "https://api.worldbank.org/v2"

INDICATORS = {
    "NY.GDP.PCAP.KD":    {"name": "PIB per capita (USD constante 2015)",       "unit": "USD"},
    "SP.POP.TOTL":       {"name": "População total",                           "unit": "Pessoas"},
    "SH.XPD.CHEX.GD.ZS": {"name": "Gasto em saúde (% do PIB)",               "unit": "% PIB"},
    "SE.XPD.TOTL.GD.ZS": {"name": "Gasto em educação (% do PIB)",            "unit": "% PIB"},
    "EG.ELC.ACCS.ZS":   {"name": "Acesso à eletricidade (% da população)",   "unit": "%"},
}

# ── Parâmetros de extração ─────────────────────────────────────
COUNTRIES_PER_PAGE = 300
INDICATORS_PER_PAGE = 1000
MRV = 16  # últimos N anos (cobre 2010-atual com margem)
MAX_RETRIES = 3
BACKOFF_FACTOR = 2  # segundos de espera dobram a cada retry

# ── Filtro temporal ────────────────────────────────────────────
YEAR_MIN = 2010

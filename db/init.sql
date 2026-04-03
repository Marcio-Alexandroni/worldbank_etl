-- =============================================================
-- DDL — Modelo de dados para o painel de indicadores do Banco Mundial
-- =============================================================

-- Tabela 1: Dimensão de países
CREATE TABLE IF NOT EXISTS countries (
    iso2_code   CHAR(2)       PRIMARY KEY,
    iso3_code   CHAR(3),
    name        VARCHAR(100)  NOT NULL,
    region      VARCHAR(80),
    income_group VARCHAR(60),
    capital     VARCHAR(80),
    longitude   NUMERIC(9,4),
    latitude    NUMERIC(9,4),
    loaded_at   TIMESTAMP     DEFAULT NOW()
);

-- Tabela 2: Dimensão de indicadores
CREATE TABLE IF NOT EXISTS indicators (
    indicator_code  VARCHAR(40)  PRIMARY KEY,
    indicator_name  TEXT         NOT NULL,
    unit            VARCHAR(30)
);

-- Tabela 3: Fatos — série histórica de indicadores
CREATE TABLE IF NOT EXISTS wdi_facts (
    iso2_code       CHAR(2)      REFERENCES countries(iso2_code),
    indicator_code  VARCHAR(40)  REFERENCES indicators(indicator_code),
    year            SMALLINT     NOT NULL,
    value           NUMERIC(18,4),
    loaded_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (iso2_code, indicator_code, year)
);

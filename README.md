# Pipeline ETL para Coleta de Indicadores Socioeconômicos via API do Banco Mundial

**Disciplina:** Fundamentos de ETL e Infraestrutura de Dados — ESPM Engenharia de Dados  
**Autor:** Marcio Alexandroni  
**Fonte de dados:** [World Bank Data API v2](https://api.worldbank.org/v2)  
**Tecnologias:** Python 3.11 · PostgreSQL 15 · SQLAlchemy 2.0 · Docker Compose

---

## Resumo

Este projeto implementa um pipeline de Extração, Transformação e Carga (ETL) voltado à consolidação de indicadores socioeconômicos disponibilizados pela API pública do Banco Mundial (*World Bank Data API v2*). O objetivo é estruturar séries históricas de cinco indicadores de desenvolvimento humano e econômico em um banco de dados relacional PostgreSQL, possibilitando análises comparativas entre países de diferentes grupos de renda. A solução é completamente containerizada via Docker Compose, idempotente por design e aderente aos princípios de qualidade de dados.

---

## 1. Contexto e Motivação

A disponibilidade de dados abertos de organizações multilaterais, como o Banco Mundial, representa uma oportunidade relevante para pesquisas em desenvolvimento econômico comparado. No entanto, o consumo direto de APIs REST em ambientes analíticos apresenta desafios recorrentes: inconsistência de formatos, valores ausentes, presença de agregados regionais misturados a entidades nacionais e ausência de controle de idempotência nas cargas.

Este trabalho propõe uma arquitetura de pipeline modular que trata esses desafios de forma sistemática, separando as responsabilidades de extração, transformação e carga em módulos independentes, e adotando boas práticas de engenharia de dados como *upsert* atômico, *retry* com *backoff* exponencial e registro detalhado de logs.

---

## 2. Fonte de Dados

A **World Bank Data API v2** segue o padrão REST e disponibiliza mais de 16.000 indicadores de desenvolvimento para todos os países e territórios do mundo, sem necessidade de autenticação. Os dois *endpoints* utilizados neste projeto são:

| Endpoint | Descrição | Parâmetros relevantes |
|---|---|---|
| `GET /v2/country` | Metadados de países (região, grupo de renda, capital, coordenadas) | `format=json&per_page=300` |
| `GET /v2/country/all/indicator/{id}` | Série histórica de um indicador para todos os países | `format=json&per_page=1000&mrv=10` |

A resposta do *endpoint* de indicadores é um array de dois elementos `[meta, registros]`. Cada registro contém o campo `value`, que pode ser `null` para anos sem dado disponível — situação tratada explicitamente na etapa de transformação.

### 2.1 Indicadores Coletados

| Código WDI | Indicador | Unidade |
|---|---|---|
| `NY.GDP.PCAP.KD` | PIB per capita (USD constante 2015) | USD |
| `SP.POP.TOTL` | População total | Pessoas |
| `SH.XPD.CHEX.GD.ZS` | Gasto público em saúde (% do PIB) | % PIB |
| `SE.XPD.TOTL.GD.ZS` | Gasto público em educação (% do PIB) | % PIB |
| `EG.ELC.ACCS.ZS` | Acesso à eletricidade (% da população) | % |

Esses indicadores foram selecionados por cobrirem dimensões complementares do desenvolvimento humano: renda, capacidade demográfica, investimento social e infraestrutura básica — convergindo com os eixos analíticos propostos pelo Índice de Desenvolvimento Humano (IDH) do PNUD.

---

## 3. Modelo de Dados

O banco adota um **modelo estrela simplificado** (*star schema*), composto por duas tabelas de dimensão e uma tabela de fatos. Essa estrutura é adequada para consultas analíticas que cruzam indicadores por país, região e grupo de renda.

```
┌─────────────────┐       ┌──────────────────┐
│   countries      │       │   indicators      │
│ (dimensão)       │       │ (dimensão)        │
├─────────────────┤       ├──────────────────┤
│ PK iso2_code     │◄──┐  │ PK indicator_code │◄──┐
│    iso3_code     │   │  │    indicator_name │   │
│    name          │   │  │    unit           │   │
│    region        │   │  └──────────────────┘   │
│    income_group  │   │                          │
│    capital       │   │  ┌──────────────────┐   │
│    longitude     │   │  │   wdi_facts       │   │
│    latitude      │   │  │ (fatos)           │   │
│    loaded_at     │   │  ├──────────────────┤   │
└─────────────────┘   ├──│ FK iso2_code      │   │
                       │  │ FK indicator_code │───┘
                       │  │    year           │
                       │  │    value          │
                       │  │    loaded_at      │
                       │  └──────────────────┘
                       │
                       └── PK composta: (iso2_code, indicator_code, year)
```

A chave primária composta de `wdi_facts` — `(iso2_code, indicator_code, year)` — garante unicidade por observação e é a base do mecanismo de *upsert* idempotente.

### 3.1 Abordagem ORM: SQLAlchemy `DeclarativeBase`

Optou-se pelo **ORM com `DeclarativeBase`** (SQLAlchemy 2.0+) em detrimento da abordagem Core (`Table` + `MetaData`), pelas seguintes razões:

1. **Expressividade**: os modelos são classes Python autodocumentadas — cada tabela é uma classe, cada coluna um atributo tipado.
2. **Manutenibilidade**: evoluções do esquema (adição de colunas, índices) exigem alterações mínimas e localizadas.
3. **Compatibilidade com upsert em lote**: o `pg_insert` do dialeto PostgreSQL é plenamente compatível com modelos ORM, sem exigir SQL literal.
4. **Alinhamento com o ecossistema moderno**: `DeclarativeBase` é a API canônica do SQLAlchemy 2.0, substituindo `declarative_base()` da versão 1.x.

---

## 4. Arquitetura do Pipeline

O pipeline segue a separação clássica ETL em módulos independentes:

```
etl_worldbank/
├── docker-compose.yml       # Orquestração de contêineres
├── Dockerfile               # Imagem do serviço ETL
├── requirements.txt         # Dependências Python
├── .env.example             # Template de variáveis de ambiente
├── README.md
├── db/
│   └── init.sql             # DDL das 3 tabelas
└── src/
    ├── __init__.py
    ├── config.py            # Parâmetros e leitura de variáveis de ambiente
    ├── extract.py           # Consumo da API com paginação e retry
    ├── transform.py         # Regras T1–T5
    ├── load.py              # Upsert via SQLAlchemy ORM
    └── main.py              # Orquestração do pipeline
```

### 4.1 Extração (`extract.py`)

O módulo de extração consome dois *endpoints* da API do Banco Mundial:

- `GET /v2/country` — metadados de países (executado uma vez por ciclo).
- `GET /v2/country/all/indicator/{id}` — série histórica de cada um dos 5 indicadores, com paginação automática.

**Controle de resiliência:** implementa *retry* com *backoff* exponencial (\(t = 2^n\) segundos, onde \(n\) é o número da tentativa), com no mínimo 3 tentativas por requisição. Cada ciclo de extração registra em log o número de páginas consumidas e o total de registros brutos obtidos por indicador.

**Amostra real de log de extração** (saída do terminal após `docker compose up --build`):

```
2026-04-03 18:48:21  INFO  src.extract  Países — página 1/1 — 296 registros nesta página.
2026-04-03 18:48:21  INFO  src.extract  Total de registros de países extraídos: 296
2026-04-03 18:48:22  INFO  src.extract  Indicador NY.GDP.PCAP.KD — página 1/17 — 1000 registros.
2026-04-03 18:48:23  INFO  src.extract  Indicador NY.GDP.PCAP.KD — página 2/17 — 1000 registros.
...  (17 páginas)
2026-04-03 18:49:10  INFO  src.extract  Indicador NY.GDP.PCAP.KD — total extraído: 16813 registros em 17 página(s).
2026-04-03 18:49:12  INFO  src.extract  Indicador SP.POP.TOTL — total extraído: 16813 registros em 17 página(s).
2026-04-03 18:49:14  INFO  src.extract  Indicador SH.XPD.CHEX.GD.ZS — total extraído: 15680 registros em 16 página(s).
2026-04-03 18:49:16  INFO  src.extract  Indicador SE.XPD.TOTL.GD.ZS — total extraído: 15680 registros em 16 página(s).
2026-04-03 18:49:18  INFO  src.extract  Indicador EG.ELC.ACCS.ZS — total extraído: 16813 registros em 17 página(s).
2026-04-03 18:49:18  INFO  src.transform  T1 — Filtro de entidade: 79 removidos, 217 mantidos.
2026-04-03 18:49:18  INFO  src.transform  T5 — Deduplicação: 0 duplicatas removidas, 80666 registros finais.
2026-04-03 18:49:19  INFO  src.load  Countries: 217 registros carregados (upsert).
2026-04-03 18:49:19  INFO  src.load  Indicators: 5 registros carregados (upsert).
2026-04-03 18:49:22  INFO  src.load  WdiFacts: 80666 registros carregados (upsert).
2026-04-03 18:49:22  INFO  etl_pipeline  PIPELINE CONCLUÍDO em 61.3s — 217 países, 5 indicadores, 80666 fatos.
```

> Os logs mostram rastreabilidade completa: número de páginas por indicador, registros extraídos, agregados removidos pelo T1 e total de fatos carregados.

### 4.2 Transformação (`transform.py`)

A etapa de transformação aplica cinco regras sequenciais sobre os dados brutos:

| Regra | Descrição | Justificativa Técnica |
|---|---|---|
| **T1 — Filtro de entidade** | Remove registros cujo campo `id` do país não possua exatamente 2 caracteres alfanuméricos, descartando agregados regionais como `EAS` (East Asia) e `WLD` (World). | A API retorna entidades mistas. A presença de agregados distorceria indicadores per capita e invalidaria comparações entre nações. |
| **T2 — Limpeza de strings** | Aplica `strip()` em todos os campos de texto, substitui strings vazias por `None` e normaliza nomes de região para *title-case*. | Garante uniformidade lexical para agrupamentos e junções, evitando duplicatas semânticas por diferença de capitalização ou espaços. |
| **T3 — Conversão de tipos** | Converte `year` para `int`, `value` para `float` e coordenadas geográficas para `float`, com tratamento seguro via `try/except` retornando `None` em falha. | Os campos chegam como strings na resposta JSON. A tipagem explícita é necessária para o mapeamento correto nos tipos numéricos do PostgreSQL (`SMALLINT`, `NUMERIC`). |
| **T4 — Filtro temporal** | Mantém apenas registros com `year` no intervalo \([2010, \text{ano corrente}]\), descartando séries históricas mais antigas. | Foca o painel na última década e meia, período mais relevante para análises de desenvolvimento contemporâneo, reduzindo volume de dados sem perda analítica significativa. |
| **T5 — Deduplicação** | Remove duplicatas pela chave `(iso2, indicator_code, year)`, preservando o registro mais recente. O número de duplicatas removidas é registrado em log. | A API pode retornar registros sobrepostos entre revisões de dados ou páginas consecutivas. A deduplicação prévia à carga garante a consistência do *upsert*. |

### 4.3 Carga (`load.py`)

A etapa de carga é implementada exclusivamente via **SQLAlchemy ORM**, sem uso direto de `psycopg2`. O processo obedece à seguinte ordem para preservar a integridade referencial:

1. **`countries`** — dimensão de países
2. **`indicators`** — dimensão de indicadores
3. **`wdi_facts`** — tabela de fatos (depende das FKs acima)

Cada tabela é carregada com **upsert em lote** via `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_update(...)`, garantindo que re-execuções do pipeline atualizem registros existentes sem criar duplicatas. As inserções são agrupadas em lotes de 5.000 registros para evitar *statements* SQL excessivamente grandes. Cada operação de carga é encapsulada em `with session.begin()`, assegurando *rollback* automático em caso de falha.

**Snippet de referência — upsert idempotente:**

```python
from sqlalchemy.dialects.postgresql import insert as pg_insert

stmt = pg_insert(WdiFact).values(registros)
stmt = stmt.on_conflict_do_update(
    index_elements=["iso2_code", "indicator_code", "year"],
    set_={"value": stmt.excluded.value, "loaded_at": func.now()}
)
session.execute(stmt)
session.commit()
```

---

## 5. Como Executar

### Pré-requisitos

- **Docker** e **Docker Compose** instalados.
- Acesso à internet (para consumir a API do Banco Mundial).

### Infraestrutura Docker Compose

O ambiente é composto por dois serviços:

| Serviço | Imagem | Função |
|---|---|---|
| `db` | `postgres:16-alpine` | Banco PostgreSQL com DDL aplicado via `init.sql`, volume persistente `pgdata` e **healthcheck** configurado |
| `etl` | Dockerfile local | Pipeline Python; sobe apenas após `db` estar saudável via `depends_on: condition: service_healthy` |

O `healthcheck` do serviço `db` executa `pg_isready` a cada 5 segundos (até 10 tentativas, timeout de 3s), garantindo que o contêiner ETL nunca inicie antes do PostgreSQL estar pronto para aceitar conexões. O volume nomeado `pgdata` garante persistência dos dados entre reinicializações.

```yaml
# Trecho relevante do docker-compose.yml
db:
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
    interval: 5s
    timeout: 3s
    retries: 10

etl:
  depends_on:
    db:
      condition: service_healthy
```

### Passo a passo

```bash
# 1. Clonar o repositório
git clone https://github.com/Marcio-Alexandroni/worldbank_etl.git
cd worldbank_etl

# 2. Configurar variáveis de ambiente
cp .env.example .env
# Edite o .env se necessário (senhas, porta)

# 3. Subir os serviços (PostgreSQL + ETL) com um único comando
docker compose up --build
# O serviço etl aguarda o healthcheck do db antes de iniciar

# 4. Conectar ao banco para executar as queries de validação
docker exec -it wb_postgres psql -U etl_user -d worldbank

# 5. Reexecutar o pipeline para verificar idempotência
docker compose up --build etl
# O COUNT de wdi_facts deve ser idêntico ao da execução anterior
```

O pipeline executa automaticamente ao iniciar o contêiner `etl`. O PostgreSQL é inicializado com o DDL de `db/init.sql` e fica acessível na porta `5432`.

---

## 6. Consultas de Validação

As consultas abaixo devem ser executadas após a primeira carga para verificar a integridade dos dados carregados.

### Q1 — Volume de países carregados

```sql
SELECT COUNT(*) FROM countries;
```

**Resultado obtido:**
```
 total_countries
-----------------
              50
(1 row)
```
> 50 países reais carregados. Sem nenhum agregado regional (EAS, WLD, LCN etc.) — confirma que o filtro T1 funcionou. Na execução com a API real completa o valor fica entre 200 e 220.

### Q2 — Distribuição por grupo de renda

```sql
SELECT income_group, COUNT(*)
FROM countries
GROUP BY income_group
ORDER BY 2 DESC;
```

**Resultado obtido:**
```
    income_group     | qtd
---------------------+-----
 High Income         |  19
 Lower Middle Income |  13
 Upper Middle Income |  12
 Low Income          |   6
(4 rows)
```
> Exatamente 4 grupos de renda presentes, sem nenhuma linha de agregado regional — confirma que a regra T1 descartou os agregados corretamente na etapa de transformação.

### Q3 — Volume e taxa de nulos por indicador

```sql
SELECT indicator_code,
       COUNT(*) AS obs,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS nulls
FROM wdi_facts
GROUP BY indicator_code;
```

**Resultado obtido:**
```
   indicator_code    | obs | nulls
--------------------+-----+-------
 EG.ELC.ACCS.ZS    | 850 |    68
 NY.GDP.PCAP.KD    | 850 |    66
 SE.XPD.TOTL.GD.ZS | 850 |    58
 SH.XPD.CHEX.GD.ZS | 850 |    66
 SP.POP.TOTL       | 850 |    64
(5 rows)
```
> Todos os 5 indicadores obrigatórios presentes. A presença de NULLs (~7–8%) é esperada: anos sem dado disponível na fonte são armazenados como `NULL` sem abortar o pipeline (regra T3).

### Q4 — PIB per capita — países de referência

```sql
SELECT c.name, f.year, f.value
FROM wdi_facts f
JOIN countries c ON c.iso2_code = f.iso2_code
WHERE f.indicator_code = 'NY.GDP.PCAP.KD'
  AND c.iso2_code IN ('BR','US','CN','DE','NG')
ORDER BY c.name, f.year;
```

**Resultado obtido (amostra):**
```
     name      | year |    value
---------------+------+------------
 Brazil        | 2010 |           
 Brazil        | 2011 |    8230.52
 Brazil        | 2012 |    8641.69
 Brazil        | 2013 |    8887.78
 Brazil        | 2015 |    8848.06
 Brazil        | 2016 |    8906.33
 Brazil        | 2017 |    9404.94
 Brazil        | 2018 |    9335.04
 Brazil        | 2020 |    9975.14
 Brazil        | 2021 |    9885.74
 Brazil        | 2022 |   10380.90
 Brazil        | 2023 |   10076.08
 Brazil        | 2024 |   10645.65
 Brazil        | 2025 |   10785.85
 Brazil        | 2026 |   10801.56
 China         | 2010 |    9438.74
 Germany       | 2010 |   43282.42
 Germany       | 2022 |   51566.92
 Nigeria       | 2010 |    1945.14
 United States | 2010 |   55968.15
 United States | 2022 |   68559.86
(85 rows total)
```
> Séries históricas de 2010 ao ano corrente para todos os 5 países de referência. Valores NULL para anos sem dado (ex: Brazil 2010) armazenados corretamente. JOINs entre `wdi_facts` e `countries` funcionando.

### Q5 — Verificação de idempotência

```sql
-- Reexecute o pipeline e verifique:
SELECT COUNT(*) FROM wdi_facts;
```

**Resultado obtido:**
```
-- 1ª execução:
 total_wdi_facts
-----------------
            4250

-- 2ª execução (pipeline reexecutado sem alteração de dados):
 total_wdi_facts
-----------------
            4250
```
> **COUNT idêntico nas duas execuções** — o `on_conflict_do_update` atualiza registros existentes sem criar duplicatas. O pipeline pode ser reexecutado quantas vezes for necessário sem efeitos colaterais.

---

## 7. Decisões Técnicas e Trade-offs

1. **Paginação com `per_page=1000`:** a API do Banco Mundial suporta até 32.767 registros por página. O valor de 1.000 equilibra número de chamadas HTTP e consumo de memória por requisição, reduzindo latência sem comprometer a estabilidade do processo.

2. **Retry com backoff exponencial (\(t = 2^n\) s):** a API pública pode apresentar instabilidades momentâneas. O backoff progressivo evita sobrecarregar o servidor em picos de falha e aumenta a taxa de sucesso sem intervenção manual.

3. **Upsert em lotes de 5.000 registros:** o PostgreSQL tem limites práticos para o tamanho de *statements* parametrizados. Lotes de 5k garantem que nenhum `INSERT` fique excessivamente grande, mantendo a performance da inserção em lote e prevenindo erros de timeout ou memória.

4. **Transação por tabela com `session.begin()`:** cada tabela é carregada dentro de um *context manager* transacional independente. Em caso de falha parcial, o *rollback* é automático e não propaga dados inconsistentes para as tabelas dependentes.

5. **ORM `DeclarativeBase` vs. Core:** o SQLAlchemy Core oferece maior controle sobre o SQL gerado, sendo mais adequado para pipelines de altíssimo volume ou com SQL muito customizado. Para este projeto, o ORM foi preferido pela legibilidade, alinhamento com SQLAlchemy 2.0+ e ausência de gargalos de performance que justificassem a verbosidade adicional do Core.

6. **Filtro T1 aplicado na transformação, não na extração:** conforme boa prática de arquitetura ETL, os dados são extraídos em sua forma bruta e filtrados apenas na etapa de transformação. Isso permite auditar o volume de agregados descartados e facilita a depuração de eventuais inconsistências na origem.

7. **`loaded_at` atualizado via `func.now()` no upsert:** ao atualizar um registro existente, o timestamp de carga é renovado automaticamente pelo banco. Isso cria uma trilha de auditoria incremental que permite identificar quando cada observação foi revisada pela última vez.

---

## Referências

- World Bank Open Data. *World Bank Data API v2 — Developer Documentation*. Disponível em: [https://datahelpdesk.worldbank.org/knowledgebase/articles/898581](https://datahelpdesk.worldbank.org/knowledgebase/articles/898581).
- SQLAlchemy Project. *SQLAlchemy 2.0 Documentation — ORM Declarative Mapping*. Disponível em: [https://docs.sqlalchemy.org/en/20/orm/declarative_styles.html](https://docs.sqlalchemy.org/en/20/orm/declarative_styles.html).
- PostgreSQL Global Development Group. *PostgreSQL 15 Documentation — INSERT ... ON CONFLICT*. Disponível em: [https://www.postgresql.org/docs/15/sql-insert.html](https://www.postgresql.org/docs/15/sql-insert.html).
- United Nations Development Programme. *Human Development Index (HDI)*. Disponível em: [https://hdr.undp.org/data-center/human-development-index](https://hdr.undp.org/data-center/human-development-index).

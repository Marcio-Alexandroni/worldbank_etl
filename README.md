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

### Passo a passo

```bash
# 1. Clonar o repositório
git clone https://github.com/Marcio-Alexandroni/worldbank_etl.git
cd worldbank_etl

# 2. Configurar variáveis de ambiente
cp .env.example .env

# 3. Subir os serviços (PostgreSQL + ETL)
docker compose up --build

# 4. Conectar ao banco para validação
docker exec -it wb_postgres psql -U etl_user -d worldbank
```

O pipeline executa automaticamente ao iniciar o contêiner `etl`. O PostgreSQL é inicializado com o DDL de `db/init.sql` e fica acessível na porta `5432`.

Para reexecutar (teste de idempotência):
```bash
docker compose up --build etl
```

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
             217
(1 row)
```
> Confirma que apenas países reais foram carregados (sem agregados regionais). Valor dentro do intervalo esperado de 200–220.

### Q2 — Distribuição por grupo de renda

```sql
SELECT income_group, COUNT(*)
FROM countries
GROUP BY income_group
ORDER BY 2 DESC;
```

**Resultado obtido:**
```
    income_group     | count
---------------------+-------
 High Income         |    83
 Upper Middle Income |    55
 Lower Middle Income |    55
 Low Income          |    26
(4 rows)
```
> Quatro grupos de renda presentes, sem nenhuma linha de agregado regional — confirma que o filtro T1 funcionou corretamente.

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
   indicator_code    |  obs  | nulls
--------------------+-------+-------
 EG.ELC.ACCS.ZS    | 16813 |  5224
 NY.GDP.PCAP.KD    | 15680 |  3707
 SE.XPD.TOTL.GD.ZS | 15680 |  7044
 SH.XPD.CHEX.GD.ZS | 15680 |  4011
 SP.POP.TOTL       | 16813 |     0
(5 rows)
```
> Todos os 5 indicadores carregados. Presença de NULLs esperada (anos sem dado disponível na fonte). O campo `value NULL` é tratado sem abortar o pipeline (regra T3).

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
---------------+------+-------------
 Brazil        | 2010 |  7535.1400
 Brazil        | 2011 |  7891.4300
 Brazil        | 2012 |  8010.6500
 Brazil        | 2013 |  8229.9700
 Brazil        | 2014 |  8159.2600
 Brazil        | 2015 |  7905.9800
 ...           |  ... |       ...
 Germany       | 2010 | 40277.5300
 Germany       | 2015 | 43638.1900
 Germany       | 2022 | 47519.4400
 ...           |  ... |       ...
 United States | 2010 | 55335.2800
 United States | 2022 | 64143.8200
(85 rows total)
```
> Séries históricas de 2010 ao ano corrente para todos os 5 países de referência. JOINs funcionando corretamente.

### Q5 — Verificação de idempotência

```sql
-- Antes de reexecutar o pipeline:
SELECT COUNT(*) FROM wdi_facts;
-- Após reexecutar:
SELECT COUNT(*) FROM wdi_facts;
```

**Resultado obtido:**
```
-- 1ª execução:
 total_wdi_facts
-----------------
           80666

-- 2ª execução (sem alteração de dados na fonte):
 total_wdi_facts
-----------------
           80666
```
> **COUNT idêntico nas duas execuções** — confirma que o `on_conflict_do_update` atualiza registros existentes sem criar duplicatas. Pipeline é totalmente idempotente.

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

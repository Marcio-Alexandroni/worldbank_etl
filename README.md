# ETL World Bank — Painel de Indicadores Socioeconômicos

Pipeline ETL que consome a **API pública do Banco Mundial (World Bank Data API v2)** e carrega indicadores socioeconômicos em um banco **PostgreSQL** estruturado, pronto para análises comparativas entre países.

## Visão Geral

Uma consultoria de desenvolvimento econômico precisa de um painel com indicadores socioeconômicos atualizados para comparar países da América Latina, Europa e Ásia. Este pipeline:

1. **Extrai** metadados de países e séries históricas de 5 indicadores da API REST do Banco Mundial.
2. **Transforma** os dados aplicando filtros, limpeza, conversão de tipos e deduplicação.
3. **Carrega** os resultados em PostgreSQL via SQLAlchemy ORM com upsert idempotente.

Indicadores coletados:

| Código WDI | Indicador | Unidade |
|---|---|---|
| `NY.GDP.PCAP.KD` | PIB per capita (USD constante 2015) | USD |
| `SP.POP.TOTL` | População total | Pessoas |
| `SH.XPD.CHEX.GD.ZS` | Gasto em saúde (% do PIB) | % PIB |
| `SE.XPD.TOTL.GD.ZS` | Gasto em educação (% do PIB) | % PIB |
| `EG.ELC.ACCS.ZS` | Acesso à eletricidade (% da população) | % |

---

## Modelo de Dados

O banco utiliza um modelo estrela simplificado com **duas dimensões** e **uma tabela de fatos**:

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
                       │  │ PK year           │
                       │  │    value          │
                       │  │    loaded_at      │
                       │  └──────────────────┘
                       │
                       └── PK composta: (iso2_code, indicator_code, year)
```

### Abordagem: SQLAlchemy ORM (DeclarativeBase)

Optou-se pelo **ORM com `DeclarativeBase`** em vez do Core (`Table` + `MetaData`) pelos seguintes motivos:

1. **Legibilidade**: os modelos são classes Python intuitivas — cada tabela é uma classe, cada coluna é um atributo.
2. **Manutenibilidade**: adicionar colunas ou tabelas é trivial; basta alterar a classe.
3. **Consistência com o ecossistema moderno**: `DeclarativeBase` é a abordagem recomendada pelo SQLAlchemy 2.0+.
4. **Compatibilidade com upsert**: o `pg_insert` do dialeto PostgreSQL funciona perfeitamente com modelos ORM, sem perder a capacidade de upsert em lote.

---

## Regras de Transformação

| Regra | Descrição | Justificativa |
|---|---|---|
| **T1 — Filtro de entidade** | Remove registros cujo ISO2 não tenha exatamente 2 caracteres. Isso descarta agregados regionais (ex.: `EAS`, `WLD`). | A API retorna tanto países reais quanto agregados. Misturá-los distorceria indicadores per capita e comparações diretas entre nações. |
| **T2 — Limpeza de strings** | Aplica `strip()` em todos os campos de texto, substitui strings vazias por `None` e padroniza regiões para title-case. | Dados da API podem vir com espaços em branco à direita ou formatação inconsistente. A padronização garante uniformidade nas consultas e agrupamentos. |
| **T3 — Conversão de tipos** | Converte `year` para `int`, `value` para `float`, `latitude` e `longitude` para `float`, com tratamento seguro (`try/except` retornando `None`). | Os campos vêm como strings na API. A conversão é necessária para cálculos e para o mapeamento correto nos tipos numéricos do PostgreSQL. |
| **T4 — Filtro temporal** | Mantém apenas registros com `year` entre 2010 e o ano corrente. | Foco na última década para manter o painel relevante e reduzir volume de dados históricos desnecessários. |
| **T5 — Deduplicação** | Remove duplicatas por `(iso2, indicator_code, year)`, mantendo o registro mais recente. Loga a quantidade de duplicatas removidas. | A API pode retornar registros duplicados entre páginas ou em revisões de dados. A deduplicação garante integridade antes do upsert. |

---

## Como Executar

### Pré-requisitos

- **Docker** e **Docker Compose** instalados.
- Acesso à internet (para consumir a API do Banco Mundial).

### Passo a passo

```bash
# 1. Clonar o repositório
git clone https://github.com/Marcio-Alexandroni/etl_worldbank.git
cd etl_worldbank

# 2. Copiar o arquivo de variáveis de ambiente
cp .env.example .env

# 3. Subir os serviços (PostgreSQL + ETL)
docker compose up --build

# 4. Após a execução do pipeline, validar com queries SQL
docker exec -it wb_postgres psql -U etl_user -d worldbank
```

O pipeline executa automaticamente ao subir o container `etl`. O PostgreSQL é inicializado com o DDL de `db/init.sql` e fica disponível na porta `5432`.

Para reexecutar o pipeline (idempotência):
```bash
docker compose up --build etl
```

---

## Consultas de Validação

As queries abaixo devem ser executadas após a primeira carga. Conecte-se ao PostgreSQL:

```bash
docker exec -it wb_postgres psql -U etl_user -d worldbank
```

### 1. Volume de países carregados

```sql
SELECT COUNT(*) FROM countries;
```

> **Saída esperada:** entre 200 e 220 (apenas países reais, sem agregados).

### 2. Distribuição por grupo de renda

```sql
SELECT income_group, COUNT(*) FROM countries
GROUP BY income_group ORDER BY 2 DESC;
```

> **Saída esperada:** grupos como "High Income", "Upper Middle Income", "Lower Middle Income", "Low Income" — sem linhas de agregados ("Aggregates" ou "Not Classified" com volume alto).

### 3. Volume e taxa de nulos por indicador

```sql
SELECT indicator_code, COUNT(*) as obs,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as nulls
FROM wdi_facts
GROUP BY indicator_code;
```

> **Saída esperada:** 5 linhas (uma por indicador), cada uma com centenas a milhares de observações.

### 4. PIB per capita — países de referência

```sql
SELECT c.name, f.year, f.value
FROM wdi_facts f
JOIN countries c ON c.iso2_code = f.iso2_code
WHERE f.indicator_code = 'NY.GDP.PCAP.KD'
  AND c.iso2_code IN ('BR','US','CN','DE','NG')
ORDER BY c.name, f.year;
```

> **Saída esperada:** séries históricas para Brazil, China, Germany, Nigeria e United States.

### 5. Teste de idempotência

```sql
-- Antes de reexecutar:
SELECT COUNT(*) FROM wdi_facts;
-- Reexecute o pipeline e verifique:
SELECT COUNT(*) FROM wdi_facts;
```

> **Saída esperada:** contagem idêntica nas duas execuções (o upsert não duplica registros).

---

## Decisões Técnicas

1. **Paginação com `per_page=1000` para indicadores**: a API do Banco Mundial permite até 32.767 registros por página. Usar 1.000 reduz o número de chamadas HTTP sem estourar memória, equilibrando performance e confiabilidade.

2. **Retry com backoff exponencial (`2^tentativa` segundos)**: a API do Banco Mundial é pública e pode apresentar instabilidades momentâneas. O backoff evita sobrecarregar o servidor e dá tempo para recuperação.

3. **Upsert em lotes de 5.000 registros**: o PostgreSQL tem limite prático para o tamanho de statements. Dividir em batches de 5k garante que nenhum INSERT fique excessivamente grande, sem sacrificar a performance da inserção em lote.

4. **Transação única por tabela com `session.begin()`**: cada tabela é carregada dentro de um context manager transacional. Em caso de falha, o rollback é automático — nenhum dado parcial persiste.

5. **Modelos ORM para as 3 tabelas**: embora o Core ofereça mais controle sobre SQL gerado, o ORM com `DeclarativeBase` foi preferido pela legibilidade e alinhamento com o SQLAlchemy 2.0+. O upsert continua sendo feito via `pg_insert` do dialeto PostgreSQL, sem perda de funcionalidade.

6. **Filtro de agregados regionais na transformação (T1)**: conforme especificado, a separação entre país real e agregado ocorre no `transform.py`, não na extração. Isso permite auditar os dados brutos extraídos antes da filtragem.

7. **`loaded_at` com `func.now()` no upsert**: ao atualizar um registro existente, o timestamp é renovado. Isso permite identificar quando cada registro foi atualizado pela última vez, útil para auditoria incremental.

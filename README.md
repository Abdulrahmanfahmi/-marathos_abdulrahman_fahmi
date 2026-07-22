# Marathos – Abdulrahman Fahmi

Ett data engineering-projekt byggt i Databricks för företaget Marathos, som arrangerar ultra-maratonlopp världen över. Projektet bygger en komplett datapipeline från rådata till en interaktiv dashboard och ett AI-drivet analysverktyg.

---

## Teknikstack

- **Databricks** — pipeline, notebook, dashboard och Genie
- **PySpark** — databearbetning och transformationer
- **Delta Lake** — tabellformat för alla lager
- **Python** — scriptfiler och hjälpfunktioner
- **SQL** — views och verifiering
- **Git & GitHub** — versionshantering

---

## Projektstruktur

```
marathos_abdulrahman_fahmi/
├── assets/                        ← Marathos-logga
├── dimensional_modeling/          ← ER-diagram (PNG + markdown)
├── explorations/
│   └── 01_eda.ipynb               ← Exploratory Data Analysis
├── transformations/
│   ├── bronze/
│   │   └── 01_bronze.ipynb        ← Inläsning av rådata
│   ├── silver/
│   │   └── 02_silver.ipynb        ← Rensning och OBT
│   └── gold/
│       └── 03_gold.ipynb          ← Dimensionell modell och views
└── utils/
    ├── helpers.py                 ← Återanvändbara funktioner (DRY)
    └── schemas.py                 ← Explicit schema för bronze
```

---

## Datapipeline — Medallion-arkitektur

### Bronze
Rådata läses in från CSV-filen `TWO_CENTURIES_OF_UM_RACES.csv` med ett explicit schema definierat i `schemas.py`. Datan sparas oförändrad som Delta-tabell i `marathos.bronze.ultra_marathon_raw`.

- **7 461 195 rader** inlästa

### Silver
Datan renas och valideras. Valideringsregeln från uppgiften implementeras:
- Kilometer- och miles-lopp ska ha prestationen i timmar
- Timbaserade lopp ska ha prestationen i kilometer
- Ogiltiga rader filtreras bort med `is_valid`-kolumnen

Unika IDs skapas med `dense_rank()`:
- `event_id` — baserat på eventnamn
- `athlete_id_new` — baserat på löpar-ID
- `result_id` — med `monotonically_increasing_id()`

Resultatet sparas som OBT (One Big Table) i `marathos.silver.ultra_marathon_obt`.

- **7 350 970 rader** efter rensning

### Gold
Ett dimensionellt stjärnschema byggs från silver-datan:

| Tabell | Beskrivning | Antal rader |
|--------|-------------|-------------|
| `dim_event` | Information om loppen | 81 900 |
| `dim_athlete` | Information om löparna | 3 931 661 |
| `fct_results` | Faktatabell med mätdata | 7 350 970 |

Fyra views skapas:
- `vw_distance_events` — kilometer och miles-lopp
- `vw_timed_events` — timbaserade lopp
- `vw_top_distance_events` — topp-lopp inom distans
- `vw_top_timed_events` — topp-lopp inom tid

---

## Utils — DRY-principen

All återanvändbar kod ligger i `utils/`:

```python
# helpers.py
get_table(table_name)              # Läser en Delta-tabell
write_delta(df, table_name)        # Skriver med overwriteSchema
add_dense_rank_id(df, col, id)     # Skapar IDs med dense_rank()
```

```python
# schemas.py
BRONZE_SCHEMA                      # Explicit StructType-schema för bronze
```

---

## Dashboard

Interaktiv dashboard byggd i Databricks AI/BI med fyra sidor:

- **Översikt** — KPI-kort, filter och grafer för hela datasetet
- **Event Analysis** — populäraste lopp, eventtyper och trender över tid
- **Athlete Analysis** — könsfördelning och prestation per ålderskategori
- **Marathos Genie** — länk till AI-verktyget

---

## Marathos Genie

Ett AI-verktyg där affärsstakeholders kan ställa frågor på naturligt språk utan att skriva SQL. Svaren är verifierade i `genie_verification`-notebooken:

| Fråga | Svar | Verifierad |
|-------|------|------------|
| Hur många resultat finns det totalt? | 7 350 970 | ✅ |
| Hur många unika lopp finns det? | 26 066 | ✅ |
| Vilket är det populäraste loppet? | Two Oceans Marathon (RSA) | ✅ |

---

## Dimensionell modell

```
fct_results
├── result_id (PK)
├── event_id (FK → dim_event)
├── athlete_id (FK → dim_athlete)
├── athlete_performance
├── performance_value
├── distance_unit
└── year_of_event

dim_event
├── event_id (PK)
├── event_name
├── event_dates
├── event_distance_length
└── distance_unit

dim_athlete
├── athlete_id (PK)
├── athlete_country
├── athlete_gender
└── athlete_year_of_birth
```

---

## Dataset

**TWO_CENTURIES_OF_UM_RACES.csv** — Ultra-maratonresultat från hela världen (1798–2022).

- 7 461 195 rader
- 13 kolumner
- 205 länder representerade
- 26 066 unika lopp

---

## AI-användning

Delar av koden är inspirerade av Claude AI för mindre uppgifter som regex-mönster och hjälpfunktioner. Alla sådana delar är kommenterade i koden med `# Inspiration from Claude AI`.

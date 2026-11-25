# Sydney Cold-Chain Logistics ETL & Analytics Pipeline

An end-to-end Data Engineering project that transforms messy daily refrigerated truck and scanner exports into a clean SQL database to track cold-chain compliance and reduce fresh food spoilage across Sydney, NSW.

## Business Problem
Refrigerated food deliveries must remain at or below **5.0°C** to prevent spoilage. Raw daily scanner logs contain duplicate entries, missing primary keys, unstandardized suburb names, corrupted timestamps, and mixed temperature units (`°C` vs `°F`)—preventing operations teams from identifying which routes and trucks cause spoilage losses.

## Project Roadmap & Progress
- [x] **Day 1 — Data Profiling & Core Python Validation:** Audited raw logs for schema anomalies and built Python validation logic (`for` loops, `if/else` filters, Pandas deduplication) to isolate invalid cargo values and missing IDs.
- [ ] **Day 2 — Data Standardization:** Convert Fahrenheit (`°F`) readings to Celsius (`°C`), flag temperature breaches (`> 5.0°C`), and calculate delivery durations.
- [ ] **Day 3 — SQL Data Warehouse:** Design a Star Schema (`fact_deliveries`, `dim_trucks`, `dim_suburbs`) and load clean records into SQLite/PostgreSQL.
- [ ] **Day 4 — Business Analytics & Live Weather API:** Query spoilage costs per suburb using SQL CTEs/Window Functions and join live Sydney temperature data.

## Tech Stack
- **Language:** Python 3 (Core Python, Pandas, NumPy)
- **Database & Querying:** SQL (SQLite / PostgreSQL)
- **Environment:** Google Colab, Git & GitHub

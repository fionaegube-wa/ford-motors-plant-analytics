# Assembly Plant Analytics (Transaction Master Build)

##  Overview
This project contains the **SQL implementation** of **Assembly Plant Transaction Master (FIN026)** — a consolidated fact table that harmonizes data from multiple operational systems including CMMS, IERP, GSDB, and Plant Hierarchy.  
The solution is to create a single source of truth for manufacturing and finance analytics.

---

##  Objective
To build a **unified master transaction table** in BigQuery that standardizes, deduplicates, and enriches production transactions across Ford’s global assembly plants — enabling accurate reporting of plant-level KPIs such as:
- Cost per unit and extended actual cost (USD)
- Variance analysis and budgeted work standards
- Scrap and adjustment tracking
- Audit-ready reconciliation across multiple systems

---

##  Technical Summary
- **Platform:** Google BigQuery  
- **Language:** SQL (Standard SQL with `EXECUTE IMMEDIATE` and `MERGE`)  
- **Data Sources:**
  - CMMS / IERP transaction messages  
  - GSDB site and supplier data  
  - Plant hierarchy and workcenter tables  
  - Part master reference  
  - ISO country codes  
  - Foreign exchange rates (LE, PO, TP, EP, CUSINV)

---

##  Key Features
- Dynamic **temp-table materialization** for each run using timestamp suffixes  
- **Automated cleanup** via 4-hour expiration windows  
- **Data standardization:** trim control characters, normalize part and department keys  
- **Effective key computation:** consistent plant, department, and work-center mapping  
- **Deduplication:** `QUALIFY ROW_NUMBER()` to retain latest records per event  
- **Incremental merge:** Upsert logic using `MERGE` into master fact table  
- **Currency normalization:** Multi-source FX joins for unified USD values  
- **Clustering:** Optimized by high-cardinality fields (plant, part) for query efficiency  
- **Error-safe casting** and date parsing with `SAFE_CAST` and `PARSE_DATE`

---

##  SQL File
- **File:** `txn_master_build.sql`  
  This script executes the end-to-end logic — from staging, joining, cleaning, and calculating, to final incremental merge into `fin026_mfg_cmms_txn_master_ct`.

---

##  Outcome / Impact
- Created a **centralized, analytics-ready transaction table** used by finance and operations teams.  
- Eliminated data silos and manual reconciliation across CMMS, IERP, and GSDB systems.  
- Improved **data quality, auditability, and refresh performance** for plant-level reporting.  
- Enabled real-time manufacturing cost and variance visibility across global plants.

---



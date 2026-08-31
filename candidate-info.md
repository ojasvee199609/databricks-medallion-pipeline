# Candidate Information

**Name:** Ojasvee  
**Role:** SSE  
**Primary Technology Stack:** Python, PySpark, SQL, Databricks  
**Primary AI Tool Used:** Cursor  
**Project Option Selected:** Data Pipeline (Medallion Architecture)  
**Assessment Start Date:** 14 August 2026  
**Submission Date:** 31 August 2026

## Tools & Environment

- Databricks: Community Edition
- Languages: Python, PySpark, SQL
- Libraries: PySpark, Delta Lake, pandas, Faker
- AI Tool: Cursor

## Setup Summary

Clone the repo into your Databricks workspace, then configure **two jobs**:

1. **Data_generation** — generates synthetic CSVs to the Unity Catalog volume `to_process/` folder.
2. **Data Ingestion and Processing** — file-arrival trigger runs Bronze → Silver → Gold → dashboard refresh.

Full local and Databricks run instructions: see `README.md`.

## Key Documentation

| Document | Purpose |
|----------|---------|
| `README.md` | Architecture, local run, Databricks jobs |
| `tool-workflow.md` | AI tool usage across the lifecycle |
| `reflection.md` | Lessons learned |
| `ai-prompts/` | Per-activity prompt history |

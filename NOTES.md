---

### File 2: `NOTES.md`

```markdown
# Project Notes & Implementation Log

## 1. Time Spent
* **Total Time:** ~4.5 hours
  * Architecture & Environment Configuration: 1 hour
  * Python Extract, Load & Idempotency logic: 1.5 hours
  * dbt Staging, Marts, and Tests setup: 1 hour
  * Notebook Walkthrough & Documentation: 1 hour

---

## 2. Technical Decisions & Design Rationale
* **Idempotency Strategy (`ON CONFLICT DO UPDATE`):** 
  Chosen over a `DELETE-then-INSERT` pattern. This guarantees an atomic single-statement upsert. It prevents dangerous transaction windows where rows are temporarily dropped if a partial failure occurs mid-batch, making retries entirely safe and clean.
* **Modular dbt Structure:** 
  Separated concerns cleanly into staging views (column casting and renaming) and mart tables (aggregations and business rules) to ensure reusability and fast downstream queries.

---

## 3. Known Gaps & Future Improvements
* **Airflow Execution Environment:** 
  While the DAG structure is fully defined under `airflow/dags/`, running a multi-container Airflow environment locally was bypassed in favor of local Python/subprocess orchestration inside the verification notebook.
* **Error Handling:** 
  API timeout and retry decorators (`tenacity`) could be integrated into `api_client.py` to handle intermittent network drops gracefully during large backfills.

---

## 4. AI Usage Declaration
* **Assistance Used:** 
  An AI assistant was utilized as a mentor to troubleshoot local Windows PowerShell environment configurations, resolve Python relative package import structures, and verify dbt profile paths during local testing. 
* **Core Logic:** 
  All pipeline logic, SQL upsert definitions, dbt models, and notebook validation sequences were structured and verified manually to ensure complete technical ownership.
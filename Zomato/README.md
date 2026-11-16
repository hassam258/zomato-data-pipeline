# Zomato dbt project

dbt models for the Zomato Data Pipeline: `models/staging` (views) → `models/marts` (dims, incremental facts, marts).
Models tagged `ai` depend on `ZOMATO.AI.REVIEW_ENRICHED` and are built in a separate step.

```bash
cp profiles.example.yml profiles.yml        # credentials come from env vars
dbt build --exclude tag:ai --profiles-dir .
dbt build --select  tag:ai --profiles-dir .
```

For setup and architecture, see the [root README](../README.md) and [ARCHITECTURE.md](../ARCHITECTURE.md).

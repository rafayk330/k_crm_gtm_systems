# K CRM – GTM Systems Architecture & RevOps Data Platform

### About K CRM

K CRM is a North American SaaS company providing CRM solutions tailored for SMBs and mid-market businesses. Serving ~2,000 accounts and 8,000 contacts with 120 active users, K CRM integrates CRM, marketing automation, product analytics, billing, and support systems into a unified platform. Its data-driven approach helps revenue, marketing, and customer success teams optimize engagement, drive predictable growth, and scale operations efficiently.

### BigQuery-Native GTM Data Pipeline • Revenue Operations Architecture • GTM Systems Mapping

This repository serves as the **technical implementation layer** for K CRM’s GTM systems - connecting CRM (Hubspot), Marketing Automation (Google Analytics 4), Product Analytics (Mixpanel), Billing (Quickbooks), Support (Zendesk), and Customer Success data into a unified warehouse-powered platform.

It provides the SQL transformations, modeling layers, data governance rules, automation logic, and activation pipelines that enable reliable GTM operations and executive decision-making.

---

# Purpose & Scope

K CRM’s revenue engine relies on accurate, integrated, and scalable data across the customer lifecycle. This repository provides:

- A centralized, governed BigQuery warehouse for all GTM systems  
- A consistent modeling pattern (**raw → stg → core → enr → retl → marts**)  
- Data pipelines powering dashboards, forecasts, and revenue metrics  
- Enrichment and scoring logic (PQL, MQL, churn, expansion signals)  
- Reverse-ETL outputs for HubSpot, Salesforce, marketing platforms, and CS tools  
- Workflow automation logic for Sales, CS, Marketing, and Product teams  
- Documentation for data governance, metric definitions, and system integrations  

This repo is the **single source of truth** for GTM data architecture within K CRM.

---

# Repository Structure

```
kcrm-gtm-systems-architecture/
│
├── README.md
├── LICENSE
│
├── /architecture/
│   ├── system-map.png
│   ├── system-map.drawio
│   ├── data-flow-diagrams/
│   ├── erd/
│   └── integration-contracts.md
│
├── /bq-pipelines/
│   ├── raw/
│   ├── stg/
│   ├── core/
│   ├── enr/
│   ├── retl/
│   ├── marts/
│   ├── orchestration/
│   └── udf/
│
├── /data-models/
│   ├── raw-definitions/
│   ├── staging-definitions/
│   ├── core-definitions/
│   ├── enrichment-definitions/
│   ├── retl-definitions/
│   ├── marts-definitions/
│   └── metric-definitions.md
│
├── /governance/
│   ├── data-dictionary/
│   ├── naming-conventions/
│   ├── data-quality-tests/
│   ├── freshness-monitoring/
│   └── pii-handling/
│
├── /analytics/
│   ├── dashboards/
│   ├── revenue/
│   ├── marketing/
│   ├── cs/
│   ├── product/
│   ├── exec/
│   ├── forecasting/
│   ├── segmentation/
│   ├── cohort-analytics/
│   └── kpi-definitions/
│
├── /revops-automation/
│   ├── lead-routing/
│   ├── lead-scoring/
│   ├── usage-triggered-flows/
│   ├── churn-signals/
│   ├── expansion-intelligence/
│   ├── pql-logic/
│   └── system-syncs/
│
└── /utils/
    ├── sql-helpers/
    ├── audit-queries/
    ├── templates/
    └── helper-scripts/
```
---

# Data Modeling Layers

### **1. raw/**
Landing zone for unmodified data ingested from CRM, MAP, product analytics, billing, and support systems.

### **2. stg/**
Standardized & cleaned staging tables with harmonized schema, naming, and types.

### **3. core/**
Canonical business entities — accounts, contacts, opportunities, plans, usage events, subscriptions, tickets.

### **4. enr/**
Enriched data assets adding calculated fields: PQL scores, churn risk, persona tagging, activation signals, ICP fit.

### **5. retl/**
Reverse-ETL-ready output tables used to sync enriched data back to operational tools like HubSpot or Salesforce.

### **6. marts/**
Dashboard-facing semantic model powering revenue analytics, product insights, marketing funnels, and executive reporting.

---

# Supported GTM Objects & Data Volumes (2022–2023)

| Object | Volume |
|--------|--------|
| Accounts | 2,000 |
| Contacts | 8,000 |
| Leads | 5,000 |
| Opportunities | 5,000 |
| Products | 12 |
| Usage Events | 150,000 |
| Login Events | 100,000 |
| Feature Adoption | 50,000 |
| Marketing Touches | 30,000 |
| Tickets | 20,000 |
| Billing Events | 24,000 |
| NPS | 2,500 |
| Churn Events | 300 |

---

# Core Capabilities in This Repo

### ✓ **Warehouse-Native ETL/ELT (BigQuery + GCP Orchestration)**
Scheduled queries + Cloud Composer DAGs to transform GTM data cleanly and reliably.

### ✓ **Data Governance & Quality**
Field definitions, naming standards, audit rules, tests, and PII handling.

### ✓ **GTM Automation Logic**
Lead scoring, routing, product-usage triggers, churn detection, and expansion intelligence.

### ✓ **BI Semantic Layers**
Consumption-ready marts used across Looker, Looker Studio, and executive reporting.

### ✓ **Reverse ETL Output Models**
Ready for syncing into HubSpot, Salesforce, MAP, CS tools, or internal applications.

---

# Documentation

Business-facing definitions, metric catalogs, lifecycle playbooks, and workflow documentation are maintained in **Confluence**, while this repository represents the **technical source of truth** for GTM systems.

---

# Contributing

Strict development and documentation standards are maintained to ensure accuracy, stability, and version consistency across the GTM data platform.

## **Branching Strategy**
- **main** → Production-ready SQL and documentation  
- **dev/\*** → Feature-specific development branches  
- **hotfix/\*** → Urgent fixes for production models  

## **Contribution Workflow**
1. Create a branch from `dev/` with a meaningful name  
2. Add or update SQL models, tests, documentation, or orchestration logic  
3. Ensure all SQL passes format checks and audit queries  
4. Update related model definitions in `/data-models/`  
5. Submit a PR with:  
   - Summary of changes  
   - Impacted models  
   - Validation steps  
6. PR must be reviewed & approved before merging into `dev/`  
7. Releases to `main` follow version-tagged deployment cycles  

## **Standards**
- Follow naming conventions in `/governance/naming-conventions`  
- All new models must include:  
  - Header documentation block  
  - Dependencies  
  - Field definitions  
  - Edge-case handling  
- Major changes must be reflected in Confluence.

---

# Roadmap

- Automated SQL lineage + CI/CD  
- Expanded data quality test coverage  
- Predictive modeling (churn, PQL, expansion propensity)  
- AI-assisted GTM workflows & agent-driven operations  
- Full automation of usage-triggered customer lifecycle motions  

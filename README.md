# 🌍 Lakehouse AI Analytics: Predictive Air Quality Platform

![Status](https://img.shields.io/badge/Status-Active-success)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Terraform](https://img.shields.io/badge/Terraform-1.0+-purple)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)

> **A Next-Generation Intelligence Platform for Real-Time Air Quality Forecasting and Analysis.**

---

## 📖 Overview

**Lakehouse AI Analytics** is an end-to-end data and AI platform designed to ingest, process, and analyze environmental data at scale. By leveraging a **Medallion Architecture** on Databricks, it transforms raw sensor data into actionable insights.

The system features a **Deep Learning (LSTM)** forecasting engine to predict future pollution levels and an autonomous **AI Agent** (powered by OpenAI GPT-4o) that serves real-time health recommendations and analytics via a high-performance **FastAPI** interface.

---

## 🏗️ Technical Architecture

This project implements a robust Data & AI Lakehouse architecture.

```mermaid
graph TD
    subgraph "Data Ingestion (Bronze)"
        Source[Sensor APIs] -->|Ingest| Runner[Pipeline Runner]
        Runner -->|Raw JSON| Bronze[(Bronze Layer)]
    end

    subgraph "Transformation (Silver)"
        Bronze -->|Clean & Schema| Silver[(Silver Layer)]
        Silver -->|Statistics| Stats[Stats & Anomalies]
    end

    subgraph "Intelligence (Gold & AI)"
        Silver -->|Aggregations| Gold[(Gold Layer)]
        Silver -->|Training Data| LSTM[LSTM Model Training]
        LSTM -->|Register| Registry[MLflow Registry]
        Registry -->|Load Model| Agent
        Gold -->|Query| Agent
    end

    subgraph "Serving Layer"
        User[End User / App] <-->|HTTP/JSON| API[FastAPI Server]
        API <-->|LangChain| Agent[AI Agent (GPT-4o)]
        Agent -->|Tools| Spark[Spark Engine]
    end
    
    style Source fill:#f9f,stroke:#333
    style Bronze fill:#dae8fc,stroke:#6c8ebf
    style Silver fill:#d5e8d4,stroke:#82b366
    style Gold fill:#ffe6cc,stroke:#d79b00
    style Agent fill:#e1d5e7,stroke:#9673a6
    style API fill:#f8cecc,stroke:#b85450
```

---

## ✨ Key Features

### 1. 🔄 Automated Data Pipeline
- **Ingestion**: Fetches real-time air quality data from global sensors (OpenAQ).
- **Processing**: Standardizes data into a Delta Lake Medallion architecture (Bronze $\to$ Silver $\to$ Gold).
- **Quality**: Automated schema validation and null handling.

### 2. 🧠 Deep Learning Forecasting
- **LSTM Network**: Uses Long Short-Term Memory networks to capture temporal dependencies in pollution data.
- **24h Forecast**: Predicts maximum Ozone ($O_3$) levels for the next day.
- **MLflow Integration**: Full experiment tracking, model registry, and version control.

### 3. 🤖 Intelligent Agent
- **Powered by GPT-4o**: Natural language understanding for complex queries.
- **Tool-Augmented**: The agent can query the Lakehouse directly (SQL), run comparisons, and fetch forecasts.
- **FastAPI Serving**: Modern, async API for low-latency responses.

### 4. ☁️ Infrastructure as Code (IaC)
- **Terraform Managed**: All resources (Jobs, Clusters, Permissions) are defined in code.
- **Reproducible**: Deploy the entire stack with a single command.

---

## 📂 Project Structure

```bash
lakehouse-ai-analytics/
├── notebooks/                  # Databricks Notebooks
│   ├── pipeline_runner.py      # Main ETL orchestrator
│   └── ml_pollution_forecasting.py  # LSTM Training & MLflow
├── src/
│   ├── data_ingestion/         # API Connectors (OpenAQ)
│   ├── transformation/         # Spark Transformations
│   └── ai/                     # AI Agent Logic
│       ├── agent.py            # LangChain Agent Definition
│       ├── agent_serving.py    # FastAPI Application
│       └── tools.py            # Lakehouse Tools (Spark/SQL)
├── terraform/                  # Infrastructure as Code
│   ├── main.tf                 # Job Definitions
│   └── variables.tf            # Configuration
└── SETUP_GUIDE.md              # Deployment Instructions
```

---

## 🚀 Getting Started

To deploy this project to your Databricks environment, please refer to the detailed **[Setup Guide](SETUP_GUIDE.md)**.

### Quick Command Reference

**1. Configure Secrets**
```bash
databricks secrets create-scope --scope ai_scope
databricks secrets put --scope ai_scope --key openai_api_key
```

**2. Deploy Infrastructure**
```bash
cd terraform
terraform init
terraform apply
```

**3. Run the Pipeline**
Trigger the **Air Quality Pipeline Runner** job in Databricks Workflows.

---

## 📊 Business Value

*   **Public Health**: Provide proactive warnings 24 hours in advance of dangerous pollution levels.
*   **Urban Planning**: Identify pollution hotspots and trends to inform city policy.
*   **Accessibility**: Natural language interface makes complex data accessible to non-technical users.

---
*Built with ❤️ using Databricks, LangChain, and TensorFlow.*

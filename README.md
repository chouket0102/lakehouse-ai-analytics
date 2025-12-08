# 🌍 Lakehouse AI Analytics: Predictive Air Quality Platform

<p align="center">
  <img src="https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white">
  <img src="https://img.shields.io/badge/GPT-10A37F?style=flat&logo=openai&logoColor=white">
</p>

> **A Next-Generation Intelligence Platform for Real-Time Air Quality Forecasting and Analysis.**

---

## 📖 Overview

**Lakehouse AI Analytics** is an **end-to-end data and AI platform** designed to ingest, process, and analyze environmental data at scale. By leveraging the **Medallion Architecture** on Databricks, it transforms raw sensor data into actionable insights.

The system features a **Deep Learning (LSTM)** forecasting engine to predict future pollution levels and an autonomous **AI Agent** (powered by OpenAI GPT-4o) that serves real-time health recommendations and complex analytics via a high-performance **FastAPI** interface.

---

## 🏗️ Technical Architecture

This project implements a robust Data & AI Lakehouse architecture with a clear separation of concerns, managed entirely by **Terraform**.



### Databricks Medallion & Data Flow

| Layer | Purpose | Key Operations | Artifacts |
| :--- | :--- | :--- | :--- |
| **🥇 Gold** | **Business-Level Data** | Aggregation, business logic, feature engineering. | Final, optimized Delta Tables for BI & AI. |
| **🥈 Silver** | **Clean & Validated Data** | Cleansing, de-duplication, schema enforcement, type casting. |Delta Tables, ready for modeling. |
| **🥉 Bronze** | **Raw Ingestion** | Store data exactly as received from the API. | Delta Tables. |

---

## ✨ Key Features

### 1. 🔄 Automated Data Pipeline
* **Ingestion**: Fetches real-time air quality data from global sensors (OpenAQ).
* **Processing**: Standardizes data into a **Delta Lake Medallion architecture** (Bronze $\to$ Silver $\to$ Gold).
* **Quality**: Automated schema validation, data quality checks, and null handling.

### 2. 🧠 Deep Learning Forecasting
* **LSTM Network**: Uses **Long Short-Term Memory** networks to effectively capture temporal dependencies in pollution time-series data.
* **24h Forecast**: Predicts maximum Ozone ($O_3$) levels for the next day.
* **MLflow Integration**: Full **experiment tracking**, model registry, and version control for model lifecycle management.

### 3. 🤖 Intelligent Agent
* **Powered by GPT-4o**: Provides **natural language understanding** for complex queries and insights generation.
* **Tool-Augmented**: The agent utilizes **LangChain** to autonomously query the Lakehouse (via SQL/Spark), run comparisons, and fetch forecasts.
* **FastAPI Serving**: Modern, **async API** for low-latency, high-performance responses.

### 4. ☁️ Infrastructure as Code (IaC)
* **Terraform Managed**: All cloud and Databricks resources (Jobs, Clusters, Permissions) are defined and managed as code.
* **Reproducible**: Deploy the entire stack into any environment with a single, reliable command.

---

## 📂 Project Structure

```bash
lakehouse-ai-analytics/
├── notebooks/                     # Databricks Notebooks
│   ├── pipeline_runner.py         # Main ETL orchestrator
│   └── ml_pollution_forecasting.py # LSTM Training & MLflow
├── src/
│   ├── data_ingestion/            # API Connectors (OpenAQ)
│   ├── transformation/            # Spark Transformations
│   └── ai/                        # AI Agent Logic
│       ├── agent.py               # LangChain Agent Definition
│       ├── agent_serving.py       # FastAPI Application
│       └── tools.py               # Lakehouse Tools (Spark/SQL)
├── terraform/                     # Infrastructure as Code
│   ├── main.tf                    # Job, Cluster, and Permission Definitions
│   └── variables.tf               # Configuration variables
└── SETUP_GUIDE.md                 # Deployment Instructions

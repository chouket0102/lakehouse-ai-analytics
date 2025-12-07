# 🛠️ Deployment & Setup Guide

This guide provides step-by-step instructions to deploy the Lakehouse AI Analytics platform using Terraform and configure the necessary credentials.

##  Prerequisites

Before you begin, ensure you have the following:

1.  **Terraform CLI** installed ([Download](https://developer.hashicorp.com/terraform/downloads)).
2.  **Databricks CLI** installed.
3.  **Databricks Workspace** access.
4.  **Databricks Personal Access Token** (PAT).
5.  **OpenAI API Key** (sk-...).

---

##  Step 1: Configure Credentials

### 1.1 Databricks Access
You need your Databricks Workspace URL and a Personal Access Token.
- **URL**: Found in your browser address bar (e.g., `https://adb-123456789.1.azuredatabricks.net`).
- **Token**: Go to `Settings` > `Developer` > `Access Tokens` > `Generate New Token`.

### 1.2 OpenAI API Key (Secret Scope)
The AI Agent requires an OpenAI API key. For security, we store this in a Databricks Secret Scope.

1.  **Create a Secret Scope** (if not exists):
    Open your terminal or Databricks CLI and run:
    ```bash
    databricks secrets create-scope ai_scope
    ```
    *Alternatively, use the UI at `https://<databricks-instance>#secrets/createScope`.*

2.  **Add the OpenAI Key**:
    ```bash
    databricks secrets put-secret ai_scope openai_api_key
    ```
    *This will open an editor (or prompt). Paste your `sk-...` key and save.*

    > **Note:** The Terraform configuration references this secret as `{{secrets/ai_scope/openai_api_key}}`.

---

##  Step 2: Configure Terraform

Navigate to the `terraform` directory:

```bash
cd terraform
```

Create a `terraform.tfvars` file to store your specific configuration. **Do not commit this file to Git.**

**File:** `terraform.tfvars`
```hcl
databricks_host  = "https://adb-YOUR_WORKSPACE_ID.azuredatabricks.net"
databricks_token = "dapi..."
cluster_id       = "1234-567890-abcdef12" # Optional: existing cluster ID
notebook_path    = "/Workspace/Users/your.email@example.com/lakehouse-ai-analytics"
```

---

##  Step 3: Run Terraform

Initialize Terraform to download providers:
```bash
terraform init
```

Preview the changes (Plan):
```bash
terraform plan
```
*Review the output to ensure it will create the 3 jobs: `pipeline_runner`, `model_training`, and `agent_serving`.*

Apply the configuration:
```bash
terraform apply
```
*Type `yes` when prompted to confirm.*

---

##  Step 4: Verify Deployment

1.  **Go to your Databricks Workspace**.
2.  Navigate to **Workflows (Jobs)**.
3.  You should see three new jobs:
    *   **Air Quality Pipeline Runner**: Ingests and processes data.
    *   **Air Quality Model Training**: Trains the LSTM forecast model.
    *   **Air Quality Agent Serving**: Runs the FastAPI server for the AI Agent.

### Running the System
1.  **Trigger the Pipeline Runner** first to populate your tables (Bronze/Silver/Gold).
2.  **Trigger the Model Training** job to train and register the initial model.
3.  **Start the Agent Serving** job to bring the API online.

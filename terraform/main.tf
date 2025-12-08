terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

# 1. Pipeline Runner Job
resource "databricks_job" "pipeline_runner" {
  name = "Air Quality Pipeline Runner"

  task {
    task_key = "run_pipeline"

    notebook_task {
      notebook_path = "${var.notebook_path}/notebooks/pipeline_runner"
    }
    
    # Using specific cluster if provided, otherwise assume one is needed or use new_cluster
    existing_cluster_id = var.cluster_id
  }
  
  # Schedule (e.g., daily at 6 AM)
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "UTC"
  }
}

# 2. Model Training Job
resource "databricks_job" "model_training" {
  name = "Air Quality Model Training"

  task {
    task_key = "train_model"

    notebook_task {
      notebook_path = "${var.notebook_path}/notebooks/ml_pollution_forecasting"
    }

    existing_cluster_id = var.cluster_id
  }

  # Dependency: Run after pipeline data is ready? 
  # For now, separate schedule or trigger.
  schedule {
    quartz_cron_expression = "0 0 8 * * ?"
    timezone_id            = "UTC"
  }
}

# 3. Agent Deployment Job (FastAPI)
# This job runs the FastAPI server. It is a long-running job.
resource "databricks_job" "agent_serving" {
  name = "Air Quality Agent Serving"

  task {
    task_key        = "run_agent_server"
    environment_key = "Default"
    max_retries     = 3

    spark_python_task {
      python_file = "${var.notebook_path}/src/ai/agent_serving.py"
      parameters  = ["--openai_api_key", "{{secrets/ai_scope/openai_api_key}}"]
    }
    
    # Using Serverless compute (Workspace default/enforced)
    # Dependencies are installed at runtime via the python script
  }

  environment {
    environment_key = "Default"
    spec {
      client = "1"
      dependencies = [
        "fastapi",
        "uvicorn",
        "langchain",
        "langchain-openai"
      ]
    }
  }
}

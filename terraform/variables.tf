variable "databricks_host" {
  description = "The Databricks workspace URL"
  type        = string
}

variable "databricks_token" {
  description = "The Databricks access token"
  type        = string
  sensitive   = true
}

variable "cluster_id" {
  description = "The ID of the existing interactions cluster to use for jobs"
  type        = string
  default     = "" # Optional: can also create a new cluster in main.tf if preferred
}

variable "notebook_path" {
  description = "Base path for notebooks in the workspace"
  type        = string
  default     = "/Workspace/Users/user@example.com/lakehouse-ai-analytics"
}

variable "git_repo_url" {
  description = "URL of the git repository"
  type        = string
  default     = "https://github.com/chouket0102/lakehouse-ai-analytics"
}

variable "git_branch" {
  description = "Git branch to use"
  type        = string
  default     = "main"
}

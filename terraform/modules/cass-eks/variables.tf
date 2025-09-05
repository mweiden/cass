variable "region" {
  description = "AWS region"
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "cluster_version" {
  description = "Kubernetes version for the EKS cluster"
  type        = string
  default     = "1.29"
}

variable "vpc_id" {
  description = "VPC ID for the EKS cluster"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for worker nodes"
  type        = list(string)
}

variable "node_instance_type" {
  description = "EC2 instance type for worker nodes"
  type        = string
  default     = "t3.medium"
}

variable "node_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "node_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 3
}

variable "node_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 1
}

variable "cass_image" {
  description = "Container image for cass"
  type        = string
  default     = "ghcr.io/example/cass:latest"
}

variable "cass_replicas" {
  description = "Number of cass replicas"
  type        = number
  default     = 1
}

variable "namespace" {
  description = "Kubernetes namespace for cass"
  type        = string
  default     = "cass"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for cass storage"
  type        = string
}

variable "s3_force_destroy" {
  description = "Force destroy S3 bucket even if not empty"
  type        = bool
  default     = false
}

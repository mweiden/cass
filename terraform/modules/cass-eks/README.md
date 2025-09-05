# cass-eks Terraform Module

This module deploys the `cass` database to an AWS Elastic Kubernetes Service (EKS) cluster and configures S3 for storage.

## Usage

```hcl
module "cass" {
  source = "./modules/cass-eks"

  region            = "us-east-1"
  cluster_name      = "cass-cluster"
  vpc_id            = "vpc-123456"
  subnet_ids        = ["subnet-1", "subnet-2"]
  s3_bucket_name    = "my-cass-data"
}
```

The module creates an S3 bucket for durable storage and deploys a Kubernetes `Deployment` of the `cass` container within the EKS cluster. Set `cass_image` to use a custom container image.

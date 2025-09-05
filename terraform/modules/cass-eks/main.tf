terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.0"
    }
  }
}

provider "aws" {
  region = var.region
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = ">= 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version
  vpc_id          = var.vpc_id
  subnets         = var.subnet_ids

  eks_managed_node_groups = {
    default = {
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
      instance_types = [var.node_instance_type]
    }
  }
}

resource "aws_s3_bucket" "data" {
  bucket        = var.s3_bucket_name
  force_destroy = var.s3_force_destroy
}

data "aws_eks_cluster" "this" {
  name = module.eks.cluster_name
}

data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_name
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.this.token
}

resource "kubernetes_namespace" "cass" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_deployment" "cass" {
  metadata {
    name      = "cass"
    namespace = kubernetes_namespace.cass.metadata[0].name
    labels = {
      app = "cass"
    }
  }

  spec {
    replicas = var.cass_replicas

    selector {
      match_labels = {
        app = "cass"
      }
    }

    template {
      metadata {
        labels = {
          app = "cass"
        }
      }

      spec {
        container {
          name  = "cass"
          image = var.cass_image

          env {
            name  = "CASS_STORAGE"
            value = "s3"
          }

          env {
            name  = "CASS_S3_BUCKET"
            value = aws_s3_bucket.data.bucket
          }
        }
      }
    }
  }
}

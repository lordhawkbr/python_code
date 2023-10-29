# Arquivo de configuração do Terraform
terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "3.27"
    }
  }
  required_version = ">= 0.13.4"
}

# Configuração do provider AWS. A conta e a região que irei utilizar
provider "aws" {
    profile = var.aws_profile
    region = var.aws_region
}

# [S3] Definição das variáveis para
locals {
  files_to_upload = ["../myClasses/ClassDownload.py","../myClasses/ClassWorkWithFiles.py", "../configs/dbConfig.py", "../myClasses/ETL.py", "../main.py", "../README.md","../configs/script.py"]
}

# [S3] Criação de um bucket S3
resource "aws_s3_bucket" "proj-repository-bucket" {
  bucket = "proj-repository-bucket"
}

# [S3] Copiar o repositorio local para o bucket proj-repository-bucket
resource "aws_s3_bucket_object" "proj-repository-bucket" {
    for_each = toset(local.files_to_upload)

    bucket = aws_s3_bucket.proj-repository-bucket.id
    key    = basename(each.value)
    source = each.value
}

# [rede] VPC
resource "aws_vpc" "my_vpc" {
  cidr_block = "10.0.0.0/16"
  tags = {
    Name = "my_vpc"
  }
}

# [rede] Subnet
resource "aws_subnet" "my_subnet" {
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = "10.0.1.0/24"
  tags = {
    Name = "my_subnet"
  }
}

# [rede] Subnet 2
resource "aws_subnet" "my_subnet2" {
  vpc_id                  = aws_vpc.my_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-east-2b"
  map_public_ip_on_launch = true

  tags = {
    Name = "my_subnet2"
  }
}

# [rede] Subnet Group
resource "aws_db_subnet_group" "my_db_subnet_group" {
  name       = "my-database-subnet-group"
  subnet_ids = [aws_subnet.my_subnet.id, aws_subnet.my_subnet2.id]

  tags = {
    Name = "my-database-subnet-group"
  }
}



# [rede] Criação de um security group para que a instância acesse a web
resource "aws_security_group" "allow_ec2_outbound" {
  name        = "allow_ec2_outbound"
  description = "Permitir tafego de saida do EC2"
  vpc_id = aws_vpc.my_vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# [rede] Criação de um security group para que a instância acesse o SGBD
resource "aws_security_group" "allow_ec2_to_rds" {
  name        = "allow_ec2_to_rds"
  description = "Allow EC2 to RDS traffic"
  vpc_id = aws_vpc.my_vpc.id

  ingress {
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = [ aws_subnet.my_subnet.cidr_block ]
  }
}

# [IAM] Policy para permitir acesso ao S3 e RDS
resource "aws_iam_policy" "access_s3_and_rds" {
  name        = "AccessS3AndRDS"
  description = "Política para permitir acesso ao S3 e RDS"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:*"],
        Resource = ["arn:aws:s3:::proj-repository-bucket", "arn:aws:s3:::proj-repository-bucket/*"]
      },
      {
        Effect    = "Allow",
        Action    = ["rds:*"],
        Resource  = "arn:aws:rds:${var.aws_region}:${var.aws_account_profile1}:db:proj_db" 
      }
    ]
  })

}

# [IAM] Role para a instância EC2
resource "aws_iam_role" "ec2_role" {
  name = "EC2S3RDSRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
}

# [IAM] Anexar a política à role
resource "aws_iam_role_policy_attachment" "ec2_s3_rds_attachment" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = aws_iam_policy.access_s3_and_rds.arn

}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "ec2_instance_profile"
  role = aws_iam_role.ec2_role.name
}


# [EC2] Criação de uma instância EC2 e tranferir o repositorio do bucket S3 para a instância EC2
# 1. Instalar o python3
# 2. Instalar o awscli
# 3. Copiar o repositorio do bucket S3 para a instância EC2
# 4. Criar um cronjob para executar o script ETL.py a cada 24 horas

resource "aws_instance" "my_instance" {
  ami           = "ami-0e83be366243f524a"
  instance_type = "t2.micro"
  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name

  user_data = <<-EOF
              #!/bin/bash
              yum update -y 
              yum install python3 -y
              pip install asyncio os sqlalchemy urllib csv zipfile io aiofiles datetime pandas
              curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
              unzip awscliv2.zip
              sudo ./aws/install
              mkdir /repositorio/
              aws s3 sync s3://proj-repository-bucket/ /repositorio/
              echo "0 0 * * * root python3 /repositorio/main.py" >> /etc/crontab
              EOF

  tags = {
    Name = "my-instance"
  }
  vpc_security_group_ids = [ aws_security_group.allow_ec2_outbound.id, aws_security_group.allow_ec2_to_rds.id ]
  subnet_id = aws_subnet.my_subnet.id
}


# [RDS] Criação de uma instância RDS com MySQL
resource "aws_db_instance" "my_db" {
  name = "proj_db"
  allocated_storage    = 20
  storage_type         = "gp2"
  engine               = "mysql"
  engine_version       = "5.7"
  instance_class       = "db.t2.micro"
  username             = var.db_username
  password             = var.db_password
  parameter_group_name = "default.mysql5.7"
  skip_final_snapshot = true
  vpc_security_group_ids = [aws_security_group.allow_ec2_to_rds.id]
  db_subnet_group_name = aws_db_subnet_group.my_db_subnet_group.name
}

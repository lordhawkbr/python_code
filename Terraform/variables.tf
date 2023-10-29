# Variaveis que serão passadas para o arquivo variables.tf
# O default é o valor que será utilizado caso não seja passado nenhum valor para a variavel usando o arquivo terraform.tfvars

variable "db_username" {
  type = string
  default = "root"
}

variable "db_password" {
  type = string
  default = "root"
}

variable "aws_profile" {
  type = string
  default = "san.sev2"
}

variable "aws_account_profile1" {
  type = string
  default = "247238014383"
}

variable "aws_region" {
  type = string
  default = "us-east-2"
  
}
variable "local_path" {
  type = string
  default = "C:\Users\Marllon\Downloads\PROJETO PUC\Downloads Files\python_code"
}
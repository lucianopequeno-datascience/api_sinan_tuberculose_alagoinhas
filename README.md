# ODA - Pipeline de Dados: Tuberculose (SINAN)

## Visão Geral
Este repositório contém o pipeline automatizado de extração, processamento e carga (ETL) dos dados epidemiológicos de Tuberculose do Sistema de Informação de Agravos de Notificação (SINAN/DATASUS). 

O projeto é uma peça central do **Observatório de Dados de Alagoinhas (ODA)**, iniciativa da consultoria **TERRITÓRIO**, focada em centralizar e monitorar indicadores de saúde pública municipal.



## Arquitetura e Fluxo de Dados
O pipeline foi desenhado para ser resiliente e escalável:
1.  **Ingestão:** Conexão direta com o FTP do DATASUS via `PySUS`.
2.  **Processamento:** Filtro de registros específicos do município de Alagoinhas (código `290070`).
3.  **Armazenamento:** Exportação em formato estruturado (CSV) e upload para o Google Cloud Storage (`dados_alagoinhas_bronze`).
4.  **Orquestração:** Execução agendada via Cloud Run Job.

## Stack Tecnológica
- **Linguagem:** Python 3.x
- **Bibliotecas:** `PySUS`, `Pandas`, `Google Cloud Storage Client`
- **Infraestrutura:** Google Cloud Platform (Cloud Run, Cloud Build, Cloud Storage)

## Principais Funcionalidades
* **Resiliência:** O script utiliza tratamento de exceções (try/except) dentro do loop de processamento, permitindo que falhas em um ano específico não interrompam a execução dos demais.
* **Incrementalidade:** O design do código facilita a inclusão de novos anos de série histórica sem necessidade de refatoração, sendo ideal para atualizações anuais automatizadas.
* **Eficiência:** O pipeline realiza a limpeza de arquivos temporários após o upload, otimizando o uso de recursos de computação.

## Configuração e Execução

### Pré-requisitos
- Conta GCP com permissões de acesso ao `Cloud Storage`.
- Google Cloud SDK configurado localmente.
- Variáveis de ambiente configuradas para autenticação GCP.

### Como rodar localmente
1. Clone o repositório:
   ```bash
   git clone [seu-repositorio]

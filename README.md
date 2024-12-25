# Projeto: Pipeline de Engenharia de Dados na Azure

## Descrição do Projeto
Desenvolvemos um pipeline completo de engenharia de dados para uma empresa imobiliária utilizando a plataforma Azure. O objetivo foi criar um Data Lake para ingestão e transformação de dados sobre preços de imóveis na cidade do Rio de Janeiro. As etapas do pipeline incluem:

- **Camada Inbound**: Ingestão dos dados brutos.
- **Camada Bronze**: Transformações iniciais e armazenamento em formato Delta.
- **Camada Silver**: Estruturação avançada dos dados e armazenamento em formato Delta.

O pipeline foi configurado para execução automática a cada hora, garantindo a atualização contínua e consistente dos dados.

---

## Ferramentas Utilizadas
- **Azure**: Plataforma de cloud para gerenciamento de recursos.
- **Databricks**: Desenvolvimento e execução de notebooks para transformação de dados.
- **Azure Data Factory**: Orquestração e agendamento do pipeline.

---

## Configuração do Projeto

### 1. Configuração de Recursos Azure
- Criação de conta Azure e monitoramento de custos.
- Configuração de grupos de recursos para gerenciamento.

### 2. Data Lake
- Estruturação do **Azure Data Lake Gen 2** com camadas **Inbound**, **Bronze** e **Silver**.
- Upload do dataset inicial: `dataset_imoveis_bruto.json`.

### 3. Registro de Aplicativo e Permissões
- Configuração de credenciais para acesso seguro ao Data Lake.
- Utilização de IAM e ACL para gerenciamento de permissões.

### 4. Configuração do Databricks
- Criação de workspace e clusters no Databricks.
- Integração com GitHub para controle de versão.
- Configuração de acesso ao Data Lake.

---

## Transformações de Dados

### Camada Bronze
- Remoção de informações irrelevantes, como imagens e usuários.
- Adição de uma coluna de identificação única (`id`).
- Salvamento em formato Delta.

### Camada Silver
- Conversão de campos JSON em colunas individuais.
- Exclusão de colunas desnecessárias.
- Salvamento em formato Delta.

---

## Automação e Orquestração

### Azure Data Factory
- Criação e configuração de pipelines para orquestrar a execução dos notebooks Databricks.
- Configuração de gatilhos para execução automática a cada hora.

---

## Testes e Deploy

- Testes de execução para validar o pipeline e detectar erros.
- Configuração de gatilhos no Data Factory para execução automática sempre que novos dados forem detectados no Data Lake.

---

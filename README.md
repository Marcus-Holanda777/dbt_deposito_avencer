# 🚀 Projeto Controle de Estoque e Validade

## 📝 Visão Geral

Este projeto combina o uso do **dbt** para modelagem e processamento SQL no **AWS Athena** com uma etapa complementar em **Pandas** para análise avançada de estoque e validade.

O objetivo principal é:

- Monitorar e consolidar o estoque disponível por depósito e produto, considerando reservas e validade dos lotes.
- Simular o consumo de produtos baseando-se na regra do **PVPS (Primeiro Vence, Primeiro Sai)**, respeitando o Prazo Médio de Estoque (PME) e o tempo de vida útil do produto.
- Integrar a média diária de vendas dos últimos 90 dias para um cálculo preciso do giro de estoque e risco de vencimento.
- Gerar bases de dados para análises, tomadas de decisão e painéis de controle.

## 🏗 Estrutura do Projeto

### 1️⃣ Camada Staging (dbt)

- Importação e tratamento dos dados brutos de estoque, entradas, lotes, vendas e mapeamento filial-depósito.

### 2️⃣ Camada Intermediária (dbt)

- Consolidação do estoque real disponível.
- Ajuste das datas de vencimento dos lotes e quantidades.
- Consolidação das vendas por depósito.

### 3️⃣ Camada de Transformação Avançada (dbt)

- Cálculo da representatividade dos lotes no estoque.
- Filtragem para cobrir 100% do estoque.
- Distribuição proporcional do estoque entre lotes considerando validade.
- Incorporação da média diária de vendas para cálculo do PME.

### 4️⃣ Etapa de Simulação (Pandas)

- Utiliza os dados gerados pelo dbt para aplicar a regra **PVPS (Primeiro Vence, Primeiro Sai)**.
- Realiza simulação do consumo acumulado, respeitando o PME e o prazo de vida útil do produto.
- Identifica os produtos e lotes com maior risco de vencimento.
- Gera alertas e indicadores para auxiliar na gestão preventiva de estoque.
- Para baixar os dados do Athena para o Pandas, é utilizada a biblioteca **[athena-mvsh](https://github.com/Marcus-Holanda777/athena-mvsh)**, desenvolvida pelo mantenedor do projeto, que facilita a extração eficiente dos dados.

## 🛠 Tecnologias Utilizadas

- **dbt**: modelagem, orquestração e documentação de dados em SQL.
- **AWS Athena**: processamento serverless de consultas SQL.
- **Pandas (Python)**: análise avançada e simulação de consumo para PVPS.
- **athena-mvsh**: biblioteca custom para interação entre Athena e Pandas.
- **S3**: armazenamento de dados intermediários e resultados.

## ⚙️ Como Executar

1. Configure o ambiente dbt com conexão para AWS Athena.
2. Clone este repositório.
3. Execute `dbt deps` e `dbt run` para gerar as tabelas e views no Athena.
4. Utilize a biblioteca **athena-mvsh** para baixar os dados gerados para o ambiente Python/Pandas.
5. Execute o script Pandas para realizar a simulação PVPS e obter os produtos a vencer.
6. Utilize os resultados para alimentar dashboards ou sistemas de alerta.

## ✅ Testes e Qualidade

- Testes de qualidade são aplicados em dbt, garantindo integridade e coerência dos dados.
- A etapa Pandas inclui validações para assegurar o cumprimento das regras PVPS e consistência dos cálculos.
- Recomenda-se monitorar logs e resultados para ajustes finos conforme evolução dos dados.


## ❌ Ajustes !

- [x] LOJA incluir data de vencimento
- [x] LOJA incluir categoria nivel 1 ate 5
- [ ] criar planilha consolidada
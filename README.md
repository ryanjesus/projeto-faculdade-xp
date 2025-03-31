# projeto-faculdade-xp


O tema do desafio final é a construção de Pipelines ETL com integração do Kafka 
com uma database (postgresql) usando kafka connect e entrega em data lake com 
kafka connect. Todos os serviços que compõem o kafka e o database PostgreSQL 
que servirá de fonte serão implantados com docker-compose. 

Portanto, deve desenvolver uma solução prática de Engenharia de Dados que 
implemente a criação de pipelines ETL utilizando o modelo bronze, silver e gold, 
processados com Apache Spark SQL API e integrados a um datalake no Amazon 
S3 via Kafka Connect. 

## Requisitos: 
### 1. Pipeline Bronze (Ingestão Bruta) 
• Fonte de Dados: arquivo JSON fornecido (com sujeiras e duplicações). 

• Ferramenta: Spark SQL para carregar os dados e criar uma tabela 
temporária ou persistente (formato Parquet ou Delta). 

• Processamento: Carregar dados brutos para a camada Bronze, sem transformação 
além da validação do esquema em um banco de dados (por exemplo, 
PostgreSQL). 


### 2. Pipeline Silver (Limpeza e Transformação) 
• Fonte de Dados: Tabela Bronze.

• Ferramenta: Spark SQL para limpeza e transformações. 

• Processamento: 

o Remover duplicações. 

o Tratar dados ausentes (ex.: preencher valores nulos ou descartar 
registros inválidos). 

o Ajustar colunas para um formato consistente (ex.: normalizar nomes). 

o Salvar os dados limpos em uma tabela Silver em um banco de dados 
(por exemplo, PostgreSQL). 

### 3. Pipeline Gold (Agregação e Enriquecimento) 

• Fonte de Dados: Tabela Silver. 

• Ferramenta: Spark SQL para realizar agregações e cálculos. 

• Processamento: 

o Gerar métricas agregadas (ex.: número de usuários ativos, média de 
idade). 

o Criar a camada Gold contendo dados prontos para consumo analítico 
em um banco de dados (por exemplo, PostgreSQL).

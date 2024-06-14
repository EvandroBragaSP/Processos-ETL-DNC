# Traffic ETL DAG

#Descrição:

Extração: Nesta etapa, os dados são extraídos da API. Isso geralmente envolve fazer solicitações à API e receber os dados em um formato como JSON ou XML. As solicitações à API podem ser feitas usando várias técnicas, como solicitações HTTP GET ou POST.
Transformação: Os dados extraídos da API são então transformados. Isso pode envolver uma variedade de operações, como limpeza de dados (por exemplo, remoção de valores nulos ou duplicados), conversão de tipos de dados, aplicação de cálculos ou agregações, e assim por diante. O objetivo é garantir que os dados estejam no formato correto e sejam úteis para o propósito pretendido.
Carga: Finalmente, os dados transformados são carregados em uma tabela PostgreSQL. Isso geralmente envolve estabelecer uma conexão com o banco de dados PostgreSQL e inserir os dados na tabela apropriada.

Estrutura do Projeto:

DAG: traffic_etl_dag
Tarefas da DAG:
create_traffic_tables: Cria as tabelas necessárias no banco de dados PostgreSQL.
extract_traffic_data: Extrai dados de tráfego da API do Google Maps Directions.
transform_and_load_data: Transforma os dados extraídos e os carrega na tabela traffic.

Requisitos:

Apache Airflow;
PostgreSQL;
Requests (biblioteca Python para fazer requisições HTTP).

Configuração:
Clone o repositório:

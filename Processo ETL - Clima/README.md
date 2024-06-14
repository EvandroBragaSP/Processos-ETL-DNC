# Weather ETL DAG

Este projeto contém uma DAG (Directed Acyclic Graph) do Apache Airflow para extrair, transformar e carregar dados de clima.

## Configurações da API e do Banco de Dados

As configurações da API e do banco de dados são definidas no início do código. A API utilizada é a OpenWeatherMap e os dados são armazenados em um banco de dados PostgreSQL.

## Funções

O código contém várias funções que são usadas para extrair, transformar e carregar os dados:

- `create_table()`: Esta função cria uma tabela no PostgreSQL para armazenar os dados de clima. Se a tabela já existir, a função não faz nada.

- `extract_weather_data()`: Esta função extrai dados de clima da API OpenWeatherMap. Os dados brutos são retornados como um dicionário Python.

- `transform_weather_data(ti)`: Esta função transforma os dados brutos em um formato mais útil para serem inseridos no banco de dados. Os dados transformados são armazenados para a próxima tarefa usando o método `xcom_push()` do Apache Airflow.

- `load_weather_data(ti)`: Esta função carrega os dados transformados no banco de dados PostgreSQL. Os dados são inseridos na tabela criada pela função `create_table()`.

## DAG

A DAG é definida no final do código. Ela contém quatro tarefas que são executadas na seguinte ordem: `create_table_task` -> `extract_weather_data_task` -> `transform_weather_data_task` -> `load_weather_data_task`.

Cada tarefa é uma instância da classe `PythonOperator` do Apache Airflow. O argumento `python_callable` de cada tarefa é definido como a função correspondente.

## Como executar

Para executar esta DAG, você precisa ter o Apache Airflow instalado e configurado em seu ambiente. Depois de ter o Airflow configurado, você pode copiar o código DAG para o diretório de DAGs do Airflow e a DAG aparecerá automaticamente na interface do usuário do Airflow.


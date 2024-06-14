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


# Traffic ETL DAG

## Descrição

Este projeto utiliza o Apache Airflow para criar uma DAG (Directed Acyclic Graph) que realiza a extração, transformação e carga (ETL) de dados de tráfego.

## Extração

Nesta etapa, os dados são extraídos da API. Isso geralmente envolve fazer solicitações à API e receber os dados em um formato como JSON ou XML. As solicitações à API podem ser feitas usando várias técnicas, como solicitações HTTP GET ou POST.

## Transformação

Os dados extraídos da API são então transformados. Isso pode envolver uma variedade de operações, como limpeza de dados (por exemplo, remoção de valores nulos ou duplicados), conversão de tipos de dados, aplicação de cálculos ou agregações, e assim por diante. O objetivo é garantir que os dados estejam no formato correto e sejam úteis para o propósito pretendido.

## Carga

Finalmente, os dados transformados são carregados em uma tabela PostgreSQL. Isso geralmente envolve estabelecer uma conexão com o banco de dados PostgreSQL e inserir os dados na tabela apropriada.

## Estrutura do Projeto

A DAG é chamada `traffic_etl_dag` e contém as seguintes tarefas:

- `create_traffic_tables`: Cria as tabelas necessárias no banco de dados PostgreSQL.
- `extract_traffic_data`: Extrai dados de tráfego da API do Google Maps Directions.
- `transform_and_load_data`: Transforma os dados extraídos e os carrega na tabela traffic.

## Requisitos

- Apache Airflow
- PostgreSQL
- Requests (biblioteca Python para fazer requisições HTTP)

## Configuração

1. Clone o repositório: `git clone https://github.com/seu-usuario/traffic-etl-dag.git`
2. Navegue até o diretório do projeto: `cd traffic-etl-dag`
3. Instale as dependências: Certifique-se de ter um ambiente virtual configurado e ativado. Em seguida, instale as dependências: `pip install -r requirements.txt`
4. Configuração do Airflow: Configure sua instância do Airflow. Certifique-se de ter um `airflow.cfg` configurado corretamente. Atualize o arquivo `airflow.cfg` com as informações do banco de dados PostgreSQL.
5. Variáveis do Airflow: No Airflow, configure a conexão para o PostgreSQL com o ID `conn_datalake_path`.
6. Credenciais da API: Atualize a variável `API_KEY` no arquivo da DAG (`traffic_etl_dag.py`) com sua chave de API do Google Maps Directions.

## Uso

1. Inicie o Airflow: `airflow webserver` e `airflow scheduler`
2. Carregue a DAG: Coloque o arquivo `traffic_etl_dag.py` na pasta `dags` do Airflow.
3. Execute a DAG: No Airflow UI, ative e execute a DAG `traffic_etl_dag`.

## Detalhes da Implementação

### Funções da DAG

- `create_tables()`: Cria a tabela `traffic` no banco de dados PostgreSQL se ela não existir.
- `extract_traffic_data()`: Faz uma requisição para a API do Google Maps Directions para obter dados de tráfego entre duas localizações específicas.
- `transform_and_load_data()`: Transforma os dados extraídos e os insere na tabela `traffic`.

### Estrutura da Tabela `traffic`

A tabela `traffic` contém os seguintes campos:

- `id`: Chave primária, serial.
- `start_address`: Endereço de início.
- `start_location_lat`: Latitude da localização de início.
- `start_location_lng`: Longitude da localização de início.
- `end_address`: Endereço de destino.
- `end_location_lat`: Latitude da localização de destino.
- `end_location_lng`: Longitude da localização de destino.
- `distance_text`: Texto da distância.
- `distance_value`: Valor da distância.
- `duration_text`: Texto da duração.
- `duration_value`: Valor da duração.
- `overview_polyline`: Polilinha de visão geral.
- `summary`: Resumo.
- `copyright`: Direitos autorais.
- `status`: Status.

## Contribuição

1. Faça um fork do projeto
2. Crie sua branch de feature (`git checkout -b feature/nova-feature`)
3. Faça commit das suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Faça push para a branch (`git push origin feature/nova-feature`)
5. Crie um novo Pull Request

## Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.

## Contato

Para mais informações, entre em contato com evandrobraga2005@gmail.com.

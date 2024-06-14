from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configurações da API e do Banco de Dados
POSTGRES_CONN_ID = "conn_datalake_path"  # Identificador da conexão com o PostgreSQL
TABLE_NAME = 'traffic'  # Nome da tabela onde os dados de tráfego serão armazenados
API_URL = "https://maps.googleapis.com/maps/api/directions/json"  # URL da API do Google Directions
ORIGIN = "-23.5345,-46.4696"  # Coordenadas de origem para a consulta da API
DESTINATION = "-23.5362,-46.5604"  # Coordenadas de destino para a consulta da API
API_KEY = "AIzaSyAu9wN2CDJN43KGGiCyEeWtThhvPSZyE20"  # Chave de API do Google Directions

# Função para criar as tabelas no PostgreSQL
def create_tables():
    # Query SQL para criar a tabela de tráfego, se ela não existir
    create_traffic_table_query = """
    CREATE TABLE IF NOT EXISTS traffic (
        id SERIAL PRIMARY KEY,
        start_address TEXT,
        start_location_lat NUMERIC,
        start_location_lng NUMERIC,
        end_address TEXT,
        end_location_lat NUMERIC,
        end_location_lng NUMERIC,
        distance_text TEXT,
        distance_value INTEGER,
        duration_text TEXT,
        duration_value INTEGER,
        overview_polyline TEXT,
        summary TEXT,
        copyright TEXT,
        status TEXT
    );
    """
    # Conexão com o banco de dados usando o PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Executa a query para criar a tabela
        cursor.execute(create_traffic_table_query)
        conn.commit()
        print("Tabelas criadas com sucesso ou já existem.")
    except Exception as e:
        print(f"Erro ao criar as tabelas no PostgreSQL: {e}")
        raise
    finally:
        # Fecha o cursor e a conexão com o banco de dados
        cursor.close()
        conn.close()

# Função para extrair dados da API do Google Directions
def extract_traffic_data():
    # Parâmetros para a requisição à API
    params = {
        "origin": ORIGIN,
        "destination": DESTINATION,
        "key": API_KEY
    }
    try:
        # Realiza a requisição GET para a API e verifica se houve erros
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        # Converte a resposta em JSON
        raw_data = response.json()
        return raw_data
    except requests.RequestException as e:
        raise Exception(f"Erro ao buscar dados de tráfego: {e}")

# Função para transformar e carregar os dados extraídos para o PostgreSQL
def transform_and_load_data(**context):
    # Recupera os dados brutos da tarefa anterior usando XCom
    raw_data = context['ti'].xcom_pull(task_ids='extract_traffic_data')
    
    # Query SQL para inserir os dados na tabela de tráfego
    insert_traffic_query = """
    INSERT INTO traffic (
        start_address, start_location_lat, start_location_lng, end_address, end_location_lat, end_location_lng,
        distance_text, distance_value, duration_text, duration_value, overview_polyline, summary, copyright, status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """
    # Conexão com o banco de dados usando o PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Executa a query para inserir os dados e faz o commit da transação
        cursor.execute(insert_traffic_query, (
            raw_data['routes'][0]['legs'][0]['start_address'],
            raw_data['routes'][0]['legs'][0]['start_location']['lat'],
            raw_data['routes'][0]['legs'][0]['start_location']['lng'],
            raw_data['routes'][0]['legs'][0]['end_address'],
            raw_data['routes'][0]['legs'][0]['end_location']['lat'],
            raw_data['routes'][0]['legs'][0]['end_location']['lng'],
            raw_data['routes'][0]['legs'][0]['distance']['text'],
            raw_data['routes'][0]['legs'][0]['distance']['value'],
            raw_data['routes'][0]['legs'][0]['duration']['text'],
            raw_data['routes'][0]['legs'][0]['duration']['value'],
            raw_data['routes'][0].get('overview_polyline', {}).get('points', ''),
            raw_data['routes'][0].get('summary', ''),
            raw_data.get('copyrights', ''),
            raw_data['status']
        ))
        conn.commit()
        print("Dados inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir dados no PostgreSQL: {e}")
        conn.rollback()
        raise
    finally:
        # Fecha o cursor e a conexão com o banco de dados
        cursor.close()
        conn.close()

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',  # Proprietário da DAG
    'start_date': days_ago(1),  # Data de início da execução da DAG
}

# Definição da DAG para ETL de dados de tráfego
with DAG(
    'traffic_etl_dag',  # Nome da DAG
    default_args=default_args,  # Argumentos padrão aplicados a todas as tarefas
    description='DAG para extrair, transformar e carregar dados de tráfego',  # Descrição da DAG
    schedule_interval=None,  # Intervalo de agendamento da DAG
) as dag:

    # Tarefa para criar as tabelas no banco de dados
    create_tables_task = PythonOperator(
        task_id='create_traffic_tables',  # Identificador da tarefa
        python_callable=create_tables  # Função Python a ser chamada pela tarefa
    )

    # Tarefa para extrair dados da API do Google Directions
    extract_traffic_data_task = PythonOperator(
        task_id='extract_traffic_data',  # Identificador da tarefa
        python_callable=extract_traffic_data  # Função Python a ser chamada pela tarefa
    )

    # Tarefa para transformar e carregar os dados extraídos para o banco de dados
    transform_and_load_data_task = PythonOperator(
        task_id='transform_and_load_data',  # Identificador da tarefa
        python_callable=transform_and_load_data,  # Função Python a ser chamada pela tarefa
        provide_context=True  # Permite que a função receba o contexto de execução
    )

    # Definição da ordem de execução das tarefas na DAG
    create_tables_task >> extract_traffic_data_task >> transform_and_load_data_task

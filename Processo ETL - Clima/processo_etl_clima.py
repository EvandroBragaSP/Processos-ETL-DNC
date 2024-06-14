from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configurações da API e do Banco de Dados
POSTGRES_CONN_ID = "conn_datalake_path"
TABLE_NAME = 'weather'
API_URL_TEMPLATE = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
LAT = -23.5489
LON = -46.6388
API_KEY = "73704b281b52a347e83a94d0e1778394"

# Função para criar a tabela no PostgreSQL
def create_table():
    # Query SQL para criar a tabela
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather (
        latitude NUMERIC,
        longitude NUMERIC,
        weather_id INTEGER,
        weather_main VARCHAR(50),
        weather_description VARCHAR(255),
        weather_icon VARCHAR(10),
        base VARCHAR(50),
        temperature_celsius NUMERIC,
        feels_like_celsius NUMERIC,
        temp_min_celsius NUMERIC,
        temp_max_celsius NUMERIC,
        pressure INTEGER,
        humidity INTEGER,
        visibility INTEGER,
        wind_speed NUMERIC,
        wind_deg INTEGER,
        cloudiness INTEGER,
        dt TIMESTAMP,
        country VARCHAR(10),
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        timezone INTEGER,
        city_id INTEGER,
        city_name VARCHAR(255),
        cod INTEGER
    )
    """
    # Conexão com o PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Executa a query de criação da tabela
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela weather criada com sucesso ou já existe.")
    except Exception as e:
        print(f"Erro ao criar a tabela no PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Função para extrair dados da API
def extract_weather_data():
    # Formata a URL da API com os parâmetros corretos
    api_url = API_URL_TEMPLATE.format(lat=LAT, lon=LON, api_key=API_KEY)
    try:
        # Faz a requisição para a API
        response = requests.get(api_url)
        response.raise_for_status()
        # Retorna os dados brutos da resposta
        raw_data = response.json()
        return raw_data
    except requests.RequestException as e:
        raise Exception(f"Erro ao buscar dados de clima: {e}")

# Função para transformar os dados
def transform_weather_data(ti):
    # Recupera os dados brutos da tarefa anterior
    raw_data = ti.xcom_pull(task_ids='extract_weather_data')
    if raw_data:
        # Transforma os dados brutos em um formato mais útil
        transformed_data = {
            'latitude': raw_data['coord']['lat'],
            'longitude': raw_data['coord']['lon'],
            'weather_id': raw_data['weather'][0]['id'],
            'weather_main': raw_data['weather'][0]['main'],
            'weather_description': raw_data['weather'][0]['description'],
            'weather_icon': raw_data['weather'][0]['icon'],
            'base': raw_data['base'],
            'temperature_celsius': raw_data['main']['temp'] - 273.15,
            'feels_like_celsius': raw_data['main']['feels_like'] - 273.15,
            'temp_min_celsius': raw_data['main']['temp_min'] - 273.15,
            'temp_max_celsius': raw_data['main']['temp_max'] - 273.15,
            'pressure': raw_data['main']['pressure'],
            'humidity': raw_data['main']['humidity'],
            'visibility': raw_data['visibility'],
            'wind_speed': raw_data['wind']['speed'],
            'wind_deg': raw_data['wind']['deg'],
            'cloudiness': raw_data['clouds']['all'],
            'dt': datetime.utcfromtimestamp(raw_data['dt']).isoformat(),
            'country': raw_data['sys']['country'],
            'sunrise': datetime.utcfromtimestamp(raw_data['sys']['sunrise']).isoformat(),
            'sunset': datetime.utcfromtimestamp(raw_data['sys']['sunset']).isoformat(),
            'timezone': raw_data['timezone'],
            'city_id': raw_data['id'],
            'city_name': raw_data['name'],
            'cod': raw_data['cod']
        }
        # Armazena os dados transformados para a próxima tarefa
        ti.xcom_push(key='transformed_data', value=transformed_data)
    else:
        raise ValueError("Nenhum dado retornado na extração de dados.")

# Função para carregar os dados no banco de dados
def load_weather_data(ti):
    # Recupera os dados transformados da tarefa anterior
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_weather_data')
    if transformed_data:
        try:
            # Conexão com o PostgreSQL
            postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()

            # Query SQL para inserir os dados na tabela
            insert_query = """
                INSERT INTO weather (
                    latitude, longitude, weather_id, weather_main, weather_description, weather_icon, base, temperature_celsius, 
                    feels_like_celsius, temp_min_celsius, temp_max_celsius, pressure, humidity, visibility, wind_speed, 
                    wind_deg, cloudiness, dt, country, sunrise, sunset, timezone, city_id, city_name, cod
                ) VALUES (
                    %(latitude)s, %(longitude)s, %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s, 
                    %(base)s, %(temperature_celsius)s, %(feels_like_celsius)s, %(temp_min_celsius)s, %(temp_max_celsius)s, 
                    %(pressure)s, %(humidity)s, %(visibility)s, %(wind_speed)s, %(wind_deg)s, %(cloudiness)s, %(dt)s, 
                    %(country)s, %(sunrise)s, %(sunset)s, %(timezone)s, %(city_id)s, %(city_name)s, %(cod)s
                )
            """
            
            # Executa a query de inserção com os dados transformados
            cursor.execute(insert_query, transformed_data)
            conn.commit()
            print("Dados inseridos com sucesso.")
        except Exception as e:
            print(f"Erro ao inserir dados no banco de dados: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    else:
        raise ValueError("Nenhum dado transformado para carregar.")

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Definição da DAG
with DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='DAG para extrair, transformar e carregar dados de clima',
    schedule_interval=None,
) as dag:

    # Definição das tarefas da DAG
    create_table_task = PythonOperator(
        task_id='create_weather_table',
        python_callable=create_table
    )

    extract_weather_data_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )

    transform_weather_data_task = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data
    )

    load_weather_data_task = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data
    )

    # Definição da ordem de execução das tarefas
    create_table_task >> extract_weather_data_task >> transform_weather_data_task >> load_weather_data_task

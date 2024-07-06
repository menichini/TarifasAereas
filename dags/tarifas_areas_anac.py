from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pandas import read_csv
from numpy import int8, int32


DAG_ID = 'tarifas_areas_anac'

@dag(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'start_date': '2024-07-01',
    },
    catchup=False,
)
def tarifas_areas_anac_dag():

    @task
    def extracao_e_limpeza(data_path: str):
        """
            Extrai os dados do caminho passado e faz a limpeza dos dados
        """

        # Quando o caminho começa com 'TARIFA'
        # existe uma coluna a mais no arquivo
        # não especificada no dicionário, ignorá-la

        if not data_path.find("TARIFA"):
            usecols = list(range(7))
        else:
            usecols = list(range(1, 8))

        # Cabeçalho do arquivo
        # para mapear as colunas que irão ser convertidas os tipos
        cabecalho = read_csv(
            data_path,nrows=1, sep=';', encoding='latin1',
            usecols=usecols, header=None
        ).values[0]

        tipos_colunas_ano_mes_tarifa_assento = {
            cabecalho[0]: int8, cabecalho[1]: int8,
            cabecalho[5]: float, cabecalho[6]: int32
        }

        dados_tarifas = read_csv(
            data_path, sep=';', encoding='latin1',
            usecols=usecols, header=0,
            dtype=tipos_colunas_ano_mes_tarifa_assento,
            decimal=',')

        dados_tarifas.columns = [
            "ano", "mes", "empresa_registrou_a_viagem", "aeroporto_origem",
            "aeroporto_destino", "valor_tarifa", "assentos_comercializados"
        ]

        print(dados_tarifas.info())

    extracao_e_limpeza("/opt/airflow/data/TARIFA_N_202305.CSV")

dag = tarifas_areas_anac_dag()

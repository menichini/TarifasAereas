"""
Pipeline dos dados da ANAC
"""
import os
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pandas import read_csv, concat, DataFrame
from numpy import int8, int16, int32


DAG_ID = 'tarifas_areas_anac'

@dag(
    dag_id=DAG_ID,
    schedule_interval='@daily',
    default_args={
        "owner": "airflow",
        "start_date": "2024-07-01",
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
            data_path,nrows=1, sep=';', encoding="latin1",
            usecols=usecols, header=None
        ).values[0]

        tipos_colunas_ano_mes_tarifa_assento = {
            cabecalho[0]: int16, cabecalho[1]: int8,
            cabecalho[5]: float, cabecalho[6]: int32
        }

        dados_tarifas = read_csv(
            data_path, sep=';', encoding="latin1",
            usecols=usecols, header=0,
            dtype=tipos_colunas_ano_mes_tarifa_assento,
            decimal=',')

        dados_tarifas.columns = [
            "ano", "mes", "empresa_registrou_a_viagem", "aeroporto_origem",
            "aeroporto_destino", "valor_tarifa", "assentos_comercializados"
        ]

        return (dados_tarifas.to_dict(), data_path.split("/")[-1])

    @task
    def conversao_oaci(tarifas_filename: tuple[dict, str]):
        """
            Converte os códigos OACI e faz a carga dos dados
        """
        tarifas, filename = tarifas_filename

        public_airfields_path = "/opt/airflow/data/metadata/cadastro-de-aerodromos-civis-publicos.csv"
        private_airfields_path = "/opt/airflow/data/metadata/AerodromosPrivados.csv"
        output_dir = "/opt/airflow/data/output"

        # Garantir que o diretório de saída exista
        os.makedirs(output_dir, exist_ok=True)

        public_airfields_df = read_csv(
            public_airfields_path, encoding="latin1", delimiter=';', usecols=[0, 1, 2, 3, 4],
            skiprows=1
        )
        private_airfields_df = read_csv(
            private_airfields_path, encoding="latin1", delimiter=';', usecols=[0, 1, 2, 3, 4],
            skiprows=1
        )
        company_registred_flights = read_csv(
            "/opt/airflow/data/metadata/iata-icao.csv", encoding="latin1",
            delimiter=',', usecols=[2, 4]
        )

        public_airfields_df.columns = ["Código OACI", "CIAD", "Nome", "Município", "UF"]
        private_airfields_df.columns = ["Código OACI", "CIAD", "Nome", "Município", "UF"]

        all_airfields_selected = concat([public_airfields_df, private_airfields_df])
        all_airfields_selected.set_index("Código OACI", inplace=True)
        company_registred_flights.set_index("iata", inplace=True)

        tarifas = DataFrame(tarifas)
        tarifas["aeroporto_origem"] = tarifas.apply(
            lambda row: all_airfields_selected["Nome"].get(
                row["aeroporto_origem"], "Aeroporto sem código OACI encontrado"), axis=1
        )

        tarifas["aeroporto_destino"] = tarifas.apply(
            lambda row: all_airfields_selected["Nome"].get(
                row["aeroporto_destino"], "Aeroporto sem código OACI encontrado"), axis=1
        )

        tarifas["empresa_registrou_a_viagem"] = tarifas.apply(
            lambda row: company_registred_flights["airport"].get(
                row["empresa_registrou_a_viagem"], "Empresa sem código ICAO encontrado"), axis=1
        )

        tarifas.to_csv(
            os.path.join(output_dir, f"{filename}.csv"), sep=';', index=False, encoding="latin1"
        )

    tarifas = extracao_e_limpeza("/opt/airflow/data/TARIFA_N_202305.CSV")

    chain(tarifas, conversao_oaci(tarifas))

dag = tarifas_areas_anac_dag()

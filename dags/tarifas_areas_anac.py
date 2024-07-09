"""
Pipeline dos dados da ANAC
"""
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pandas import read_csv
from numpy import int8, int16, int32
import os


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
            cabecalho[0]: int16, cabecalho[1]: int8,
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
        return dados_tarifas.to_dict(orient='records')

    @task
    
   def conversao_oaci():
        """
            Converte os códigos OACI e faz a carga dos dados
        """
        # Definir os caminhos dos arquivos
        public_airfields_path = "/opt/airflow/data/metadata/cadastro-de-aerodromos-civis-publicos.csv"
        private_airfields_path = "/opt/airflow/data/metadata/AerodromosPrivados.csv"
        output_dir = "/opt/airflow/TarifasAereas/data/Output"

        # Garantir que o diretório de saída exista
        os.makedirs(output_dir, exist_ok=True)

        # Carregar os arquivos CSV com a codificação 'latin1' e delimitador ';'
        public_airfields_df = pd.read_csv(public_airfields_path, encoding='latin1', delimiter=';', usecols=[1, 2, 3, 4])
        private_airfields_df = pd.read_csv(private_airfields_path, encoding='latin1', delimiter=';', usecols=[1, 2, 3, 4, 5])

        # Selecionar os campos necessários e renomear as colunas para um formato consistente
        public_airfields_df.columns = ['Código OACI', 'CIAD', 'Nome', 'Município']
        private_airfields_df.columns = ['Código OACI', 'CIAD', 'Nome', 'Município', 'UF']

        # Adicionar a coluna 'UF' ao dataframe public_airfields_df com valores nulos, se necessário
        if 'UF' not in public_airfields_df.columns:
            public_airfields_df['UF'] = None

        # Concatenar os dois DataFrames
        all_airfields_selected = pd.concat([public_airfields_df, private_airfields_df])

        # Salvar os dados extraídos em formato JSON e XLS
        output_json_path = os.path.join(output_dir, "conversao_oaci.json")
        output_xls_path = os.path.join(output_dir, "conversao_oaci.xls")

        all_airfields_selected.to_json(output_json_path, orient='records', lines=True)
        all_airfields_selected.to_excel(output_xls_path, index=False)

        # Imprimir os 10 primeiros registros para validação
        print(all_airfields_selected.head(10))

    tarifas = extracao_e_limpeza("/opt/airflow/data/TARIFA_N_202305.CSV")

    chain(tarifas, conversao_oaci())

dag = tarifas_areas_anac_dag()
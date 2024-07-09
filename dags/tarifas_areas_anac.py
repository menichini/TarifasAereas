"""
Pipeline dos dados da ANAC
"""
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pandas import read_csv
from numpy import int8, int16, int32


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
    def conversao_oaci(dados_tarifas: dict):
        """
            Converte os códigos OACI e faz a carga dos dados
        """
    # Definir os caminhos dos arquivos
    public_airfields_path = "data/metadata/cadastro-de-aerodromos-civis-publicos.csv"
    private_airfields_path = "data/metadata/AerodromosPrivados.csv"
    output_dir = "TarifasAereas/data/Output"

    # Carregar os arquivos CSV com a codificação 'latin1' e delimitador ';'
    public_airfields_df = pd.read_csv(public_airfields_path, encoding='latin1', delimiter=';')
    private_airfields_df = pd.read_csv(private_airfields_path, encoding='latin1', delimiter=';')

    # Selecionar os campos necessários
    fields = ['Código OACI', 'CIAD', 'Nome', 'Município', 'UF']
    public_airfields_selected = public_airfields_df[fields]
    private_airfields_selected = private_airfields_df[fields]

    # Excluir a primeira linha
    public_airfields_selected = public_airfields_selected.iloc[1:]
    private_airfields_selected = private_airfields_selected.iloc[1:]

    # Concatenar os dois DataFrames
    all_airfields_selected = pd.concat([public_airfields_selected, private_airfields_selected])

    # Salvar os dados extraídos em formato JSON e XLS
    output_json_path = f"{output_dir}/conversao_oaci.json"
    output_xls_path = f"{output_dir}/conversao_oaci.xls"

    all_airfields_selected.to_json(output_json_path, orient='records', lines=True)
    all_airfields_selected.to_excel(output_xls_path, index=False)

    # Visualização dos 10 primeiros registros para validação
    print(all_airfields_selected.head(10))

        print(dados_tarifas)
        return dados_tarifas


    tarifas = extracao_e_limpeza("/opt/airflow/data/TARIFA_N_202305.CSV")

    chain(tarifas, conversao_oaci(tarifas))

dag = tarifas_areas_anac_dag()

## Sobre

Projeto final da disciplina Fundamentos em BigData do curso
de pós-graduação DevOps da UFMT. Utilização de ubuntu/debian para executar o projeto. O arquivo [.env](./.env) incluído por não tratar de credenciais fora usuário necessário para o airflow. Serão incluídos formas de instalação à parte do docker para realizar o desenvolvimento das DAGs. Link dos dados: https://dados.gov.br/dados/conjuntos-dados/voos-e-operaes-areas---tarifas-areas-domsticas. Link do collab: https://colab.research.google.com/drive/1r4-QtATiNM95Ht6vrvImyJ2AHV3u9Tg4

## Requerimentos
- Docker
- Python
- Pyenv
- Poetry
- Airflow

## Build

- Instalação do Python - Confira  uma das formas definidas em https://python.org.br/

- Instalação e configuração do pyenv e a versão do python do projeto
```sh
# Instalação
curl https://pyenv.run | bash
```

```sh
# Configuração de ambiente do pyenv (fazer o export e eval sempre que inicializar o terminal)
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```
```sh
# Instalar a mesma versão do python da imagem do airflow
pyenv install 3.12.4
# Selecionar a versão do python pro repositório
pyenv local 3.12.4
```

- Instalação e configuração do Poetry
```sh
pip install poetry
poetry install --no-root
```
"""Atalho para rodar a pipeline. Executa o mesmo flow do Prefect.

(Temporário: pode ser removido; o caminho oficial é o orchestrate_prefect.py.)
"""
from orchestrate_prefect import run_etl

if __name__ == "__main__":
    run_etl()

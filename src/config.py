import os
from dotenv import load_dotenv

load_dotenv()


def _obrigatoria(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise RuntimeError(f"Variável '{nome}' não definida no .env")
    return valor


MONGODB_URI: str = _obrigatoria("MONGODB_URI")
DATABASE_URL: str = _obrigatoria("DATABASE_URL")
SQLITE_PATH: str = os.getenv("SQLITE_PATH", "weather.db")
import os
import sqlite3
from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi


class Loading:

    def __init__(self):
        """Carrega variáveis do .env e inicializa conexões com Mongo e SQLite."""
        load_dotenv()
        self.uri = os.getenv("MONGODB_URI")
        self.client = MongoClient(self.uri, server_api=ServerApi("1"))
        self.conn = sqlite3.connect("weather.db")

    def load_sqlite(self, df: pd.DataFrame, table_name: str) -> None:
        """Salva o DataFrame transformado em uma tabela no SQLite.

        Parameters
        ----------
        df : pd.DataFrame
            Dado a ser inserido no SQLite.
        table_name : str
            Nome da tabela de destino no banco de dados.
        """
        df.to_sql(table_name, con=self.conn, if_exists="replace", index=False)

        print(f"Dados enviados para a tabela '{table_name}' no SQLite com sucesso! ✅")
        print("--------------------------")

    def load_mongo(self, data: dict | pd.DataFrame | list, db_name: str, collection: str) -> None:
        """
        Envia o resultado bruto para uma coleção no MongoDB.

        Parameters
        ----------
        data : dict | pd.DataFrame | list
            Dado a ser inserido no banco.
        db_name : str
            Nome do banco de dados no Mongo.
        collection : str
            Nome da coleção de destino.

        """
        db = self.client[db_name]
        coll = db[collection]

        # Se for DataFrame
        if isinstance(data, (pd.DataFrame, pd.Series)):
            df = data.to_frame() if isinstance(data, pd.Series) else data.copy()
            df_reset = df.reset_index()

            for col in df_reset.select_dtypes(include=["datetime64", "datetimetz"]).columns:
                df_reset[col] = df_reset[col].astype(str)

            payload = df_reset.to_dict(orient="records")
            if payload:
                coll.insert_many(payload)

        # Se for lista
        elif isinstance(data, list):
            if data:
                coll.insert_many(data)

        # Se for dicionário
        elif isinstance(data, dict):
            coll.insert_one(data)

        # Se for string isolada
        elif isinstance(data, str):
            coll.insert_one({"dado": data})

        print("Dados brutos enviados para o Mongo DB com sucesso! ✅")
        print("--------------------------")
        
    def close_connections(self):
        """Fecha as conexões ativas."""
        self.conn.close()
        self.client.close()    
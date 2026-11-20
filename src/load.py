from pymongo import MongoClient, ReplaceOne
from pymongo.server_api import ServerApi
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from src import config

# Chave natural de um documento bruto: uma extração de uma cidade.
CHAVE_BRONZE: list[str] = ["cidade", "data_extracao"]
LOTE_UPSERT: int = 1000


class Loading:
    """Carga nas camadas da pipeline.

    * bronze -> MongoDB (JSON bruto, índice único por extração)
    * prata  -> Neon: forecast_daily, forecast_hourly
    * ouro   -> Neon: urban_risk_daily
    * dimensão -> Neon: dim_city (localidades)
    """

    # Chave primária de cada tabela do Neon. É o que torna a carga idempotente:
    # o upsert usa essas colunas no ON CONFLICT.
    CHAVES_PRIMARIAS: dict[str, list[str]] = {
        "dim_city": ["city"],
        "forecast_daily": ["city", "date"],
        "forecast_hourly": ["city", "time"],
        "urban_risk_daily": ["city", "date"],
    }

    def __init__(self):
        """Inicializa o carregador. Cada conexão só é aberta quando for usada."""
        self._client: MongoClient | None = None
        self._engine = None

    # ---------- conexões (abertas sob demanda) ----------

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._client = MongoClient(config.MONGODB_URI, server_api=ServerApi("1"))
        return self._client

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(config.DATABASE_URL)
        return self._engine

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close_connections()

    # ---------- Neon (PostgreSQL) ----------

    def create_schema(self) -> None:
        """Cria as tabelas do Neon (se ainda não existirem), com tipos e chaves.

        A dim_city vem primeiro porque as outras tabelas a referenciam.
        """
        ddl = [
            # dimensão: localidades (uf, mesorregião IBGE, região do app, coordenadas)
            """
            CREATE TABLE IF NOT EXISTS dim_city (
                city        TEXT PRIMARY KEY,
                nome        TEXT NOT NULL,
                uf          CHAR(2) NOT NULL,
                mesorregiao TEXT NOT NULL,
                regiao      TEXT NOT NULL,
                latitude    DOUBLE PRECISION NOT NULL,
                longitude   DOUBLE PRECISION NOT NULL
            )
            """,
            # prata: previsão diária
            """
            CREATE TABLE IF NOT EXISTS forecast_daily (
                city                            TEXT NOT NULL REFERENCES dim_city(city),
                date                            DATE NOT NULL,
                temperature_2m_max              DOUBLE PRECISION,
                temperature_2m_min              DOUBLE PRECISION,
                uv_index_max                    DOUBLE PRECISION,
                precipitation_sum               DOUBLE PRECISION,
                rain_sum                        DOUBLE PRECISION,
                precipitation_probability_max   DOUBLE PRECISION,
                wind_speed_10m_max              DOUBLE PRECISION,
                wind_gusts_10m_max              DOUBLE PRECISION,
                data_extracao                   TIMESTAMP NOT NULL,
                PRIMARY KEY (city, date)
            )
            """,
            # prata: previsão horária (time = horário local de Brasília)
            """
            CREATE TABLE IF NOT EXISTS forecast_hourly (
                city                        TEXT NOT NULL REFERENCES dim_city(city),
                time                        TIMESTAMP NOT NULL,
                temperature_2m              DOUBLE PRECISION,
                precipitation_probability   DOUBLE PRECISION,
                precipitation               DOUBLE PRECISION,
                wind_speed_10m              DOUBLE PRECISION,
                wind_gusts_10m              DOUBLE PRECISION,
                data_extracao               TIMESTAMP NOT NULL,
                PRIMARY KEY (city, time)
            )
            """,
            # ouro: índices de risco urbano por cidade e dia
            """
            CREATE TABLE IF NOT EXISTS urban_risk_daily (
                city                TEXT NOT NULL REFERENCES dim_city(city),
                date                DATE NOT NULL,
                is_forecast         BOOLEAN NOT NULL,
                chuva_72h           DOUBLE PRECISION,
                chuva_7d            DOUBLE PRECISION,
                risco_alagamento    DOUBLE PRECISION,
                risco_deslizamento  DOUBLE PRECISION,
                risco_queda_arvore  DOUBLE PRECISION,
                risco_buracos       DOUBLE PRECISION,
                risco_calor         DOUBLE PRECISION,
                risco_geral         DOUBLE PRECISION,
                risco_principal     TEXT,
                nivel_risco         TEXT,
                data_extracao       TIMESTAMP NOT NULL,
                PRIMARY KEY (city, date)
            )
            """,
        ]
        with self.engine.begin() as conn:
            for comando in ddl:
                conn.execute(text(comando))
        print("Schema do Neon verificado ✅")

    def upsert_neon(self, registros: list[dict], table_name: str) -> int:
        """Grava os registros no Neon com upsert pela chave primária da tabela.

        Linhas novas são inseridas; linhas que já existem (mesma chave) são
        atualizadas. Rodar de novo com os mesmos dados não duplica nada.

        Parameters
        ----------
        registros : list[dict]
            Linhas a gravar (chaves = nomes das colunas).
        table_name : str
            Tabela de destino (precisa estar em Loading.CHAVES_PRIMARIAS).

        Returns
        -------
        int
            Quantidade de linhas enviadas.
        """
        if not registros:
            return 0

        pk = self.CHAVES_PRIMARIAS[table_name]
        tabela = Table(table_name, MetaData(), autoload_with=self.engine)
        colunas = list(registros[0].keys())

        with self.engine.begin() as conn:
            for i in range(0, len(registros), LOTE_UPSERT):
                stmt = insert(tabela).values(registros[i:i + LOTE_UPSERT])
                stmt = stmt.on_conflict_do_update(
                    index_elements=pk,
                    set_={c: stmt.excluded[c] for c in colunas if c not in pk},
                )
                conn.execute(stmt)

        print(f"{len(registros)} linhas gravadas (upsert) na tabela '{table_name}' do Neon ✅")
        print("--------------------------------------------------")
        return len(registros)

    # ---------- MongoDB (bronze) ----------

    def load_mongo(self, data: dict | list[dict], db_name: str, collection: str) -> None:
        """Envia o resultado bruto para uma coleção no MongoDB, sem duplicar.

        A coleção tem um índice único em (cidade, data_extracao). O documento é
        gravado com upsert por essa chave: se a mesma extração for enviada de
        novo (por exemplo, num retry do Prefect), ela é substituída, não duplicada.

        Parameters
        ----------
        data : dict | list[dict]
            Documento(s) bruto(s), com as chaves 'cidade' e 'data_extracao'.
        db_name : str
            Nome do banco de dados no Mongo.
        collection : str
            Nome da coleção de destino.
        """
        coll = self.client[db_name][collection]
        coll.create_index(
            [(c, 1) for c in CHAVE_BRONZE],
            unique=True,
            name="uniq_cidade_data_extracao",
            # documentos antigos sem essas chaves ficam fora do índice
            partialFilterExpression={c: {"$exists": True} for c in CHAVE_BRONZE},
        )

        docs = [data] if isinstance(data, dict) else list(data)
        operacoes = [
            ReplaceOne({c: d[c] for c in CHAVE_BRONZE}, d, upsert=True) for d in docs
        ]
        if operacoes:
            coll.bulk_write(operacoes)

        print("Dados brutos enviados para o Mongo DB com sucesso! ✅")
        print("--------------------------")

    def close_connections(self) -> None:
        """Fecha as conexões que foram abertas."""
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
        print('conexões fechadas com sucesso! ✅')

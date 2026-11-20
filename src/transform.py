"""Transform: transformação com PySpark, bronze (Mongo) -> prata e ouro (Neon).

Fluxo:
1. Os documentos brutos do Mongo entram no Spark como texto JSON e são lidos
   com um schema explícito (from_json), montado a partir de Extract.VARIAVEIS.
2. Prata: as listas paralelas da API (time, temperature_2m_max, ...) viram
   linhas com arrays_zip + explode. Nada é descartado e nulos continuam nulos.
3. Ouro: índices de risco urbano por cidade e dia, com janelas (Window) para
   a chuva acumulada em 72h e em 7 dias.

Não há UDFs Python: tudo roda em funções nativas do Spark (JVM).
"""

import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

from src.config import LIMIARES
from src.extract import Extract

FUSO = "America/Sao_Paulo"


COLUNAS_OURO: list[str] = [
    "city", "date", "is_forecast", "chuva_72h", "chuva_7d",
    *[f"risco_{n}" for n in LIMIARES],
    "risco_geral", "risco_principal", "nivel_risco", "data_extracao",
]


def _json_default(valor):
    """Serializa datas do Mongo como ISO em UTC, sem fuso (TIMESTAMP_NTZ no Spark)."""
    if isinstance(valor, datetime):
        if valor.tzinfo is not None:
            valor = valor.astimezone(timezone.utc).replace(tzinfo=None)
        return valor.isoformat()
    return str(valor)


def _schema_bronze(tipo: str) -> StructType:
    """Contrato do documento bruto: só as chaves que a pipeline usa."""
    bloco = StructType(
        [StructField("time", ArrayType(StringType()))]
        + [StructField(v, ArrayType(DoubleType())) for v in Extract.VARIAVEIS[tipo]]
    )
    return StructType([
        StructField("cidade", StringType()),
        StructField("data_extracao", StringType()),
        StructField(tipo, bloco),
    ])


def _caminho_curto(caminho: str) -> str:
    """No Windows, devolve o caminho 8.3 (só ASCII, ex: C:\\Users\\CECLIA~1\\...).

    O spark-submit do Windows não encontra as próprias classes quando o caminho
    tem acento ou espaço (ex: 'C:\\Users\\Cecília'). Em outros sistemas, não muda nada.
    """
    if os.name != "nt":
        return caminho
    import ctypes
    buffer = ctypes.create_unicode_buffer(1024)
    ok = ctypes.windll.kernel32.GetShortPathNameW(caminho, buffer, len(buffer))
    return buffer.value if ok else caminho


def _rampa(coluna: Column, inicio: float, fim: float) -> Column:
    """Risco de 0 a 100, linear entre `inicio` e `fim`. Dado ausente continua nulo."""
    proporcao = (coluna - F.lit(inicio)) / F.lit(fim - inicio)
    limitado = F.least(F.greatest(proporcao, F.lit(0.0)), F.lit(1.0))
    return F.when(coluna.isNull(), F.lit(None)).otherwise(F.round(limitado * 100, 1))


class Transform:

    def __init__(self):
        """Abre uma sessão Spark local."""
        import pyspark

        # Spark e Python em caminhos sem acento (ver _caminho_curto)
        os.environ.setdefault("SPARK_HOME", _caminho_curto(os.path.dirname(pyspark.__file__)))
        # garante que o Spark use o mesmo Python do venv
        os.environ.setdefault("PYSPARK_PYTHON", _caminho_curto(sys.executable))
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _caminho_curto(sys.executable))

        self.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("ETL OpenMeteo - Spark")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def close(self) -> None:
        self.spark.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------- prata ----------

    def silver(self, docs: list[dict], tipo: str) -> DataFrame:
        """Converte os documentos brutos ('daily' ou 'hourly') em linhas tabulares.

        Parameters
        ----------
        docs : list[dict]
            Carga mais recente de cada cidade, como veio do Mongo.
        tipo : str
            'daily' ou 'hourly'.

        Returns
        -------
        DataFrame
            Uma linha por cidade e dia (daily) ou por cidade e hora (hourly).
        """
        variaveis = Extract.VARIAVEIS[tipo]
        campos = ["time", *variaveis]

        bruto = self.spark.createDataFrame(
            [(json.dumps(d, default=_json_default),) for d in docs], "raw STRING"
        )
        lido = bruto.select(F.from_json("raw", _schema_bronze(tipo)).alias("j"))

        arrays = lido.select(
            F.lower(F.trim(F.col("j.cidade"))).alias("city"),
            F.to_timestamp_ntz(F.col("j.data_extracao")).alias("data_extracao"),
            *[F.col(f"j.{tipo}.{c}").alias(c) for c in campos],
        )

        # listas paralelas -> uma linha por posição (equivale ao pd.DataFrame(dict))
        linhas = arrays.select(
            "city", "data_extracao", F.explode(F.arrays_zip(*campos)).alias("z")
        ).select("city", "data_extracao", *[F.col(f"z.{c}").alias(c) for c in campos])

        if tipo == "daily":
            chave = "date"
            linhas = linhas.withColumn(chave, F.to_date("time"))
        else:
            chave = "time"
            linhas = linhas.withColumn(
                chave, F.to_timestamp_ntz("time", F.lit("yyyy-MM-dd'T'HH:mm"))
            )

        # a chave primária precisa ser única dentro do lote (exigência do upsert)
        return (
            linhas.select("city", chave, *variaveis, "data_extracao")
            .dropDuplicates(["city", chave])
        )

    # ---------- ouro ----------

    def gold_risk(self, daily: DataFrame) -> DataFrame:
        """Calcula os índices de risco urbano por cidade e dia a partir da prata diária."""
        janela = Window.partitionBy("city").orderBy("date")

        df = (
            daily
            .withColumn("chuva_72h", F.round(F.sum("precipitation_sum").over(janela.rowsBetween(-2, 0)), 1))
            .withColumn("chuva_7d", F.round(F.sum("precipitation_sum").over(janela.rowsBetween(-6, 0)), 1))
            # dia de hoje em diante (no fuso local) = previsão; antes disso = dias passados
            .withColumn(
                "is_forecast",
                F.col("date") >= F.to_date(
                    F.from_utc_timestamp(F.col("data_extracao").cast("timestamp_ltz"), FUSO)
                ),
            )
        )

        for nome, (variavel, inicio, fim) in LIMIARES.items():
            df = df.withColumn(f"risco_{nome}", _rampa(F.col(variavel), inicio, fim))

        riscos = [F.col(f"risco_{n}") for n in LIMIARES]
        df = df.withColumn("risco_geral", F.greatest(*riscos))

        geral = F.col("risco_geral")
        principal = F.coalesce(*[F.when(F.col(f"risco_{n}") == geral, F.lit(n)) for n in LIMIARES])
        df = df.withColumn(
            "risco_principal",
            F.when(geral.isNull(), F.lit(None)).when(geral == 0, F.lit("nenhum")).otherwise(principal),
        ).withColumn(
            "nivel_risco",
            F.when(geral.isNull(), F.lit(None))
            .when(geral < 30, F.lit("baixo"))
            .when(geral < 60, F.lit("moderado"))
            .otherwise(F.lit("alto")),
        )

        return df.select(*COLUNAS_OURO)

    # ---------- utilidades ----------

    @staticmethod
    def contar_por_cidade(df: DataFrame) -> dict[str, int]:
        return {r["city"]: r["count"] for r in df.groupBy("city").count().collect()}

    @staticmethod
    def to_records(df: DataFrame) -> list[dict]:
        """Traz o resultado para o Python como lista de dicts (pronto para o upsert)."""
        return [r.asDict() for r in df.collect()]

import pandas as pd
from sqlalchemy import create_engine, inspect

from src import config
from src.extract import Extract

CONFIG = [
    ("WEATHER_DAILY", "daily", "forecast_7days_daily"),
    ("WEATHER_HOURLY", "hourly", "forecast_7days_hourly"),
]

CABECALHO = ("Verificação", "Bronze", "Final", "Status")
NA = "não aplicável"


def imprimir_tabela(linhas: list[tuple]) -> None:
    """Imprime as linhas como uma tabela alinhada no terminal."""
    todas = [CABECALHO] + [tuple(str(c) for c in l) for l in linhas]
    larguras = [max(len(l[i]) for l in todas) for i in range(len(CABECALHO))]
    sep = "+" + "+".join("-" * (w + 2) for w in larguras) + "+"

    def fmt(l):
        return "| " + " | ".join(c.ljust(w) for c, w in zip(l, larguras)) + " |"

    print(sep)
    print(fmt(todas[0]))
    print(sep)
    for l in todas[1:]:
        print(fmt(l))
    print(sep)


def status(ok: bool) -> str:
    return "OK" if ok else "ATENÇÃO"


# ---------- conexões ----------
extractor = Extract()
engine = create_engine(config.DATABASE_URL)

# ---------- diagnóstico: está olhando o banco certo? ----------
tabelas_existentes = inspect(engine).get_table_names()
print("Banco:", engine.url.database, "| host:", engine.url.host)
print("Tabelas encontradas:", tabelas_existentes)

faltando = [t for _, _, t in CONFIG if t not in tabelas_existentes]
if faltando:
    print(f"\nTabela(s) não encontrada(s) neste banco: {faltando}")
    extractor.client.close()
    engine.dispose()
    raise SystemExit(1)

# ---------- verificações ----------
problemas = 0

for colecao, chave, tabela in CONFIG:
    coll = extractor.client["OPEN_METEO"][colecao]
    df = pd.read_sql(f"SELECT * FROM {tabela}", engine)

    if "data_extracao" not in df.columns:
        print(f"\n{tabela}: sem a coluna data_extracao. Ajuste o Transform e rode o pipeline de novo.")
        problemas += 1
        continue
    df["data_extracao"] = pd.to_datetime(df["data_extracao"])

    # carga mais recente de cada cidade (é a que o Transform lê)
    docs = {
        c: coll.find_one({"cidade": c}, sort=[("data_extracao", -1)])
        for c in Extract.CIDADES
    }
    ausentes = [c for c, d in docs.items() if d is None]
    if ausentes:
        print(f"\n{colecao}: sem carga com 'cidade' para {ausentes}. Rode o pipeline antes.")
        problemas += 1
        continue

    linhas = []

    # 1) registros por cidade
    for cidade, doc in docs.items():
        n_bronze = len(doc[chave]["time"])
        n_final = int((df["city"] == cidade).sum())
        linhas.append((f"Registros - {cidade}", n_bronze, n_final, status(n_bronze == n_final)))

    # 2) colunas: final = bronze + city + data_extracao
    cols_bronze = len(next(iter(docs.values()))[chave])
    cols_final = len(df.columns)
    linhas.append((
        "Colunas (final = bronze + 2)",
        cols_bronze, cols_final, status(cols_final == cols_bronze + 2),
    ))

    # 3) nulos nas colunas de dado (as colunas novas ficam de fora)
    cols_dado = [c for c in df.columns if c not in ("city", "data_extracao")]
    nulos_bronze = sum(
        v is None for doc in docs.values() for lista in doc[chave].values() for v in lista
    )
    nulos_final = int(df[cols_dado].isna().sum().sum())
    linhas.append(("Nulos (total)", nulos_bronze, nulos_final, status(nulos_bronze == nulos_final)))

    # 4) zeros por coluna numérica
    for col in df.select_dtypes("number").columns:
        zeros_bronze = sum(
            v == 0 for doc in docs.values() for v in doc[chave][col] if v is not None
        )
        zeros_final = int((df[col] == 0).sum())
        linhas.append((f"Zeros - {col}", zeros_bronze, zeros_final, status(zeros_bronze == zeros_final)))

    # 5) carga lida: data_extracao mais recente no bronze = data_extracao da final
    for cidade, doc in docs.items():
        bronze_dt = pd.Timestamp(doc["data_extracao"])
        datas_final = df.loc[df["city"] == cidade, "data_extracao"].unique()
        final_dt = pd.Timestamp(datas_final[0]) if len(datas_final) == 1 else None
        linhas.append((
            f"Carga lida - {cidade}",
            bronze_dt.strftime("%d/%m %H:%M:%S"),
            final_dt.strftime("%d/%m %H:%M:%S") if final_dt is not None else f"{len(datas_final)} datas",
            status(final_dt is not None and bronze_dt == final_dt),
        ))

    # 6) cargas acumuladas (só existe no bronze)
    for cidade in Extract.CIDADES:
        n_cargas = coll.count_documents({"cidade": cidade})
        linhas.append((f"Cargas acumuladas - {cidade}", n_cargas, NA, status(n_cargas >= 1)))

    # 7) data_extracao ausente
    sem_data = coll.count_documents({"data_extracao": {"$exists": False}})
    nulos_data_final = int(df["data_extracao"].isna().sum())
    linhas.append((
        "Sem data_extracao", sem_data, nulos_data_final,
        status(sem_data == 0 and nulos_data_final == 0),
    ))

    problemas += sum(1 for l in linhas if l[3] != "OK")

    print(f"\n{colecao} -> {tabela}")
    imprimir_tabela(linhas)
    print("Observações:")
    print("* Datas em UTC, como são gravadas pelo pipeline.")
    print("* Cargas acumuladas: não aplicável na final, que guarda só a carga mais recente")
    print("  de cada cidade (replace). O histórico de cargas fica apenas no bronze (Mongo).")
    print("* Nulos: as colunas city e data_extracao ficam de fora, pois são criadas pelo pipeline.")

print(f"\nResumo: {'tudo OK' if problemas == 0 else f'{problemas} verificação(ões) com ATENÇÃO'}")

extractor.client.close()
engine.dispose()
import pandas as pd
from sqlalchemy import create_engine, inspect, text

from src import config
from src.extract import Extract
from src.load import Loading

# (coleção no Mongo, chave do bloco no JSON, tabela prata no Neon, coluna de tempo)
CONFIG = [
    ("WEATHER_DAILY", "daily", "forecast_daily", "date"),
    ("WEATHER_HOURLY", "hourly", "forecast_hourly", "time"),
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

faltando = [t for _, _, t, _ in CONFIG if t not in tabelas_existentes]
if faltando:
    print(f"\nTabela(s) não encontrada(s) neste banco: {faltando}. Rode a pipeline antes.")
    extractor.close()
    engine.dispose()
    raise SystemExit(1)

# ---------- verificações ----------
problemas = 0

for colecao, chave, tabela, col_tempo in CONFIG:
    coll = extractor.client["OPEN_METEO"][colecao]
    df = pd.read_sql(f"SELECT * FROM {tabela}", engine)
    df["data_extracao"] = pd.to_datetime(df["data_extracao"])

    # carga mais recente de cada cidade (é a que o Spark lê)
    docs = {
        c: coll.find_one({"cidade": c}, sort=[("data_extracao", -1)])
        for c in Extract.CIDADES
    }
    ausentes = [c for c, d in docs.items() if d is None]
    if ausentes:
        print(f"\n{colecao}: sem carga com 'cidade' para {ausentes}. Rode o pipeline antes.")
        problemas += 1
        continue

    # linhas da final que vieram da carga mais recente de cada cidade
    recentes = pd.concat([
        df[(df["city"] == c) & (df["data_extracao"] == pd.Timestamp(doc["data_extracao"]))]
        for c, doc in docs.items()
    ])

    linhas = []

    # 1) registros da carga mais recente, por cidade
    for cidade, doc in docs.items():
        n_bronze = len(doc[chave]["time"])
        n_final = int((recentes["city"] == cidade).sum())
        linhas.append((f"Registros (última carga) - {cidade}", n_bronze, n_final, status(n_bronze == n_final)))

    # 2) colunas: final = bronze + city + data_extracao ('time' vira '{col_tempo}')
    cols_bronze = len(next(iter(docs.values()))[chave])
    cols_final = len(df.columns)
    linhas.append((
        "Colunas (final = bronze + 2)",
        cols_bronze, cols_final, status(cols_final == cols_bronze + 2),
    ))

    # 3) nulos nas colunas de dado (as colunas criadas pela pipeline ficam de fora)
    cols_dado = [c for c in df.columns if c not in ("city", "data_extracao", col_tempo)]
    nulos_bronze = sum(
        v is None for doc in docs.values()
        for k, lista in doc[chave].items() if k != "time" for v in lista
    )
    nulos_final = int(recentes[cols_dado].isna().sum().sum())
    linhas.append(("Nulos (última carga)", nulos_bronze, nulos_final, status(nulos_bronze == nulos_final)))

    # 4) zeros por coluna numérica
    for col in cols_dado:
        zeros_bronze = sum(
            v == 0 for doc in docs.values() for v in doc[chave][col] if v is not None
        )
        zeros_final = int((recentes[col] == 0).sum())
        linhas.append((f"Zeros - {col}", zeros_bronze, zeros_final, status(zeros_bronze == zeros_final)))

    # 5) idempotência: nenhuma chave primária repetida na final
    pk = ", ".join(Loading.CHAVES_PRIMARIAS[tabela])
    with engine.connect() as conn:
        dup_final = conn.execute(text(
            f"SELECT COUNT(*) FROM (SELECT {pk} FROM {tabela} GROUP BY {pk} HAVING COUNT(*) > 1) t"
        )).scalar()
    dup_bronze = sum(1 for _ in coll.aggregate([
        {"$match": {"cidade": {"$exists": True}, "data_extracao": {"$exists": True}}},
        {"$group": {"_id": {"c": "$cidade", "d": "$data_extracao"}, "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}},
    ]))
    linhas.append(("Chaves duplicadas", dup_bronze, dup_final, status(dup_bronze == 0 and dup_final == 0)))

    # 6) histórico: cargas no bronze x períodos acumulados na final
    for cidade in Extract.CIDADES:
        n_cargas = coll.count_documents({"cidade": cidade})
        n_periodos = int((df["city"] == cidade).sum())
        linhas.append((
            f"Cargas no bronze / {col_tempo}s na final - {cidade}",
            n_cargas, n_periodos, status(n_cargas >= 1 and n_periodos >= 1),
        ))

    # 7) data_extracao ausente
    sem_data = coll.count_documents({"data_extracao": {"$exists": False}})
    nulos_data_final = int(df["data_extracao"].isna().sum())
    linhas.append((
        "Sem data_extracao", sem_data, nulos_data_final,
        status(nulos_data_final == 0),
    ))

    problemas += sum(1 for l in linhas if l[3] != "OK")

    print(f"\n{colecao} -> {tabela}")
    imprimir_tabela(linhas)
    print("Observações:")
    print("* Datas em UTC, como são gravadas pela pipeline.")
    print("* A final acumula histórico (upsert por chave primária); as comparações com o")
    print("  bronze usam só as linhas que vieram da carga mais recente de cada cidade.")
    print("* 'Sem data_extracao' no bronze conta documentos antigos, anteriores a essa coluna.")

print(f"\nResumo: {'tudo OK' if problemas == 0 else f'{problemas} verificação(ões) com ATENÇÃO'}")

extractor.close()
engine.dispose()

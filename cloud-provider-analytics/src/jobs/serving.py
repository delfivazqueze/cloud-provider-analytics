"""Gold -> Cassandra/AstraDB con el driver de Python (datastax cassandra-driver).

La carga se hace desde el driver de Spark con sentencias preparadas y escritura concurrente.
En Cassandra un INSERT con la misma PRIMARY KEY sobrescribe la fila (upsert), así que recargar no duplica.
"""
import os

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def connect(scb_path, token, keyspace):
    cluster = Cluster(cloud={"secure_connect_bundle": scb_path},
                      auth_provider=PlainTextAuthProvider("token", token))
    session = cluster.connect(keyspace)
    session.default_timeout = 60
    return cluster, session


def run_cql_file(session, path):
    """Ejecuta un archivo .cql sentencia por sentencia (separadas por ';'), ignorando comentarios."""
    with open(path) as f:
        lines = [l for l in f.read().splitlines() if not l.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    for s in statements:
        session.execute(s)
    return len(statements)


def load_dataframe(session, df, table, columns, concurrency=50, chunk=2000):
    """Upsert de un DataFrame de Spark en una tabla Cassandra. Devuelve filas enviadas y errores."""
    cql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})"
    stmt = session.prepare(cql)
    sent, errors, buf = 0, 0, []

    def flush(rows):
        res = execute_concurrent_with_args(session, stmt, rows, concurrency=concurrency, raise_on_first_error=False)
        return sum(1 for ok, _ in res if not ok)

    for row in df.select(*columns).toLocalIterator():
        buf.append(tuple(row))
        if len(buf) == chunk:
            errors += flush(buf)
            sent += len(buf)
            buf = []
    if buf:
        errors += flush(buf)
        sent += len(buf)
    return sent, errors


def count_rows(session, table, org_ids):
    """Cuenta filas partición por partición (org_id). Evita un COUNT(*) sobre toda la tabla,
    que en Cassandra recorre todos los nodos y puede dar timeout."""
    stmt = session.prepare(f"SELECT COUNT(*) FROM {table} WHERE org_id = ?")
    return sum(session.execute(stmt, (o,)).one()[0] for o in org_ids)


def credentials_from_env(cfg):
    """Busca el token y el bundle en variables de entorno (en Colab se cargan desde Secrets)."""
    c = cfg["cassandra"]
    return os.environ.get(c["secure_bundle_env"]), os.environ.get(c["token_env"])

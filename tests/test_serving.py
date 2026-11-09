"""El script CQL se parte bien en sentencias y define las claves query-first esperadas."""
import os

from src.jobs.serving import run_cql_file

CQL = os.path.join(os.path.dirname(__file__), "..", "cql", "01_schema.cql")


class FakeSession:
    def __init__(self):
        self.statements = []

    def execute(self, stmt):
        self.statements.append(stmt)


def test_schema_cql():
    s = FakeSession()
    assert run_cql_file(s, CQL) == 2
    org, top = s.statements
    assert "PRIMARY KEY ((org_id), usage_date, service)" in org
    assert "CLUSTERING ORDER BY (usage_date DESC, service ASC)" in org
    assert "PRIMARY KEY ((org_id, as_of_date), rank)" in top
    assert not any("--" in x for x in s.statements)        # los comentarios no llegan a Cassandra

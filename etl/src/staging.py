"""Staging I/O. write() and read() are symmetric: what a stage writes, the next stage reads back
as the same list-of-dicts the old in-memory code passed around (Decimals, dates, row order kept)."""
import staging_schema as S


def _q(names):
    return ",".join(f"`{n}`" for n in names)


def write(conn, schema, table, rows, batch_id, size=2000) -> int:
    if not rows:
        return 0
    cols = S.cols(schema, table)
    sql = (f"INSERT INTO `{schema}`.`{table}` (batch_id,{_q(cols)}) "
           f"VALUES (%s,{','.join(['%s'] * len(cols))})")
    with conn.cursor() as c:
        for i in range(0, len(rows), size):
            c.executemany(sql, [(batch_id, *[r[k] for k in cols]) for r in rows[i:i + size]])
    return len(rows)


def read(conn, schema, table, batch_id) -> list:
    with conn.cursor() as c:
        c.execute(f"SELECT {_q(S.cols(schema, table))} FROM `{schema}`.`{table}` "
                  f"WHERE batch_id=%s ORDER BY row_id", (batch_id,))
        return list(c.fetchall())


def read_many(conn, schema, tables, batch_id) -> dict:
    return {t: read(conn, schema, t, batch_id) for t in tables}


def clear(conn, schema, tables, batch_id) -> None:
    with conn.cursor() as c:
        for t in tables:
            c.execute(f"DELETE FROM `{schema}`.`{t}` WHERE batch_id=%s", (batch_id,))


def count(conn, schema, table, batch_id) -> int:
    with conn.cursor() as c:
        c.execute(f"SELECT COUNT(*) AS c FROM `{schema}`.`{table}` WHERE batch_id=%s", (batch_id,))
        return int(c.fetchone()["c"])


def clear_rejects(conn, batch_id, stage) -> None:
    with conn.cursor() as c:
        c.execute("DELETE FROM `stg_transform`.`rejected_rows` WHERE batch_id=%s AND stage=%s",
                  (batch_id, stage))


def write_rejects(conn, batch_id, stage, rej) -> int:
    rows = [dict(stage=stage, source_table=r["table"], source_id=r["source_id"],
                 rule_name=r["rule"], reason=r["reason"]) for r in rej]
    return write(conn, "stg_transform", "rejected_rows", rows, batch_id)


def count_rejects(conn, batch_id, source_table, stage_like) -> int:
    with conn.cursor() as c:
        c.execute("SELECT COUNT(*) AS c FROM `stg_transform`.`rejected_rows` "
                  "WHERE batch_id=%s AND source_table=%s AND stage LIKE %s",
                  (batch_id, source_table, stage_like))
        return int(c.fetchone()["c"])


def purge(conn, cutoff_batch_id) -> None:
    with conn.cursor() as c:
        for schema, tables in S.SCHEMAS.items():
            for t in tables:
                c.execute(f"DELETE FROM `{schema}`.`{t}` WHERE batch_id<=%s", (cutoff_batch_id,))
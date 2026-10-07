"""Connections to the databases: OLTP source, DW target and the staging-db."""
import pymysql
import pymysql.cursors

from config import oltp_cfg, dw_cfg, stg_cfg


def oltp_conn():
    # autocommit ON: the extract opens its own consistent-snapshot transaction explicitly.
    return pymysql.connect(**oltp_cfg(), cursorclass=pymysql.cursors.DictCursor, autocommit=True)


def dw_read_conn():
    # separate read-only-use connection (find still-open records, resolve surrogate keys)
    return pymysql.connect(**dw_cfg(), cursorclass=pymysql.cursors.DictCursor, autocommit=True)


def dw_conn():
    # autocommit OFF: each load task is ONE transaction (commit at the end, rollback on error)
    return pymysql.connect(**dw_cfg(), cursorclass=pymysql.cursors.DictCursor, autocommit=False)


def stg_conn():
    # staging layers (stg_extract / stg_transform / stg_load); each task clears its batch first,
    # so autocommit is fine and a retry never duplicates rows.
    return pymysql.connect(**stg_cfg(), cursorclass=pymysql.cursors.DictCursor, autocommit=True)
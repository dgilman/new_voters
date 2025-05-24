import os
from typing import Annotated
import datetime
import csv
import io
import zoneinfo
import enum

import duckdb
from fastapi import FastAPI, Depends, Query
from fastapi.responses import FileResponse, StreamingResponse
import pydantic

app = FastAPI()
EASTERN = zoneinfo.ZoneInfo("America/New_York")


def filename_ts():
    return datetime.datetime.now(tz=EASTERN).strftime("%Y%m%d%H%M%S")


def get_db(db_dir, table_name):
    with duckdb.connect() as conn:
        conn.sql(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{db_dir}')")
        yield conn


def get_delta_db():
    yield from get_db(os.environ["WORK_DIR"] + "/deltas/*/*/*/*.parquet", "deltas")


def get_whole_db():
    yield from get_db(os.environ["WORK_DIR"] + "/whole/*/*/*.parquet", "whole")


def _make_column_names():
    db_dep = get_whole_db()

    whole_db = next(db_dep)

    whole_db.execute("DESCRIBE SELECT * FROM whole")

    addl_delta_cols = ["delta_date", "county_id", "precinct_abbrv", "street_name"]
    addl_whole_cols = ["county_id", "precinct_abbrv", "street_name"]
    db_colnames = [
        (row[0], row[0]) for row in whole_db.fetchall() if row[0] not in addl_delta_cols
    ]

    # close connection
    next(db_dep, None)

    delta_cols = [(addl_col, addl_col) for addl_col in addl_delta_cols] + db_colnames
    whole_cols = [(addl_col, addl_col) for addl_col in addl_whole_cols] + db_colnames

    delta_col_enum = enum.StrEnum("DeltaColumnNames", delta_cols)
    whole_col_enum = enum.StrEnum("WholeColumnNames", whole_cols)

    return delta_col_enum, whole_col_enum


DeltaColumnNames, WholeColumnNames = _make_column_names()


class DeltaCsvParams(pydantic.BaseModel):
    delta_date: datetime.date
    county_id: int
    precinct_id: str | None = pydantic.Field(default=None)
    columns: list[DeltaColumnNames] | None = pydantic.Field(default=None)


class WholeCsvParams(pydantic.BaseModel):
    county_id: int
    precinct_id: str | None = pydantic.Field(default=None)
    columns: list[WholeColumnNames] | None = pydantic.Field(default=None)


def _csv_route(
    db: duckdb.DuckDBPyConnection, filename: str, cols: list[str], query: str
):
    buffa = io.StringIO()
    writer = csv.writer(buffa)
    writer.writerow(cols)

    db.execute(query)

    writer.writerows(db.fetchall())
    buffa.seek(0)
    return StreamingResponse(
        buffa,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment;filename={filename}",
            "Access-Control-Expose-Headers": "Content-Disposition",
        },
    )


@app.post("/delta_csv")
def delta_csv(
    q: Annotated[DeltaCsvParams, Query()],
    delta_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_delta_db)],
) -> StreamingResponse:
    filename = f"delta_c{q.county_id}"
    if q.precinct_id:
        filename += f"_p{q.precinct_id}"
    filename += f"_d{q.delta_date}_{filename_ts()}.csv"

    if not q.columns:
        cols = [k for k in DeltaColumnNames]
    else:
        cols = q.columns
    cols_sql = ",".join(cols)

    if q.precinct_id:
        precinct_pred = f"AND precinct_abbrv = '{q.precinct_id}'"
    else:
        precinct_pred = ""

    query = f"""
        SELECT {cols_sql}
        FROM deltas
        WHERE
         delta_date = '{q.delta_date}'
         AND county_id = {q.county_id}
         {precinct_pred}
    """

    return _csv_route(delta_db, filename, cols, query)


@app.post("/whole_csv")
def whole_csv(
    q: Annotated[WholeCsvParams, Query()],
    whole_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_whole_db)],
) -> StreamingResponse:
    filename = f"delta_c{q.county_id}"
    if q.precinct_id:
        filename += f"_p{q.precinct_id}"
    filename += f"_{filename_ts()}.csv"

    if not q.columns:
        cols = [k for k in WholeColumnNames]
    else:
        cols = q.columns
    cols_sql = ",".join(cols)

    if q.precinct_id:
        precinct_pred = f"AND precinct_abbrv = '{q.precinct_id}'"
    else:
        precinct_pred = ""

    query = f"""
        SELECT {cols_sql}
        FROM whole
        WHERE
         county_id = {q.county_id}
         {precinct_pred}
    """

    return _csv_route(whole_db, filename, cols, query)


class CountyDesc(pydantic.BaseModel):
    county_id: int
    county_name: str


@app.get("/counties")
def get_counties(
    whole_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_whole_db)],
) -> list[CountyDesc]:
    return [
        CountyDesc(county_id=row[0], county_name=row[1])
        for row in whole_db.execute(
            """
            SELECT county_id, county_desc
            FROM whole
            GROUP BY county_id, county_desc
            ORDER BY county_desc
            """
        ).fetchall()
    ]


class PrecinctDesc(pydantic.BaseModel):
    precinct_id: str | None
    precinct_desc: str | None


@app.get("/counties/{county_id}/precincts")
def get_county_precincts(
    county_id: int,
    whole_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_whole_db)],
) -> list[PrecinctDesc]:
    return [
        PrecinctDesc(precinct_id=row[0], precinct_desc=row[1])
        for row in whole_db.execute(
            """
            SELECT precinct_abbrv, precinct_desc
            FROM whole
            WHERE county_id = ? AND precinct_abbrv IS NOT NULL
            GROUP BY precinct_abbrv, precinct_desc
            ORDER BY precinct_abbrv, precinct_desc
            """,
            [county_id],
        ).fetchall()
    ]


class DeltaDays(pydantic.BaseModel):
    delta_day: datetime.date
    freq: int


@app.get("/counties/{county_id}/delta_days")
def get_county_delta_days(
    county_id: int,
    delta_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_delta_db)],
) -> list[DeltaDays]:
    return [
        DeltaDays(delta_day=row[0], freq=row[1])
        for row in delta_db.execute(
            """
            SELECT delta_date, count(*) as freq
            FROM deltas
            WHERE county_id = ?
            GROUP BY delta_date
            ORDER BY delta_date
            """,
            [county_id],
        ).fetchall()
    ]


@app.get("/counties/{county_id}/precinct/{precinct_id}/delta_days")
def get_county_precinct_delta_days(
    county_id: int,
    precinct_id: str,
    delta_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_delta_db)],
) -> list[DeltaDays]:
    return [
        DeltaDays(delta_day=row[0], freq=row[1])
        for row in delta_db.execute(
            """
            SELECT delta_date, count(*) as freq
            FROM deltas
            WHERE county_id = ? AND precinct_abbrv = ?
            GROUP BY delta_date
            ORDER BY delta_date
            """,
            [county_id, precinct_id],
        ).fetchall()
    ]

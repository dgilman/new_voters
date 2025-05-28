import os
from typing import Annotated, Literal
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

    whole_db.execute("SELECT DISTINCT party_cd FROM whole ORDER BY party_cd")
    party_enum = enum.StrEnum(
        "Parties", [(row[0], row[0]) for row in whole_db.fetchall()]
    )

    # close connection
    next(db_dep, None)

    delta_cols = [(addl_col, addl_col) for addl_col in addl_delta_cols] + db_colnames
    whole_cols = [(addl_col, addl_col) for addl_col in addl_whole_cols] + db_colnames

    delta_col_enum = enum.StrEnum("DeltaColumnNames", delta_cols)
    whole_col_enum = enum.StrEnum("WholeColumnNames", whole_cols)

    return delta_col_enum, whole_col_enum, party_enum


DeltaColumnNames, WholeColumnNames, Parties = _make_column_names()


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


class Generation(enum.Enum):
    ww2 = "ww2"
    postwar = "postwar"
    boomer1 = "boomer1"
    boomer2 = "boomer2"
    genx = "genx"
    millennial = "millennial"
    zoomer = "zoomer"
    gena = "gena"


class Race(enum.Enum):
    A = "A"
    B = "B"
    I = "I"
    M = "M"
    O = "O"
    P = "P"
    U = "U"
    W = "W"


class Ethnicity(enum.Enum):
    HL = "HL"
    NL = "NL"
    UN = "UN"


race_desc = {
    Race.A: "Asian",
    Race.B: "Black",
    Race.I: "American Indian or Alaskan Native",
    Race.M: "Two or more",
    Race.O: "Other",
    Race.P: "Hawaiian or Pacific Islander",
    Race.U: "Undesignated",
    Race.W: "White",
}

ethnicity_desc = {
    Ethnicity.HL: "Hispanic/Latino",
    Ethnicity.NL: "Non-Hispanic/Latino",
    Ethnicity.UN: "Undesignated",
}


class Aggregates(enum.StrEnum):
    generation = "generation"
    party = "party"
    has_phone = "has_phone"
    race = "race"
    ethnicity = "ethnicity"


class StatsParams(pydantic.BaseModel):
    aggs: list[Aggregates] | None = pydantic.Field(None)
    generation: Generation | None = pydantic.Field(None)
    party: Parties | None = pydantic.Field(None)
    has_phone: bool | None = pydantic.Field(None)
    race: Race | None = pydantic.Field(None)
    ethnicity: Ethnicity | None = pydantic.Field(None)


class StatsResponse(pydantic.BaseModel):
    freq: int
    generation: Generation | None = pydantic.Field(None)
    party: Parties | None = pydantic.Field(None)
    has_phone: bool | None = pydantic.Field(None)
    race: Race | None = pydantic.Field(None)
    ethnicity: Ethnicity | None = pydantic.Field(None)


# XXX some of these may be more useful as arrays and not scalars
#     e.g. get stats for multiple precincts at once


@app.get(
    "/counties/{county_id}/precinct/{precinct_id}/stats",
    response_model_exclude_none=True,
)
def get_county_precinct_stats(
    county_id: int,
    precinct_id: str,
    stats_params: Annotated[StatsParams, Query()],
    whole_db: Annotated[duckdb.DuckDBPyConnection, Depends(get_whole_db)],
) -> list[StatsResponse]:
    sql_named_params = {
        "county_id": county_id,
        "precinct_id": precinct_id,
    }

    agg_cols = stats_params.aggs
    if not agg_cols:
        agg_cols = list(Aggregates)
    agg_cols_str = ",".join(col.value for col in agg_cols)

    preds = []

    for attr_name in ("generation", "party", "has_phone", "race", "ethnicity"):
        if attr_val := getattr(stats_params, attr_name):
            preds.append(f"{attr_name} = ${attr_name}")
            sql_named_params[attr_name] = attr_val.value

    preds_str = ""
    if preds:
        preds_str = " AND " + " AND ".join(preds)

    inner_query = """
    SELECT
        county_id,
        precinct_abbrv as precinct_id,
        CASE
            WHEN birth_year <= 1927 THEN 'ww2'
            WHEN birth_year BETWEEN 1928 AND 1945 THEN 'postwar'
            WHEN birth_year BETWEEN 1946 AND 1954 THEN 'boomer1'
            WHEN birth_year BETWEEN 1955 AND 1964 THEN 'boomer2'
            WHEN birth_year BETWEEN 1965 AND 1980 THEN 'genx'
            WHEN birth_year BETWEEN 1981 AND 1996 THEN 'millennial'
            WHEN birth_year BETWEEN 1997 AND 2012 THEN 'zoomer'
            WHEN birth_year >= 2013 THEN 'gena'
        END AS generation,
        party_cd as party,
        full_phone_number is not null as has_phone,
        race_code as race,
        ethnic_code as ethnicity
    FROM whole
    """

    outer_query = f"""
    SELECT count(*) as freq, {agg_cols_str}
    FROM ({inner_query}) inn
    WHERE county_id = $county_id
     AND precinct_id = $precinct_id
     {preds_str}
    GROUP BY {agg_cols_str}
    ORDER BY {agg_cols_str}, freq DESC
    """

    def _row_formatter(row):
        kwargs = {"freq": row[0]}
        for col_idx, colname in enumerate(agg_cols):
            kwargs[colname] = row[col_idx + 1]
        return StatsResponse(**kwargs)

    return [
        _row_formatter(row)
        for row in whole_db.execute(outer_query, sql_named_params).fetchall()
    ]

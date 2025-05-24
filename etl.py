import os
import datetime
import requests
import logging
import subprocess
import duckdb
import glob
import argparse
import itertools

TODAY = datetime.date.today()


WORK_DIR = "statewide_data"
ZIP_DIR = WORK_DIR + f"/zips"
DELTA_DIR = WORK_DIR + f"/deltas"
WHOLE_DIR = WORK_DIR + f"/whole"

STATEWIDE_ZIP = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def available_zips():
    statewide_zips = []
    for dirname in glob.glob(ZIP_DIR + "/*"):
        try:
            dir_date = datetime.datetime.strptime(dirname[len(ZIP_DIR)+1:], '%Y-%m-%d').date()
        except ValueError:
            continue

        existing_hash = None

        try:
            with open(dirname + "/statewide_hash", 'r') as f:
                existing_hash = f.read().strip()
        except FileNotFoundError:
            continue

        if not existing_hash:
            continue

        statewide_zips.append((dir_date, existing_hash))

    statewide_zips.sort(key=lambda x: x[0])

    return statewide_zips


def download_latest_zip(statewide_zips):
    if statewide_zips and statewide_zips[-1][0] == TODAY:
        logger.info("Already downloaded the latest file!")
        return

    logger.info("HEAD latest zip")
    head_resp = requests.head(STATEWIDE_ZIP)
    head_resp.raise_for_status()
    etag = head_resp.headers['ETag']

    if not statewide_zips or statewide_zips[-1][1] != etag:
        # get new file
        logger.info(f"Downloading CSV for {TODAY}")
        output_dir = ZIP_DIR + f"/{TODAY}"
        os.makedirs(output_dir, exist_ok=True)
        subprocess.check_call(["curl", "-O", "-L", STATEWIDE_ZIP], cwd=output_dir)

        with open(ZIP_DIR + f"/{TODAY}/statewide_hash", 'w') as f:
            f.write(etag)

        statewide_zips.append((TODAY, etag))

    for date, _ in statewide_zips:
        if not os.path.exists(ZIP_DIR + f"/{date}/ncvoter_Statewide.txt.zst"):
            logger.info(f"Iconv for {date}")
            unzip_proc = subprocess.Popen(["unzip", "-p", ZIP_DIR + f"/{date}/ncvoter_Statewide.zip", "ncvoter_Statewide.txt"], stdout=subprocess.PIPE)
            iconv_proc = subprocess.Popen(["iconv", "-f", "WINDOWS-1252", "-t", "utf-8"], stdin=unzip_proc.stdout, stdout=subprocess.PIPE)
            unzip_proc.stdout.close()
            zstd_proc = subprocess.Popen(["zstd", "-z", "-o", ZIP_DIR + f"/{date}/ncvoter_Statewide.txt.zst"], stdin=iconv_proc.stdout)
            iconv_proc.stdout.close()

            zstd_proc.communicate()


# unzip -p ncvoter_Statewide.zip ncvoter_Statewide.txt | iconv -f WINDOWS-1252 -t utf-8 | zstd -z -o ncvoter_Statewide.txt.zst

def load_csv(con, table_name, date):
    csv_path = ZIP_DIR + f"/{date}/ncvoter_Statewide.txt.zst"
    con.execute(f"CREATE TABLE {table_name} AS FROM read_csv('{csv_path}', delim = '\t', encoding='utf-8', header = true, sample_size=-1) WHERE status_cd = 'A'")


def create_deltas(new_date: datetime.date, old_date: datetime.date):
    os.makedirs(DELTA_DIR, exist_ok=True)

    if os.path.exists(DELTA_DIR + f"/delta_date={new_date}"):
        logger.info(f"Skipping delta date {new_date}")
        return

    con = duckdb.connect()

    logger.info(f"Reading CSV for {old_date}")
    load_csv(con, 'old_t', old_date)
    logger.info(f"Reading CSV for {new_date}")
    load_csv(con, 'new_t', new_date)

    con.sql(f"""
    create view delta_table as
    select
        new_t.*,
        cast('{new_date}' as date) as delta_date,
        (old_t.ncid is null) as new_registration,
        (coalesce(old_t.county_id != new_t.county_id, false)) as moved_counties,
        (coalesce(old_t.county_id = new_t.county_id, false) and coalesce(old_t.precinct_abbrv, '') != new_t.precinct_abbrv) as moved_precints,
        regexp_extract(new_t.res_street_address, '[^ ]+  ?(.+)  ', 1) as street_name,
    from new_t
    left join old_t
    using (ncid)
    where
        ((old_t.ncid is null) or (old_t.county_id != new_t.county_id) or (old_t.precinct_abbrv != new_t.precinct_abbrv))
    """)

    #stats = con.sql("""
    #SELECT
    #    county_id, new_registration, moved_counties, moved_precints,
    #    count(*) as freq
    #    FROM delta_table
    #    group by 1, 2, 3, 4
    #    order by 1, 2, 3
    #""")
    #print(stats.show(max_rows=10000))

    logger.info(f"Writing deltas {new_date}")
    con.sql(f"""
    COPY (select * from delta_table order by delta_date, county_id, precinct_abbrv, street_name)
    TO '{DELTA_DIR}' (FORMAT parquet, PARTITION_BY (delta_date, county_id, precinct_abbrv), OVERWRITE_OR_IGNORE)
    """)

    con.close()

def create_whole(new_date: datetime.date):
    os.makedirs(WHOLE_DIR, exist_ok=True)

    old_whole_date = None
    old_whole_date_file = WHOLE_DIR + "/whole_date"
    try:
        with open(old_whole_date_file, 'r') as f:
            old_whole_date = f.read().strip()
    except FileNotFoundError:
        pass

    if str(new_date) == old_whole_date:
        logger.info(f"Skipping whole file for {new_date}")
        return

    with open(old_whole_date_file, 'w') as f:
        f.write(str(new_date))

    con = duckdb.connect()

    load_csv(con, 'new_t', new_date)

    logger.info(f"Reading whole CSV for {new_date}")
    con.sql(f"""
    create view whole_table as
    select
        new_t.*,
        regexp_extract(new_t.res_street_address, '[^ ]+  ?(.+)  ', 1) as street_name,
    from new_t
    """)

    logger.info(f"Writing whole table {new_date}")
    con.sql(f"""
    COPY (select * from new_t order by county_id, precinct_abbrv, street_name)
    TO '{WHOLE_DIR}' (FORMAT parquet, PARTITION_BY (county_id, precinct_abbrv), OVERWRITE_OR_IGNORE)
    """)


def main():
    # check to see what files we have and get their hashes
    statewide_zips = available_zips()

    # download a new one for today if its hash does not match
    download_latest_zip(statewide_zips)

    # create deltas for each date pair
    for pairs in itertools.pairwise(reversed(statewide_zips)):
        new_date = pairs[0][0]
        old_date = pairs[1][0]

        create_deltas(new_date, old_date)

    # write out a copy of the whole file
    create_whole(statewide_zips[-1][0])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="nc voter etl",
        description="Creates the parquet hierarchy out of voter CSVs",
    )
    main()

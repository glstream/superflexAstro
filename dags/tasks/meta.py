from datetime import datetime, timedelta
import geocoder
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from psycopg2.extras import execute_batch


@task
def get_user_meta():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    cursor.execute(
        """select * 
            from dynastr.user_meta du 
            where to_timestamp(du.insert_date, 'YYYY-MM-DDTHH:MI:SS.FF6') 
                > (select max(to_timestamp(hu.insert_date, 'YYYY-MM-DDTHH:MI:SS.FF6')) 
                        from history.user_geo_meta hu)"""
    )

    user_meta_list = cursor.fetchall()

    return user_meta_list


@task
def add_geo_meta(user_meta_list: list):
    raw_geos = [
        [row[0], row[1], row[2], row[3], row[4], row[5], geocoder.ip(row[1]).json]
        for row in user_meta_list
    ]
    return raw_geos


@task
def geo_transforms(raw_geos: list):

    preped_geos = [
        [
            i[0],
            i[1],
            i[6].get("address", None),
            i[6].get("city", None),
            i[6].get("country", None),
            i[6].get("hostname", None),
            i[6].get("lat", None),
            i[6].get("lng", None),
            i[6].get("org", None),
            i[6].get("postal", None),
            i[2],
            i[3],
            i[4],
            i[5],
        ]
        for i in raw_geos if i is not None
    ]
    return preped_geos


@task
def history_meta_load(preped_geos: list):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    execute_batch(
        cursor,
        """
            INSERT INTO history.user_meta (
                session_id,
                ip_address,
                address,
                city,
                country,
                hostname,
                lat,
                lng,
                org,
                postal,
                agent,
                host,
                referrer,
                insert_date
        )        
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (session_id)
        DO UPDATE SET ip_address = EXCLUDED.ip_address
            , address = EXCLUDED.address
            , city = EXCLUDED.city
            , country = EXCLUDED.country
            , hostname = EXCLUDED.hostname
            , lat = EXCLUDED.lat
            , lng = EXCLUDED.lng
            , org = EXCLUDED.org
            , postal = EXCLUDED.postal
            , agent = EXCLUDED.agent
            , host = EXCLUDED.host
            , referrer = EXCLUDED.referrer
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(preped_geos),
        page_size=1000,
    )
    return

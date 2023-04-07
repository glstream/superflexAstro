from airflow.decorators import task
import json, requests, re
from bs4 import BeautifulSoup
from selenium import webdriver
from psycopg2.extras import execute_batch
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task()
def ktc_one_qb_rookies():
    rookie_one_qb_picks = []
    for i in range(1,5):
        round = str(i)
        url = f"https://keeptradecut.com/trade-calculator?var=5&pickVal=0&teamOne=2023{round}1|2023{round}2|2023{round}3|2023{round}4|2023{round}5|2023{round}6|2023{round}7|2023{round}8|2023{round}9|2023{round}10|2023{round}11|2023{round}12&teamTwo=&format=1&isStartup=0"
        print(url)
        headOption = webdriver.FirefoxOptions()
        headOption.add_argument("--headless")

        browser = webdriver.Firefox(options=headOption)
        browser.get(url)
        html = browser.page_source
        soup = BeautifulSoup(html, features="html.parser")
        td = soup.findAll('div', 'team-player-wrapper')
        for k in td:
            draft_position = k.find("p", "player-name").string
            draft_pos_value = k.find("p", "player-value").string
            ktc_player_id = draft_position.split(' ')[-1].replace('.', '') + '000'
            enrty_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            rookie_one_qb_picks.append([draft_position, ktc_player_id, draft_pos_value, enrty_time])

        browser.quit()



    return rookie_one_qb_picks

@task()
def one_qb_data_validation(rookie_one_qb_picks: list):
    return rookie_one_qb_picks if len(rookie_one_qb_picks) > 0 else False

@task()
def ktc_one_qb_rookies_load(rookie_one_qb_picks:list):

    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    execute_batch(
        cursor,
        """
            INSERT INTO dynastr.ktc_player_ranks (
                player_full_name,
                ktc_player_id,
                one_qb_value,
                insert_date
        )        
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (ktc_player_id)
        DO UPDATE SET 
            , one_qb_value = EXCLUDED.one_qb_value
            , insert_date = EXCLUDED.insert_date;
        """,
        tuple(rookie_one_qb_picks),
        page_size=1000,
    )

    print(f"{len(rookie_one_qb_picks)} ktc players to inserted or updated.")
    conn.commit()
    cursor.close()
    return "dynastr.ktc_player_ranks"
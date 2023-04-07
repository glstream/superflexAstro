from airflow.decorators import task
from psycopg2.extras import execute_batch
from datetime import datetime
from scipy.stats import hmean
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_player_data(row, cursor):
    insert_query = '''
    INSERT INTO dynastr.sf_player_ranks (
        player_full_name, ktc_player_id, team, _position, ktc_sf_value,
        ktc_sf_rank, ktc_one_qb_value, ktc_one_qb_rank, fc_sf_value,
        fc_sf_rank, fc_one_qb_value, fc_one_qb_rank, dp_sf_value,
        dp_sf_rank, dp_one_qb_value, dp_one_qb_rank,
        ktc_sf_normalized_value, fc_sf_normalized_value,
        dp_sf_normalized_value, ktc_one_qb_normalized_value,
        fc_one_qb_normalized_value, dp_one_qb_normalized_value,
        average_normalized_sf_value, average_normalized_one_qb_value,
        superflex_sf_value, superflex_one_qb_value, superflex_sf_rank,
        superflex_one_qb_rank, superflex_sf_pos_rank, superflex_one_qb_pos_rank, insert_date
    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ktc_player_id) DO UPDATE SET
    player_full_name = EXCLUDED.player_full_name,
    team = EXCLUDED.team,
    _position = EXCLUDED._position,
    ktc_sf_value = EXCLUDED.ktc_sf_value,
    ktc_sf_rank = EXCLUDED.ktc_sf_rank,
    ktc_one_qb_value = EXCLUDED.ktc_one_qb_value,
    ktc_one_qb_rank = EXCLUDED.ktc_one_qb_rank,
    fc_sf_value = EXCLUDED.fc_sf_value,
    fc_sf_rank = EXCLUDED.fc_sf_rank,
    fc_one_qb_value = EXCLUDED.fc_one_qb_value,
    fc_one_qb_rank = EXCLUDED.fc_one_qb_rank,
    dp_sf_value = EXCLUDED.dp_sf_value,
    dp_sf_rank = EXCLUDED.dp_sf_rank,
    dp_one_qb_value = EXCLUDED.dp_one_qb_value,
    dp_one_qb_rank = EXCLUDED.dp_one_qb_rank,
    ktc_sf_normalized_value = EXCLUDED.ktc_sf_normalized_value,
    fc_sf_normalized_value = EXCLUDED.fc_sf_normalized_value,
    dp_sf_normalized_value = EXCLUDED.dp_sf_normalized_value,
    ktc_one_qb_normalized_value = EXCLUDED.ktc_one_qb_normalized_value,
    fc_one_qb_normalized_value = EXCLUDED.fc_one_qb_normalized_value,
    dp_one_qb_normalized_value = EXCLUDED.dp_one_qb_normalized_value,
    average_normalized_sf_value = EXCLUDED.average_normalized_sf_value,
    average_normalized_one_qb_value = EXCLUDED.average_normalized_one_qb_value,
    superflex_sf_value = EXCLUDED.superflex_sf_value,
    superflex_one_qb_value = EXCLUDED.superflex_one_qb_value,
    superflex_sf_rank = EXCLUDED.superflex_sf_rank,
    superflex_one_qb_rank = EXCLUDED.superflex_one_qb_rank,
    superflex_sf_pos_rank = EXCLUDED.superflex_sf_pos_rank,
    superflex_one_qb_pos_rank = EXCLUDED.superflex_one_qb_pos_rank,
    insert_date = EXCLUDED.insert_date;
    '''

    cursor.execute(insert_query, tuple(row))

@task()
def fetch_run_data():
    query = """with ktc_picks as (select *
from dynastr.ktc_player_ranks ktc 
where 1=1
and (ktc.player_full_name like '%2023 Round%' or ktc.position = 'RDP') 
),
dp_picks as (
	select CASE WHEN (player_full_name like '%Mid%' or player_first_name = '2025') 
		THEN (CASE WHEN player_first_name = '2025' 
				THEN CONCAT(player_first_name, ' Mid ', player_last_name) else player_full_name end)
else player_full_name end as player_full_name
	,sf_value
   ,one_qb_value
from dynastr.dp_player_ranks 
where 1=1
and player_position = 'PICK'
),

fc_picks as (SELECT 
CASE WHEN player_full_name NOT LIKE '%Pick%' THEN 
    (player_first_name || ' Mid ' || player_last_name || 
    CASE
        WHEN player_last_name::INTEGER % 10 = 1 THEN 'st'
        WHEN player_last_name::INTEGER % 10 = 2 THEN 'nd'
        WHEN player_last_name::INTEGER % 10 = 3 THEN 'rd'
        ELSE 'th'
    END) ELSE player_full_name END as player_full_name
		,sf_value
   ,one_qb_value		  
FROM dynastr.fc_player_ranks
WHERE 1 = 1
AND player_position = 'PICK'
AND rank_type = 'dynasty'
),

asset_values as (
select p.full_name as player_full_name
,ktc.ktc_player_id as player_id
, case when ktc.team = 'KCC' then 'KC' else ktc.team end as team
,ktc.sf_value as ktc_sf_value
,ktc.one_qb_value as ktc_one_qb_value
,fc.sf_value as fc_sf_value
,fc.one_qb_value as fc_one_qb_value
,dp.sf_value as dp_sf_value
,dp.sf_value as dp_one_qb_value
, position as _position
from dynastr.players p
inner join dynastr.ktc_player_ranks ktc on concat(p.first_name, p.last_name) = concat(ktc.player_first_name, ktc.player_last_name)
inner join (select sleeper_player_id, sf_value, one_qb_value from dynastr.fc_player_ranks where rank_type = 'dynasty') fc on fc.sleeper_player_id = p.player_id 
inner join dynastr.dp_player_ranks dp on concat(p.first_name, p.last_name) = concat(dp.player_first_name, dp.player_last_name)
and ktc.rookie = 'false'
UNION ALL 
select ktc.player_full_name as player_full_name
,ktc.ktc_player_id as player_id
, null as team
,ktc.sf_value as ktc_sf_value
,ktc.one_qb_value as ktc_one_qb_value
,coalesce(fc.sf_value, ktc.sf_value) as fc_sf_value
,coalesce(fc.one_qb_value, ktc.one_qb_value) as fc_one_qb_value
,coalesce(dp.sf_value, ktc.sf_value) as dp_sf_value
,coalesce(dp.one_qb_value, ktc.one_qb_value) as dp_one_qb_value
, CASE WHEN substring(lower(ktc.player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	WHEN position = 'RDP' THEN 'Pick'
	ELSE position END as _position
from ktc_picks ktc
left join fc_picks fc on ktc.player_full_name = fc.player_full_name
inner join dp_picks dp on ktc.player_full_name = dp.player_full_name
where 1=1
and (ktc.player_full_name like '%2023 Round%' or ktc.position = 'RDP')
	)
	
select 
player_full_name
, player_id
, team
, _position
, ktc_sf_value
, ROW_NUMBER() over (order by ktc_sf_value desc) as ktc_sf_rank
, ktc_one_qb_value
, ROW_NUMBER() over (order by ktc_one_qb_value desc) as ktc_one_qb_rank
, fc_sf_value
, ROW_NUMBER() over (order by fc_sf_value desc) as fc_sf_rank
, fc_one_qb_value
, ROW_NUMBER() over (order by fc_one_qb_value desc) as fc_one_qb_rank
, dp_sf_value
, ROW_NUMBER() over (order by dp_sf_value desc) as dp_sf_rank
, dp_one_qb_value
, ROW_NUMBER() over (order by dp_one_qb_value desc) as dp_one_qb_rank

from asset_values"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    cursor.execute(query)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    values_df = pd.DataFrame(data, columns=column_names)

    enrty_time = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f%z")

    source_columns = ['ktc_sf_value', 'fc_sf_value', 'dp_sf_value',]
    normalized_sf_columns = ['ktc_sf_normalized_value', 'fc_sf_normalized_value', 'dp_sf_normalized_value']

    for source, sf_normalized in zip(source_columns, normalized_sf_columns):
        min_value = values_df[source].min()
        max_value = values_df[source].max()
        values_df[sf_normalized] = ((values_df[source] - min_value) / (max_value - min_value)) * 9999
    
    source_columns = ['ktc_one_qb_value', 'fc_one_qb_value', 'dp_one_qb_value']
    normalized_one_qb_columns = ['ktc_one_qb_normalized_value','fc_one_qb_normalized_value', 'dp_one_qb_normalized_value']

    for source, one_qb_normalized in zip(source_columns, normalized_one_qb_columns):
        min_value = values_df[source].min()
        max_value = values_df[source].max()
        values_df[one_qb_normalized] = ((values_df[source] - min_value) / (max_value - min_value)) * 9999
    
    values_df['average_normalized_sf_value'] = values_df[normalized_sf_columns].mean(axis=1)
    values_df['average_normalized_one_qb_value'] = values_df[normalized_one_qb_columns].mean(axis=1)
    sorted_values_df = values_df.sort_values(by='average_normalized_sf_value', ascending=False)
    values_df = values_df.fillna(0)
    values_df['sf_harmonic_mean_normalized'] = values_df[normalized_sf_columns].apply(hmean, axis=1)
    values_df['one_qb_harmonic_mean_normalized'] = values_df[normalized_one_qb_columns].apply(hmean, axis=1)
    sorted_values_df_1 = values_df.sort_values(by='sf_harmonic_mean_normalized', ascending=False)
    sorted_values_df_1['sf_harmonic_normalized_rank'] = sorted_values_df_1['sf_harmonic_mean_normalized'].rank(ascending=False, method='min')
    sorted_values_df_2 = sorted_values_df_1.sort_values(by='one_qb_harmonic_mean_normalized', ascending=False)
    sorted_values_df_2['one_qb_harmonic_normalized_rank'] = sorted_values_df_2['one_qb_harmonic_mean_normalized'].rank(ascending=False, method='min')
    df = sorted_values_df_2
    df.rename(columns = {'sf_harmonic_mean_normalized':'superflex_sf_value', 'sf_harmonic_normalized_rank':'superflex_sf_rank','one_qb_harmonic_mean_normalized':'superflex_one_qb_value', 'one_qb_harmonic_normalized_rank':'superflex_one_qb_rank'}, inplace = True)
    df['superflex_sf_value'] = df['superflex_sf_value'].fillna(0.0).astype(int)
    df['superflex_sf_rank'] = df['superflex_sf_rank'].fillna(0.0).astype(int)
    df['superflex_one_qb_value'] = df['superflex_one_qb_value'].fillna(0.0).astype(int)
    df['superflex_one_qb_rank'] = df['superflex_one_qb_rank'].fillna(0.0).astype(int)
    df['superflex_sf_pos_rank'] = df.groupby('_position')['superflex_sf_value'].rank(method='min', ascending=False).fillna(0.0).astype(int)
    df['superflex_one_qb_pos_rank'] = df.groupby('_position')['superflex_one_qb_value'].rank(method='min', ascending=False).fillna(0.0).astype(int)
    df['insert_date'] = enrty_time

    # Insert the rows
    for index, row in df.iterrows():
    
        insert_player_data(row, cursor)
    # Commit the transaction and close the cursor
    conn.commit()
    cursor.close()



    

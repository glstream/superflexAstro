with ktc_picks as (select *
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

from asset_values
INSERT INTO history.managers 
(SELECT * FROM dynastr.managers)
ON CONFLICT (user_id) DO UPDATE 
  SET display_name = excluded.display_name,
  league_id = excluded.league_id,
      avatar = excluded.avatar;
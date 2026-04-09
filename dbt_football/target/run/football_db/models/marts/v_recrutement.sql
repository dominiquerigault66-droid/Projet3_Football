
  
    

  create  table
    `football_db`.`v_recrutement__dbt_tmp`
    
    
      as
    
    (
      -- models/marts/v_recrutement.sql
-- Vue profil recruteur — colonnes mercato et performance
-- Exclut les colonnes notoriété/réseaux sociaux

SELECT
    joueur_id,
    nom,
    age,
    birth_date,
    nationality,
    position,
    position_detail,
    club,
    league,
    photo_url,
    team_logo,
    height_cm,
    weight_kg,
    pied_fort,
    player_number,

    -- Valeur marchande
    valeur_eur,
    valeur_texte,
    tm_player_url,

    -- Contrat
    contrat_expiration,
    contrat_expiration_date,
    salaire_brut_semaine_eur,
    salaire_brut_annuel_eur,
    cap_position,

    -- Stats performance
    sfs_rating,
    sfs_appearances,
    sfs_minutes_played,
    sfs_goals,
    sfs_assists,
    goals_assists_p90,
    goals_p90,
    assists_p90,
    saves_p90,
    sfs_expected_goals,
    sfs_expected_assists,
    sfs_yellow_cards,
    sfs_red_cards,

    -- Score sportif
    score_sport,
    score_sport_label,

    -- Score marketing utile aussi pour le recruteur (image du joueur)
    score_marketing,
    score_marketing_label,

    -- Traçabilité
    fbref_url

FROM `football_db`.`v_joueurs_complets`
WHERE position IS NOT NULL
    )

  
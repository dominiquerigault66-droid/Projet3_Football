
    
    

select
    joueur_id as unique_field,
    count(*) as n_records

from `football_db`.`v_joueurs_complets`
where joueur_id is not null
group by joueur_id
having count(*) > 1



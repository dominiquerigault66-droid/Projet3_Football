
    
    

select
    joueur_id as unique_field,
    count(*) as n_records

from `football_db`.`stg_scores`
where joueur_id is not null
group by joueur_id
having count(*) > 1



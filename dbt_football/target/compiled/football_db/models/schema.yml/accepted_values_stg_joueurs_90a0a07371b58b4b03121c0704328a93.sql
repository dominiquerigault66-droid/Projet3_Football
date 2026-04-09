
    
    

with all_values as (

    select
        position as value_field,
        count(*) as n_records

    from `football_db`.`stg_joueurs`
    group by position

)

select *
from all_values
where value_field not in (
    'Attacker','Midfielder','Defender','Goalkeeper'
)



select
      count(*) as failures,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_warn,
      case
        when count(*) <> 0 then 'true'
        else 'false'
      end as should_error
    from (
      
    
    

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



      
    ) dbt_internal_test
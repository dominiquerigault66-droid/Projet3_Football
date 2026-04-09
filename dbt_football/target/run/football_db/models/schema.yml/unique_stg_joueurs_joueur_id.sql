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
      
    
    

select
    joueur_id as unique_field,
    count(*) as n_records

from `football_db`.`stg_joueurs`
where joueur_id is not null
group by joueur_id
having count(*) > 1



      
    ) dbt_internal_test
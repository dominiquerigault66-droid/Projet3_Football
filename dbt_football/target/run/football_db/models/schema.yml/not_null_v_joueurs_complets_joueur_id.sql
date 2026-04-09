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
      
    
    



select joueur_id
from `football_db`.`v_joueurs_complets`
where joueur_id is null



      
    ) dbt_internal_test
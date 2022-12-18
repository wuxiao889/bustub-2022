select
  title,
  votes
from
  (
    select
      r.title_id,
      votes
    from
      (
        select
          title_id
        from
          (
            select
              person_id
            from
              people
            where
              name like '%Cruise%'
              and born = 1962
          ) p
          left join crew c on p.person_id = c.person_id
      ) pc
      left join ratings r on r.title_id = pc.title_id
    order by
      votes desc
    limit
      10
  ) rpc
  inner join akas a on a.title_id = rpc.title_id
group by
  a.title_id
order by
  votes desc;
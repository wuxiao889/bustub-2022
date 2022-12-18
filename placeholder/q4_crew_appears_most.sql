select
  name,
  num_appearances
from
  (
    select
      person_id,
      count(*) num_appearances
    from
      crew
    group by
      person_id
    order by
      num_appearances desc
    limit
      20
  ) c
  left join people p on p.person_id = c.person_id;
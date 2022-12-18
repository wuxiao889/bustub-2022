select
  name,
  y - x age
from
  (
    select
      name,
      born x,
      ifnull(died, 2022) y
    from
      people
    where
      born is not null
      and born >= 1900
  )
ORDER by
  y - x desc,
  name
limit
  20;
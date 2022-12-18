select
  count(*)
from
  (
    select
      premiered
    from
      (
        select
          title_id
        from
          akas
        where
          title = 'Army of Thieves'
        limit
          1
      ) a
      inner join titles on titles.title_id = a.title_id
  ) ta
  inner join titles t on t.premiered = ta.premiered;
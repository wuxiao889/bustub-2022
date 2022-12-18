select
  name
from
  (
    select
      DISTINCT person_id
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
              name = 'Nicole Kidman'
          ) p
          inner join crew on crew.person_id = p.person_id
      ) tc
      inner join crew on crew.title_id = tc.title_id
  ) ctc inner join people on people.person_id = ctc.person_id order by name;
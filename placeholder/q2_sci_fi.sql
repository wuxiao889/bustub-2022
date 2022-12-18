SELECT
  a.title,
  t.premiered,
  t.runtime_minutes || ' mins'
FROM
  (
    SELECT
      title_id,
      premiered,
      runtime_minutes
    FROM
      titles
    WHERE
      genres like '%Sci-Fi%'
  ) t
  INNER JOIN akas a ON a.title_id = t.title_id
LIMIT
  10;
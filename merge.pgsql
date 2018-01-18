do
$$

declare  SEPARATOR constant character := ',';

begin

if not exists (
   select 1
   from information_schema.tables
   where table_schema = 'public' and table_name = 'expired_doc'
) then raise exception 'table not found';
end if;

create index idx_tank_id on tank using btree(id);

update tank set
  serie = split_part("raw", SEPARATOR, 1),
  number = split_part("raw", SEPARATOR, 2);

create index idx_tank_serie_number on tank using btree (serie collate "C", number collate "C");

delete from tank t
where id in (
  select id
  from (
    select *, row_number() over(partition by serie, number order by id asc) rn
    from tank
  ) t
  where t.rn > 1 or (upper(t.serie) == 'PASSP_SERIES' and upper(t.number) = 'PASSP_NUMBER')
);

with rr as (
  select
    s.serie sser,
    s.number snum,
    t.id tid,
    t.serie tser,
    t.number tnum,
    t.is_removed tremoved,
    t.created_at tcreated_at,
    t.updated_at tupdated_at
  from
    tank s
    full join expired_doc t on t.serie = s.serie and t.number = s.number
)
update expired_doc t
set
  is_removed = (
    case
      when (tt.sser is null and tt.tser is not null and tt.tremoved = false) then true
      when (tt.sser is not null and tt.tser is not null and tt.tremoved = true) then false
    end)::boolean,
  updated_at = now()
from (
  select * from rr c
  where 
    (c.sser is null and c.tser is not null and c.tremoved = false) or
    (c.sser is not null and c.tser is not null and c.tremoved = true)
) tt
where t.id = tt.tid;

insert into expired_doc (serie, number)
select s.serie, s.number
from tank s
where not exists (
  select 1 from expired_docs t
  where t.serie = s.serie and t.number = s.number
  limit 1 
);

drop table if exists tank;
 
end

$$
language plpgsql;


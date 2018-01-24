create table expired_passport (
  id bigserial primary key not null,
  created_at timestamp not null default now(),
  updated_at timestamp null,
  serie character varying(15) null,
  number character varying(15) null,
  is_removed boolean not null default false
);
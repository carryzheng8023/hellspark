create table day_video_city_topn_stat (
  day    varchar(8) not null,
  cms_id bigint(10) not null,
  city varchar(20) not null,
  times  bigint(10) not null,
  times_rank  int(10) not null,
  primary key (day, city, cms_id)
);
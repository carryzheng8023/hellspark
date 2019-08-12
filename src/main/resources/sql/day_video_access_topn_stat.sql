create table day_video_access_topn_stat (
  day    varchar(8) not null,
  cms_id bigint(10) not null,
  times  bigint(10) not null,
  primary key (day, cms_id)
);
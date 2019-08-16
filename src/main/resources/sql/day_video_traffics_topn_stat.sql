create table day_video_traffics_topn_stat (
  day    varchar(8) not null,
  cms_id bigint(10) not null,
  traffics bigint(20) not null,
  primary key (day, cms_id)
);
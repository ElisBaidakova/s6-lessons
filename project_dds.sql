drop table if exists VT260419031E56__DWH.l_user_group_activity;

CREATE TABLE VT260419031E56__DWH.l_user_group_activity (
  hk_l_user_group_activity BIGINT PRIMARY KEY,
  hk_user_id BIGINT NOT NULL,
  hk_group_id BIGINT NOT NULL,
  load_dt DATETIME,
  load_src VARCHAR(20),
  CONSTRAINT fk_l_user_group_activity_user FOREIGN KEY (hk_user_id) REFERENCES VT260419031E56__DWH.h_users (hk_user_id),
  CONSTRAINT fk_l_user_group_activity_group FOREIGN KEY (hk_group_id) REFERENCES VT260419031E56__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2);

INSERT INTO VT260419031E56__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select
hash(hu.hk_user_id, hg.hk_group_id),
hu.hk_user_id,
hg.hk_group_id,
now() as load_dt,
's3' as load_src
from VT260419031E56__STAGING.group_log as gl
left join VT260419031E56__DWH.h_users as hu on gl.user_id = hu.user_id
left join VT260419031E56__DWH.h_groups as hg on gl.group_id = hg.group_id
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from VT260419031E56__DWH.l_user_group_activity);

drop table if exists VT260419031E56__DWH.s_auth_history;

CREATE TABLE VT260419031E56__DWH.s_auth_history (
hk_l_user_group_activity BIGINT NOT NULL,
user_id_from int,
event varchar,
event_dt timestamp,
load_dt DATETIME,
load_src varchar(20),
CONSTRAINT s_auth_history_user_group_activity FOREIGN KEY (hk_l_user_group_activity)
REFERENCES VT260419031E56__DWH.l_user_group_activity (hk_l_user_group_activity)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VT260419031E56__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select 
luga.hk_l_user_group_activity,
gl.user_id_from,
gl.event,
gl.datetime as event_dt,
now() as load_dt,
's3' as load_src
FROM VT260419031E56__STAGING.group_log gl
JOIN VT260419031E56__DWH.h_users hu ON gl.user_id = hu.user_id
JOIN VT260419031E56__DWH.h_groups hg ON gl.group_id = hg.group_id
JOIN VT260419031E56__DWH.l_user_group_activity luga
  ON luga.hk_user_id = hu.hk_user_id AND luga.hk_group_id = hg.hk_group_id
WHERE gl.datetime > (
    SELECT COALESCE(MAX(event_dt), '1900-01-01'::timestamp) 
    FROM VT260419031E56__DWH.s_auth_history
);

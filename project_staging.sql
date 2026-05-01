drop table if exists VT260419031E56__STAGING.group_log;

create table if not exists VT260419031E56__STAGING.group_log(
group_id int,
user_id int,
user_id_from int,
event varchar,
datetime timestamp,
FOREIGN KEY (user_id) REFERENCES VT260419031E56__STAGING.users (id),
FOREIGN KEY (user_id_from) REFERENCES VT260419031E56__STAGING.users (id)
) ORDER BY group_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

copy VT260419031E56__STAGING.group_log (
group_id, user_id, user_id_from, event, datetime)
from local '/Users/alisabaidakova/s6-lessons/data/group_log.csv'
delimiter ',';
with user_group_log as (
    select
        luga.hk_group_id,
        COUNT (DISTINCT luga.hk_user_id) AS cnt_added_users
        FROM VT260419031E56__DWH.l_user_group_activity luga
        JOIN (
            SELECT DISTINCT hk_l_user_group_activity 
            FROM VT260419031E56__DWH.s_auth_history 
            WHERE event = 'add'
        ) sah ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
        WHERE luga.hk_group_id IN (
            SELECT hk_group_id 
            FROM VT260419031E56__DWH.h_groups 
            ORDER BY registration_dt ASC 
            LIMIT 10
        )
        GROUP BY luga.hk_group_id
), 
user_group_messages as (
    select 
            luga.hk_group_id,
            COUNT(DISTINCT luga.hk_user_id) AS cnt_users_in_group_with_messages
            FROM VT260419031E56__DWH.l_user_group_activity luga
            JOIN VT260419031E56__DWH.l_groups_dialogs lgd ON luga.hk_group_id = lgd.hk_group_id
            JOIN VT260419031E56__DWH.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id
            AND luga.hk_user_id = lum.hk_user_id
            GROUP BY luga.hk_group_id
)
select
ugl.hk_group_id,
ugl.cnt_added_users,
ugm.cnt_users_in_group_with_messages,
ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;
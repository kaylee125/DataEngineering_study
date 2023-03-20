SELECT count(user_id)
from user_info
-- 0개 이상의 문자 대신 표현할때: %
-- 1개의 문자 대신 표현할때: _
where joined like '2021-%-%'
and age>=20 and age<=29
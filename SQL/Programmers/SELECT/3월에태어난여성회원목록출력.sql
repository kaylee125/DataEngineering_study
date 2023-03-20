SELECT MEMBER_ID,MEMBER_NAME,GENDER,date_format(DATE_OF_BIRTH,'%Y-%m-%d') as DATE_OF_BIRTH
from member_profile
where  gender='W'and tlno is not null and month(DATE_OF_BIRTH)='3'
order by member_id
SELECT DR_NAME,DR_ID,MCDP_CD,date_format(hire_ymd,'%Y-%m-%d') as HIRE_YMD
from doctor
where mcdp_cd ='GS' or mcdp_cd='CS'
order by hire_ymd desc,dr_name
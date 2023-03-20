--  '통풍시트', '열선시트', '가죽시트' 가 하나 이상 포함된 자동차
-- 해당되는 자동차의 갯수를 자동차 종류별로 그룹핑 후 count한 결과값을 출력
-- 종류별 오름차순 정렬
-- ** options like A or B or C (X)
-- ** options like A or options like B or options like C ->이렇게 입력해야함
select car_type,count(*) as cars
from (select * from CAR_RENTAL_COMPANY_CAR where options like '%통풍시트%' or options like'%열선시트%' or options like '%가죽시트%') a
group by car_type
order by car_Type
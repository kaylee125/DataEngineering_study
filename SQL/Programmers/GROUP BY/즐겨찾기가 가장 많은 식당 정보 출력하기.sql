
-- 테이블에서 음식종류별로 즐겨찾기 수가 가장 많은 식당:FOOD_TYPE과 MAX(FAVORITES)인 테이블 만들기
-- select food_type,max(favorites) from rest_info group by food_type
-- 조회결과와 원래 테이블을 where조건 서브쿼리로 연결하여 최종결과 출력

select FOOD_TYPE,REST_ID,REST_name,favorites
from rest_info 
where (food_type,favorites) in (select food_type,max(favorites) from rest_info group by food_type)
order by food_type desc

-- inner join 으로 풀수도 있음
SELECT REST_INFO.FOOD_TYPE, REST_INFO.REST_ID, REST_INFO.REST_NAME, REST_INFO.FAVORITES as FAVORITES
FROM
(SELECT FOOD_TYPE, MAX(FAVORITES) as max_fav
FROM REST_INFO
GROUP BY FOOD_TYPE) tmp 
INNER JOIN REST_INFO on tmp.FOOD_TYPE = REST_INFO.FOOD_TYPE AND tmp.max_fav = REST_INFO.FAVORITES
ORDER BY FOOD_TYPE DESC
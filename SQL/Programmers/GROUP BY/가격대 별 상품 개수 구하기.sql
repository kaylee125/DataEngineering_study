--PRODUCT 테이블에서 만원 단위의 가격대 별로 상품 개수를 출력하는 SQL 문을 작성
-- 만원대별로 그룹핑하는게 관건
-- 가격에 10000원을 나누면 float형태의 값이 나옴.
--floor 혹은 trunc 사용해서 소숫점 자르면 해당 금액대가 나옴
SELECT floor(price/10000)*10000 as price_group,count(*) as products 
from product
group by floor(price/10000) 
order by price

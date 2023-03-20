
-- 1.거래가 완료된 테이블을 만든다.
-- select * from USED_GOODS_BOARD where status='DONE' 
-- 2.그 테이블에서 유저별 그룹핑을 하고 총 금액 70만원 이상인 사람 조회한 후 정렬
select a.WRITER_ID,b.NICKNAME, sum(a.PRICE) as TOTAL_SALES
from (select * from USED_GOODS_BOARD where status='DONE') a
inner join USED_GOODS_USER b on a.WRITER_ID	=b.USER_ID
group by a.writer_id
having sum(a.price)>=700000
order by sum(a.PRICE)
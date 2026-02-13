

============= checking dupplicate==========================
select * from(
select *,
ROW_NUMBER() over(partition by transactionid order by transactionid)  as rnk 
from trans) a
where a.rnk>1

---------------- Cleaning Data ------------------------------------------
----------------               ------------------------------------------

exec sp_rename 'transaction','transactions'

select CustomerDOB,DATETRUNC(month,CustomerDOB) from trans

select * from trans_backup

select  *  from [dbo].[transaction] a
inner join trans_backup b on a.transactionid=b.transactionid
;
update a
set a.product_id=b.product_id,
a.category_id=b.category_id from [dbo].[transaction] a
inner join trans_backup b on a.transactionid=b.TransactionID

alter table [dbo].[transaction] add Product_id bigint,category_id bigint

WITH CLEANED_CATEGORY AS(
select  category_code,
CASE WHEN category_code like'%auto%' THEN 
SUBSTRING(REPLACE(category_code,'.',' '),CHARINDEX(' ',REPLACE(category_code,'.',' ')),
charindex(' ',REPLACE(category_code,'.',' '),CHARINDEX(' ',REPLACE(category_code,'.',' '))+1)-(CHARINDEX(' ',REPLACE(category_code,'.',' '))))
ELSE LEFT(category_code,CHARINDEX('.',CATEGORY_CODE)-1) END AS CLEANED_CAT
from transactions) --- EXTRACTING CATEGORY FROM CATEGORY_CODE
SELECT * FROM CLEANED_CATEGORY

update [dbo].[transaction]
set products=(
				CASE WHEN LEN(CATEGORY_CODE)-LEN(REPLACE(CATEGORY_CODE,'.',''))>1
				THEN right(category_code,LEN(CATEGORY_CODE)-CHARINDEX('.',category_code,CHARINDEX('.',category_code)+1))
				else right(category_code,LEN(CATEGORY_CODE)-CHARINDEX('.',category_code)) end) 
				FROM [dbo].[transaction]

alter table [dbo].[transaction] add products varchar(55);
alter table [dbo].[transaction] add Category varchar(55);
select * from [dbo].[transaction]

update [dbo].[transaction]
set Category=(
				CASE WHEN category_code like'%auto%' THEN 
				SUBSTRING(REPLACE(category_code,'.',' '),CHARINDEX(' ',REPLACE(category_code,'.',' ')),
				charindex(' ',REPLACE(category_code,'.',' '),CHARINDEX(' ',REPLACE(category_code,'.',' '))+1)-(CHARINDEX(' ',REPLACE(category_code,'.',' '))))
				ELSE LEFT(category_code,CHARINDEX('.',CATEGORY_CODE)-1) END )
				from [dbo].[transaction] 

select distinct category from [dbo].[transaction]
select distinct products from [dbo].[transaction]

select category_code from [dbo].[transaction]
where category_code like '%faucet%'

SET STATISTICS IO ON;
SET STATISTICS TIME ON;
go;
create view cohort as(
with first_purchase as (
select customerid,MIN(transactiondate) as first_purchase,
DATETRUNc(month,MIN(transactiondate)) as cohort_mnth 
from transaction
group by customerid),
cohort_count as (
select f.cohort_mnth,
DATEDIFF(month,f.first_purchase,t.TransactionDate) as cohort_index,
COUNT(distinct t.CustomerID) as active_users
from first_purchase f
inner join transaction t on f.CustomerID=t.CustomerID
group by f.cohort_mnth,DATEDIFF(month,f.first_purchase,t.TransactionDate)),
retention_rate_PERCENT as(
select cohort_mnth,cohort_index,active_users,
round((cast(active_users as float))/(FIRST_VALUE(active_users) 
over(partition by cohort_mnth order by cohort_index))*100,2) as retention_rate 
from cohort_count)
select cohort_mnth,
MAX(case when cohort_index=0 then retention_rate ELSE '' end) as MNTH_0,
MAX(case when cohort_index=1 then retention_rate ELSE '' end) as MNTH_1,
MAX(case when cohort_index=2 then retention_rate ELSE '' end) as MNTH_2,
MAX(case when cohort_index=3 then retention_rate ELSE '' end) as MNTH_3,
MAX(case when cohort_index=4 then retention_rate ELSE '' end) as MNTH_4,
MAX(case when cohort_index=5 then retention_rate ELSE '' end) as MNTH_5,
MAX(case when cohort_index=6 then retention_rate  ELSE ''end) as MNTH_6,
MAX(case when cohort_index=7 then retention_rate ELSE '' end) as MNTH_7,
MAX(case when cohort_index=8 then retention_rate ELSE '' end) as MNTH_8,
MAX(case when cohort_index=9 then retention_rate ELSE '' end) as MNTH_9,
MAX(case when cohort_index=10 then retention_rate ELSE'' end) as MNTH_10,
MAX(case when cohort_index=11 then retention_rate ELSE '' end ) as MNTH_11
from retention_rate_PERCENT
group by cohort_mnth);
--order by cohort_mnth 

SET NOCOUNT ON;
SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

USE ankit

CREATE CLUSTERED INDEX CIX_TXN_DATE_ID ON [dbo].[transaction] (TRANSACTIONDATE,TRANSACTIONID);
CREATE NONCLUSTERED INDEX NIX_CUSTOMER_DATE ON [dbo].[transaction] (CUSTOMERID,TRANSACTIONDATE);

DROP INDEX CIX_TXN_DATE_ID ON [dbo].[transaction] 
DROP INDEX NIX_CUSTOMER_DATE ON [dbo].[transaction]

SELECT T1.Category AS CATEGORY_A,
T2.CATEGORY AS CATEGORY_B, COUNT(DISTINCT T1.CustomerID) AS CNT FROM [dbo].[transaction] T1
INNER JOIN [dbo].[transaction] T2 ON T1.CustomerID=T2.CustomerID AND T1.Category<T2.Category
GROUP BY T1.Category,T2.Category
ORDER BY CNT DESC

SELECT TransactionID,COUNT(CATEGORY) AS ITEMBASKET FROM [dbo].[transaction]
GROUP BY TransactionID
HAVING COUNT(CATEGORY)>1

select * from dbo.[transaction]

create view dim_customer as(
select * from (
select customerid,customerdob,custgender,custlocation,
ROW_NUMBER() over(partition by customerid order by transactiondate desc) rn --scd tye 2
from transactions)a
where rn=1)

create view dim_products as(
select distinct PRODUCT_id,product_name,brand,category from transactions)
go
create procedure cohort_retention 
as begin
with first_purchase as (
select customerid,MIN(transactiondate) as first_purchase,
DATETRUNc(month,MIN(transactiondate)) as cohort_mnth 
from transactions
group by customerid),
cohort_count as (
select f.cohort_mnth,
DATEDIFF(month,f.first_purchase,t.TransactionDate) as cohort_index,
COUNT(distinct t.CustomerID) as active_users
from first_purchase f
inner join transactions t on f.CustomerID=t.CustomerID
group by f.cohort_mnth,DATEDIFF(month,f.first_purchase,t.TransactionDate)),
retention_rate_PERCENT as(
select cohort_mnth,cohort_index,active_users,
round((cast(active_users as float))/(FIRST_VALUE(active_users) 
over(partition by cohort_mnth order by cohort_index))*100,2) as retention_rate 
from cohort_count)
select cohort_mnth,
MAX(case when cohort_index=0 then retention_rate ELSE '' end) as MNTH_0,
MAX(case when cohort_index=1 then retention_rate ELSE '' end) as MNTH_1,
MAX(case when cohort_index=2 then retention_rate ELSE '' end) as MNTH_2,
MAX(case when cohort_index=3 then retention_rate ELSE '' end) as MNTH_3,
MAX(case when cohort_index=4 then retention_rate ELSE '' end) as MNTH_4,
MAX(case when cohort_index=5 then retention_rate ELSE '' end) as MNTH_5,
MAX(case when cohort_index=6 then retention_rate  ELSE ''end) as MNTH_6,
MAX(case when cohort_index=7 then retention_rate ELSE '' end) as MNTH_7,
MAX(case when cohort_index=8 then retention_rate ELSE '' end) as MNTH_8,
MAX(case when cohort_index=9 then retention_rate ELSE '' end) as MNTH_9,
MAX(case when cohort_index=10 then retention_rate ELSE'' end) as MNTH_10,
MAX(case when cohort_index=11 then retention_rate ELSE '' end ) as MNTH_11
from retention_rate_PERCENT
group by cohort_mnth
order by cohort_mnth
end;
go

 ====================== Creating Dimension View =======================
create view dim_category as 
select *,
ROW_NUMBER() over (order by category_id) as catskey
from (
select distinct category,category_id
from transactions)a

select t from dim_products

sp_helptext 'dbo.dim_customer';

alter view dbo.dim_products as 
select *,row_number() over(order by product_id) as prodkey from (
select distinct PRODUCT_id,product_name,brand,category from transactions)a

alter view dbo.dim_customer as 
select *,
ROW_NUMBER() over(order by customerid) as custkey 
from (  
select customerid,customerdob,custgender,custlocation,  
ROW_NUMBER() over(partition by customerid order by transactiondate desc) rn --Convert Into SCD TYPE !
from transactions)a  
where rn=1

select top 1.* from dim_products
select top 1.* from dim_category
select top 1.* from dim_customer
select COUNT(*) from fact_transactions

sp_helptext 'dbo.fact_transactions'

create view dim_pymntmethod as
select *, 
row_number() over(order by paymentmode) as pymtkey from (
select distinct PaymentMode from transactions)a

alter view dbo.fact_transactions as
select  a.TransactionID,a.TransactionDate,a.price,p.prodkey,c.catskey,cu.custkey,pymnt.pymtkey from transactions a
inner join dim_products p on a.Product_id =p.Product_id
inner join dim_category c on a.category=c.category
inner join dim_customer cu on a.CustomerID=cu.CustomerID
inner join dim_pymntmethod pymnt on a.PaymentMode=pymnt.PaymentMode

select * from fact_transactions

alter view  dim_category as
Select *,ROW_NUMBER() over(order by Category) as catskey from (
select DISTINCT Category from dim_category)a

select *
 from dim_category

 sp_helptext 'dbo.dim_category'
 
 create view dim_category as 
 select *,ROW_NUMBER() over(order by category) as catskey from (
select distinct category from transactions)a

select * from dim_category
select * from fact_transactions
select * from dim_products
select * from dim_pymntmethod
select * from market_basket

sp_helptext 'fact_transactions'
sp_helptext 'dim_products'

select * from market_basket




CREATE or alter view dbo.fact_transactions 
as  
select  a.TransactionID,a.TransactionDate,a.price,p.prodkey,c.catskey,cu.custkey,pymnt.pymtkey from transactions a  
inner join dim_products p on a.Product_id =p.Product_id  
inner join dim_category c on a.category=c.category  
inner join dim_customer cu on a.CustomerID=cu.CustomerID  
inner join dim_pymntmethod pymnt on a.PaymentMode=pymnt.PaymentMode



select top 1* from fact_transactions
======================== RFM Calaculation ================================================================
create view RFM as
with base_metrics as(
select custkey,
DATEDIFF(day,MAX(TransactionDate),(select MAX(transactiondate) from fact_transaction)) as raw_receny,
COUNT(transactionid) as raw_frequency,
round(SUM(price),2) as raw_monetary
from fact_transaction
group by custkey),
rfm_score as(
select *,CONCAT(cast(r_score as varchar),cast(f_score as varchar),cast(m_score as varchar)) as segment_score from(
select *,
NTILE(5)over(order by raw_receny desc) as r_score,
NTILE(5) over(order by raw_frequency asc) as f_score,
NTILE(5) over(order by raw_monetary asc) as m_score
from base_metrics)a)
select * from rfm_score r
inner join
Segment_Scores s on r.segment_score=s.Scores 

select top 50 p1.product_name as primary_product,p2.product_name as associated_name,COUNT(distinct t1.custkey) from fact_transaction t1
inner join fact_transaction t2 on t1.custkey=t1.custkey and t1.prodkey<t2.prodkey
inner join dim_product p1 on t1.prodkey=p1.prodkey
inner join dim_product p2 on t2.prodkey=p2.prodkey
group by p1.product_name,p2.product_name

create nonclustered index ix_fact_prodkey on fact_transactions(prodkey)

select * from transactions a  
inner join dim_products p on a.Product_id =p.Product_id  
inner join dim_category c on a.category=c.category  
inner join dim_customer cu on a.CustomerID=cu.CustomerID  
inner join dim_pymntmethod pymnt on a.PaymentMode=pymnt.PaymentMode

================Convert view into Table for Performance ===============================

select * into dim_customers from dim_customer
select * into dim_product from dim_products
select * into dim_categories from dim_category
select * into fact_transaction from fact_transactions
select * into dim_pymnth from dim_pymntmethod
SELECT *,ROW_NUMBER() OVER(ORDER BY PRODUCT_NAME)AS prodkey into dim_product FROM (
SELECT DISTINCT product_name FROM TRANSACTIONS)A

select * from dim_product
select * from dim_category
select * from dim_categories
select * from dim_brand

select *,ROW_NUMBER() over(order by brand) as brandkey into dim_brand from (
select distinct brand from transactions)a


create unique clustered index PK_dim_prod on dim_product(prodkey)
create unique clustered index PK_dim_cust on dim_customers(custkey)
create unique clustered index Pk_dim_cats on dim_categories(catskey)

create nonclustered index ix_fact_rfm_optimization on fact_transaction(custkey) include(transactiondate,price,transactionid);
drop index ix_fact_rfm_optimization on fact_transaction
create nonclustered index IX_rfm_mba on fact_transaction(catskey,transactiondate) include(price,prodkey)


create nonclustered index IX_Fact_transaction on fact_transaction (custkey,prodkey)

drop table dim_product


sp_helptext 'fact_transactions'


select  a.TransactionID,a.TransactionDate,a.price,p.prodkey,c.catskey,cu.custkey,pymnt.pymtkey,brandkey  into fact_transaction from transactions a    
inner join dim_product p on a.product_name =p.Product_name   
inner join dim_category c on a.category=c.category    
inner join dim_customer cu on a.CustomerID=cu.CustomerID    
inner join dim_pymntmethod pymnt on a.PaymentMode=pymnt.PaymentMode
inner join dim_brand b on b.brand=a.brand

drop table fact_transaction


/* dim_product
dim_categories
dim_brand
dim_pymnth
dim_customer
dim_date
fact_table





===========================================================================================================


sp_helpindex 'fact_transaction'

drop index IX_Fact_transaction on fact_transaction
create nonclustered index IX_rfm_mba on fact_transaction(catskey,transactiondate) include(price,prodkey)

select * from nrr

sp_helptext'nrr'
sp_helptext'new_cohort_retention'
sp_helptext'RFM'


=========================================================================views=========================


create  or alter view NRR as
with first_purchase as (  
select custkey,MIN(TransactionDate) as first_purchase,  
DATETRUNC(month,MIN(TransactionDate)) as cohort_mnth  
from fact_transaction  
group by custkey),  
  
cohort_num as (  
select cohort_mnth,DATEDIFF(month,cohort_mnth,Transactiondate) as cohort_index,  
SUM(price) as active_revenue  
from first_purchase f   
inner join fact_transaction t  
on f.custkey=t.custkey  
group by cohort_mnth,DATEDIFF(month,cohort_mnth,Transactiondate)),  
  
retention_rate_PERCENT as(  
select cohort_mnth,cohort_index,active_revenue,  
round((cast(active_revenue as float))/(FIRST_VALUE(active_revenue)   
over(partition by cohort_mnth order by cohort_index))*100,2) as retention_rate   
from cohort_num  
)  
select * from retention_rate_PERCENT


=======================================================================================================================

create or alter view new_cohort_retention as
with first_purchase as (  
select custkey,MIN(transactiondate) as first_purchase,  
DATETRUNc(month,MIN(transactiondate)) as cohort_mnth   
from fact_transaction  
group by custkey),  

cohort_count as (  
select f.cohort_mnth,  
DATEDIFF(month,f.first_purchase,t.TransactionDate) as cohort_index,  
COUNT(distinct t.custkey) as active_users  
from first_purchase f  
inner join fact_transaction t on f.custkey=t.custkey  
group by f.cohort_mnth,DATEDIFF(month,f.first_purchase,t.TransactionDate)),  

retention_rate_PERCENT as(  
select cohort_mnth,cohort_index,active_users,  
round((cast(active_users as float))/(FIRST_VALUE(active_users)   
over(partition by cohort_mnth order by cohort_index))*100,2) as retention_rate   
from cohort_count)  


select * from retention_rate_PERCENT



=====================================================================================================================

with base_metrics as(  
select custkey,  
DATEDIFF(day,MAX(TransactionDate),(select MAX(transactiondate) from fact_transaction)) as raw_receny,  
COUNT(transactionid) as raw_frequency,  
round(SUM(price),2) as raw_monetary  
from fact_transaction  
group by custkey),  
rfm_score as(  
select *,CONCAT(cast(r_score as varchar),cast(f_score as varchar),cast(m_score as varchar)) as segment_score from(  
select *,  
NTILE(5)over(order by raw_receny desc) as r_score,  
NTILE(5) over(order by raw_frequency asc) as f_score,  
NTILE(5) over(order by raw_monetary asc) as m_score  
from base_metrics)a)  
select * from rfm_score r  
inner join  
Segment_Scores s on r.segment_score=s.Scores   */

with brand_cummulative as(
with cte as(
select brand,SUM(price) as total from fact_transaction f 
inner join dim_brand d on d.brandkey=f.brandkey
group by brand)
select *,
concat(round(SUM(total) over(order by total desc)/SUM(total) over(),2)*100,'%') as running_percent
from cte
====================================================================================================
with cte as(
select category,SUM(price) as total from fact_transaction f 
inner join dim_categories d on d.catskey=f.catskey
group by category)
select *,
concat(round(SUM(total) over(order by total desc)/SUM(total) over(),2)*100,'%') as running_percent
from cte

==========================================================================================================
with cte as(
select product_name,SUM(price) as total from fact_transaction f 
inner join dim_product d on d.prodkey=f.prodkey
group by product_name)
select *,
concat(round(SUM(total) over(order by total desc)/SUM(total) over(),2)*100,'%') as running_percent
from cte
===================================================================================================================

select * from nrr
select top 1 * from fact_transaction

select * from rfm

sp_helptext 'rfm'


with base_metrics as(  
select custkey,  
DATEDIFF(day,MAX(TransactionDate),(select MAX(transactiondate) from fact_transactions)) as raw_receny,  
COUNT(transactionid) as raw_frequency,  
round(SUM(price),2) as raw_monetary  
from fact_transactions  
group by custkey),  
rfm_score as(  
select *,CONCAT(cast(r_score as varchar),cast(f_score as varchar),cast(m_score as varchar)) as segment_score from(  
select *,  
NTILE(5)over(order by raw_receny desc) as r_score,  
NTILE(5) over(order by raw_frequency asc) as f_score,  
NTILE(5) over(order by raw_monetary asc) as m_score  
from base_metrics)a)  
select * from rfm_score r  
inner join  
Segment_Scores s on r.segment_score=s.Scores   
  
  select count(*) from dim_product d
  left join fact_transaction f on d.prodkey=f.prodkey 
  where d.prodkey is null

  select * from fact_transaction
  where prodkey is null

select custkey,count(*) from rfm
group by custkey
having count(*)>1
order by custkey desc

select count(*) from (
select custkey,count(*) as tot from fact_transaction
group by custkey
having count(*)>1)a

select * from segment_scores
group by scores
having count(*)>1

select * from segment_scores
 where scores in (231,241,251)

 with cte as(
 select *,
 row_number() over(partition by scores order by segment desc) as rn
 from segment_scores)
 delete from cte where rn>1

 
--Total No. of Host and Superhost

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,No_Of_Host from (
select host_is_superhost,count(*) as No_OF_Host from dbo.host_rome_df where
host_is_superhost = 0 or host_is_superhost =1 group by host_is_superhost ) c;

--Response Rate

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,AvgResponseRate,MinResponseRate,MaxResponseRate from (
select host_is_superhost,avg(host_response_rate) as AvgResponseRate, min
(host_response_rate) as MinResponseRate , max(host_response_rate) as maxResponseRate 
from dbo.host_rome_df where host_is_superhost = 1 and host_response_rate !=0 or
host_is_superhost = 0 and host_response_rate!=0 group by host_is_superhost) c;--No of host having response rate > avg(response rate)

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,No_Of_Host from(
select host_is_superhost,count(host_id) as No_OF_Host from dbo.host_rome_df where
host_response_rate > (
select avg(host_response_rate) as Avg_Response_rate from dbo.host_rome_df) group by
host_is_superhost)c;--Listing Countselect case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,AvgListingCount,MinListingCount,MaxListingCount from (
select host_is_superhost,avg(host_listings_count) as AvgListingCount, min
(host_listings_count) as MinListingCount , max(host_listings_count) as MaxListingCount 
from dbo.host_rome_df where host_is_superhost = 1 or host_is_superhost = 0 group by
host_is_superhost) cc;--Listing Count > Avg. listing countselect case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,No_Of_Host from(
select host_is_superhost,count(host_id) as No_OF_Host from dbo.host_rome_df where
host_listings_count > (
select avg(host_listings_count) as Avg_Listing_rate from dbo.host_rome_df) group by
host_is_superhost)c;--Response Timeselect case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,host_response_time,TotalHost from (
select host_is_superhost,host_response_time,count(host_id) as TotalHost from
dbo.host_rome_df where host_is_superhost =0 and host_response_time is not null or
host_is_superhost =1 and host_response_time is not null group by
host_is_superhost,host_response_time ) ccc order by host_Superhost;--Acceptance Rateselect case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,AvgAcceptanceRate,MinAcceptanceRate,MaxAcceptanceRate from (
select host_is_superhost,avg(host_acceptance_rate) as AvgAcceptanceRate, min
(host_acceptance_rate) as MinAcceptanceRate , max(host_acceptance_rate) as
MaxAcceptanceRate from dbo.host_rome_df where host_is_superhost = 1 or
host_is_superhost = 0 group by host_is_superhost) c;--No of host having acceptance rate > avg(acceptance rate)select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,No_Of_Host from(
select host_is_superhost,count(host_id) as No_OF_Host from dbo.host_rome_df where
host_acceptance_rate > (
select avg(host_acceptance_rate) as Avg_Acceptance_rate from dbo.host_rome_df) group
by host_is_superhost)c;

--profile Pic

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,host_has_profile_pic,TotalHost from (
select host_is_superhost,host_has_profile_pic,count(host_id) as TotalHost from
dbo.host_rome_df where host_is_superhost =0 and host_has_profile_pic is not null or
host_is_superhost =1 and host_has_profile_pic is not null group by
host_is_superhost,host_has_profile_pic ) ccc order by host_Superhost;

--Identity Verified

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as host_Superhost,
case when host_identity_verified = 0 then 'NO'
when host_identity_verified = 1 then 'YES'end as
host_identity_verified,TotalHost from (
select host_is_superhost,host_identity_verified,count(host_id) as TotalHost from
dbo.host_rome_df where host_is_superhost =0 and host_identity_verified is not null or
host_is_superhost =1 and host_identity_verified is not null group by
host_is_superhost,host_identity_verified ) ccc order by host_Superhost;

--Comments

select q.hosts,Avg(count_comments) as avg_comments from (select
h.host_id,h.host_is_superhost as hosts,count(r.comments) as count_comments from
host_rome_df h join listing_rome_df l on l.host_id = h.host_id
join review_rome_df r on r.listing_id = l.id
join df_rome_availability a on a.listing_id = l.id
group by h.host_id,h.host_is_superhost) q
where q.hosts is not null
group by q.hosts;

--Avg. Monthly Booking

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,Average_montly_Booking from(
select h.host_is_superhost , avg (g.Avg_booking) as Average_montly_Booking from (
select host_id,avg(Total_booking) as Avg_booking from (
select distinct b.host_id,month(a.date) as month,count(a.available) as Total_Booking 
from dbo.df_rome_availability a inner join dbo.listing_rome_df b on a.listing_id=b.id 
where a.available = 'False' group by host_id,month(a.date))c 
group by host_id )g inner join dbo.host_rome_df h on g.host_id=h.host_id where
h.host_is_superhost=0 or h.host_is_superhost =1 group by h.host_is_superhost)cccc;

--Instant Booking

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,NO_of_host from
(select b.host_is_superhost,count(distinct a.host_id) as
NO_of_host from dbo.listing_rome_df a 
inner join dbo.host_rome_df b on a.host_id=b.host_id
where instant_bookable = 1 group by b.host_is_superhost)c where host_is_superhost=0 or
host_is_superhost=1;



 select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as host_Superhost,
No_of_Positive_comment from(
select host_is_superhost,sum(case when
 comments like '%great%' or comments like '%nice%' or
comments like '%wonderful%' or comments like '%brilliant%'or
comments like '%great 
location%' or comments like '%good%' or comments like '%lovely%' or comments like
'%friendly%'or
comments like '%perfect%' or
comments like '%beautiful%' or comments like '%definetly stay%' or comments like
'%excellent%'
or comments like '%highly 
recommended%'
then 1 else 0 end) as
No_of_Positive_comment from (
select a.comments,c.host_is_superhost from dbo.review_rome_df a 
inner join dbo.listing_rome_df b on a.listing_id=b.id
inner join dbo.host_rome_df c on b.host_id = c.host_id)dd where host_is_superhost=0 or
host_is_superhost=1 group by host_is_superhost)ee;

select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as
host_Superhost,No_Of_property from (
select host_is_superhost , count(property_type) as No_Of_Property from (select 
a.property_type,b.host_is_superhost from dbo.listing_rome_df a inner join
dbo.host_rome_df b on a.host_id=b.host_id where a.property_type like '%entire%' or
a.property_type like '%RV%' or a.property_type like '%castle%' or a.property_type like
'%houseboat%' or a.property_type like '%boat%' or a.property_type like '%holiday 
park%')c where host_is_superhost=0 or host_is_superhost=1 group by host_is_superhost 
) cc;select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as host_Superhost,
Avg_Price from(
select host_is_superhost,avg(avg_price_host) as Avg_Price from (
SELECT DISTINCT A.HOST_ID,a.host_is_superhost,AVG(B.PRICE) OVER (PARTITION BY
A.HOST_ID) as Avg_Price_host
FROM LISTING_ROME_DF AS B
INNER JOIN HOST_ROME_DF A ON B.HOST_ID = A.HOST_ID where B.PRICE!=0)c
where host_is_superhost =0 or host_is_superhost =1 group by host_is_superhost ) ee;select case when host_is_superhost = 0 then 'Host'
 when host_is_superhost = 1 then 'SuperHost' end as host_Superhost,
Avg_Available from(
select host_is_superhost,avg(sum1) as Avg_Available,year from (select
host_is_superhost,sum(available) as sum1,month(date) as month,year(date) as year from 
(
select a.date,cast(a.available as int) as available,c.host_is_superhost from
dbo.df_rome_availability a
inner join dbo.listing_rome_df b on a.listing_id = b.id
inner join dbo.host_rome_df c on c.host_id = b.host_id )cc where host_is_superhost=1 
or host_is_superhost =0 group by host_is_superhost, month(date),year(date))ccc
group by host_is_superhost,year)cccc;select host,count(Host) as No_Of_Host from (select case when host_location like
'%rome%' then 'LocalHost'
 else 'Host' end as Host from dbo.host_rome_df)c
group by host;select host,count(host) as Number_of_Host from (select case when host_location 
like '%rome%' then 'LocalHost'
 else 'Host' end as Host,host_response_rate from dbo.host_rome_df)c where
host_response_rate > (
select avg(host_response_rate) as Avg_Response_rate from dbo.host_rome_df) group by
host;select host,count(host) as Number_of_Host from (select case when host_location like
'%rome%' then 'LocalHost'
 else 'Host' end as Host,host_acceptance_rate from dbo.host_rome_df)c 
where host_acceptance_rate > (
select avg(host_acceptance_rate) as Avg_acceptance_rate from dbo.host_rome_df) group
by host;select host,host_response_time,count(host_id) as No_Of_Host from (select case when
host_location like '%rome%' then 'LocalHost'
 else 'Host' end as Host,host_response_time, host_id from dbo.host_rome_df 
where host_response_time is not null )c group by host,host_response_time order
by host;select host,host_has_profile_pic,count(host_id) as No_Of_Host from (select case when
host_location like '%rome%' then 'LocalHost'
 else 'Host' end as Host,host_has_profile_pic,host_id from dbo.host_rome_df 
where host_has_profile_pic is not null)cc group by host,host_has_profile_pic 
order by host;select host,case when host_identity_verified = 0 then 'NO'
when host_identity_verified = 1 then 'YES'end as
host_identity_verified,count(host_id) as No_Of_Host from (select case when
host_location like '%rome%' then 'LocalHost'
 else 'Host' end as Host,host_identity_verified,host_id from
dbo.host_rome_df where host_identity_verified is not null)cc group by
host,host_identity_verified order by host;select host,count(host_id) as No_Of_Host from(select case when host_location like
'%rome%' then 'LocalHost'
 else 'Host' end as Host,host_id from dbo.host_rome_df where
host_listings_count > (
select avg(host_listings_count) as Avg_Listing_rate from dbo.host_rome_df))c group by
host;
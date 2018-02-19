Create or replace view coin.coin_rank as 
select a.id, market_cap_usd, (row_number() OVER (order by market_cap_usd desc)) as current_rank from coin.price_24h a
        Inner join 
(select max(insert_timestamp) as insert_timestamp 
                from coin.price_24h ) b
on a.insert_timestamp = b.insert_timestamp
group by 1,2
;

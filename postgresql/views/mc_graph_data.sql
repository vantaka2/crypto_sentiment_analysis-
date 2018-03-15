Create or replace view coin.mc_graph_data as
Select name, last_updated, insert_timestamp, a.market_cap_usd  
        from coin.price_24h a
        inner join coin.coin_rank b
        on a.id = b.id 
        inner join coin.dim_coin c
        on a.id = c.id
        where insert_timestamp is not null
        and current_rank <= 100
        and insert_timestamp > (now() at time zone 'utc')  - interval '8 day'
        order by insert_timestamp;
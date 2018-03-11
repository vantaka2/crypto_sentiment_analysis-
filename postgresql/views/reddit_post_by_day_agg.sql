create or replace view coin.reddit_post_by_day_agg as 
Select count(post_id) as num_posts, name, created,sentiment from (
Select a.post_id, d.name,e.sentiment, b.created:: date from coin.xref_post_to_coin a
inner join coin.dim_reddit_post b
on a.post_id = b.post_id
inner join coin.coin_rank c
on a.coin_id = c.id
inner join coin.dim_coin d
on a.coin_id = d.id
inner join coin.sentiment e
on a.post_id = e.source_id
where a.coin_id is not null
and c.current_rank <= 100
and b.created >= current_Date - Interval '7 days'
group by 1,2,3,4
)a
group by 2,3,4
;
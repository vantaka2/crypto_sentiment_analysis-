#Clean this up.... 
create or replace view coin.reddit_trends as
Select a.post_id,b.created,b.title,e.name, EXTRACT('epoch' FROM (a.update_time - b.created)):: int / 60 as diff , max(a.score) as score, max(a.num_comments) as num_comments from coin.reddit_post_trends a
inner join coin.dim_Reddit_post b
on a.post_id =  b.post_id
inner join coin.xref_post_to_coin c
on a.post_id = c.post_id 
inner join coin.coin_rank d
on c.coin_id = d.id
inner join coin.dim_coin e
on c.coin_id = e.id
inner join (select post_id from (
                Select a.post_id, score from coin.reddit_post_trends a
                        inner join 
                                (Select post_id, max(update_time) as last_call  
                                from coin.reddit_post_trends 
                                group by 1) b
                on a.post_id = b.post_id and a.update_time = b.last_call
                where score > 50
) h) f
on a.post_id = f.post_id
where current_rank <= 100
and b.created >= current_Date - Interval '7 days'
group by 1,2,3,4,5
;
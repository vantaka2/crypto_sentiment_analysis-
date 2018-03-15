Create view coin.reddit_post_info as
    Select a.post_id, a.created, a.title, b.sentiment, b.confidence, d.name as coin_name 
    from  coin.dim_reddit_post a
        Inner Join coin.sentiment b
            on a.post_id = b.source_id
        Inner Join coin.xref_post_to_coin c
            on a.post_id = c.post_id
        Inner join coin.dim_coin d
            on c.coin_id = d.id
        Inner join coin.coin_rank e
            on c.coin_id = e.id
    where a.created >= current_date - 7 
        and c.coin_id is not null
        and e.current_rank < 101
       group by 1,2,3,4,5,6;
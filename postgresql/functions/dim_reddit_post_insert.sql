CREATE OR REPLACE FUNCTION coin.dim_reddit_post_insert()

RETURNS VARCHAR

as 
 
$body$

Declare 

v_start timestamp;
v_end timestamp;
v_return_text varchar;
v_new_cnt int;

BEGIN

--Create temp table of new coins to compare

Execute $$ DROP TABLE if exists coin.stg_reddit_post_compare $$; 
Execute $$
    CREATE TABLE coin.stg_reddit_post_compare as
            Select 
                a.id, 
                a.author, 
                a.created_utc,
                a.subreddit,
                a.title,
                a.selftext 
            FROM coin.stg_reddit_post a 
    LEFT JOIN 
            coin.dim_reddit_post b
    on a.id = b.post_id 

    where b.post_id is null
$$; 


--Get count of new coins
EXECUTE $$ Select count(id) 
    FROM coin.stg_reddit_post_compare $$ INTO v_new_cnt ; 

IF v_new_cnt >= 1
    THEN 
        INSERT INTO coin.dim_reddit_post (post_id, created, subreddit, author, title, selftext)
        Select id, 
                created_utc,
                subreddit,
                author, 
                title,
                selftext 
            FROM coin.stg_reddit_post_compare ;
        
        Select string_agg(id, ',') 
            FROM coin.stg_reddit_post_compare INTO v_return_text;
ELSE
        v_return_text := 'no new coins';
END IF;

return v_return_text; 


END;
$body$

LANGUAGE 'plpgsql';



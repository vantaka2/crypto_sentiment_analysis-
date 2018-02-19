CREATE OR REPLACE FUNCTION coin.reddit_trends_insert()

RETURNS VARCHAR

as 
 
$body$

Declare 

v_start timestamp;
v_end timestamp;
v_return_text varchar;
v_new_cnt int;

BEGIN


INSERT INTO coin.reddit_post_trends (post_id, update_time, score, num_comments)
        
        Select id, max(update_time) as update_time, max(score) as score, max(num_comments) as num_comments
                
                FROM(
        Select  id, 
                current_timestamp as update_time,
                score,
                num_comments
        FROM coin.stg_reddit_post            
        
        union all 
        
        select post_id, update_time,score, num_comments
                from coin.stg_trends )l
         group by 1
                ;             

v_return_text := 'data inserted';


return v_return_text; 


END;
$body$

LANGUAGE 'plpgsql';



CREATE OR REPLACE FUNCTION coin.dim_coin_insert()

RETURNS VARCHAR

as 
 
$body$

Declare 

v_start timestamp;
v_end timestamp;
v_return_text varchar;
v_new_cnt varchar;

BEGIN

--Create temp table of new coins to compare
Execute $$
    CREATE TABLE coin.dim_coin_compare as
            Select 
                a.id, 
                a.name, 
                a.symbol 
            FROM coin.stg_coin_data a 
    LEFT JOIN 
            coin.dim_coin b
    on a.symbol = b.symbol 

    where b.symbol is null
$$; 

--Get count of new coins
EXECUTE $$ Select count(id) 
    FROM coin.dim_coin_compare $$ INTO v_new_cnt; 

IF v_new_cnt >= 1
    THEN 
        INSERT INTO coin.dim_coin (id, name, symbol)
        Select id, name, symbol 
            FROM coin.dim_coin_compare ;
        
        Select string_agg(id, ',') 
            FROM coin.dim_coin_compare INTO v_return_text;
ELSE
        v_return_text := 'no new coins';
END IF;

return v_return_text; 


END;
$body$

LANGUAGE 'plpgsql';



CREATE OR REPLACE FUNCTION coin.price_24h_insert()

RETURNS VARCHAR

as 
 
$body$

Declare 

v_start timestamp;
v_end timestamp;
v_return_text varchar;

BEGIN

--Create temp table of new coins to compare


--Get count of new coins
INSERT INTO coin.price_24h (id, price_usd, last_updated, percent_change_1h, percent_change_24h, percent_change_7d, volume_usd_24h,market_cap_usd,insert_timestamp)
Select id, price_usd, last_update, percent_change_1h, percent_change_24h, percent_change_7d, volume_usd_24h,market_cap_usd, now() at time zone 'utc'
    FROM coin.stg_coin_data
;


RETURN 'Inserted into coin_price_24h';


END;
$body$

LANGUAGE 'plpgsql';



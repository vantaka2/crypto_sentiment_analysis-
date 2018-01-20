--Schema = 'coin'
CREATE TABLE
    price_24h
    (
        id TEXT,
        price_usd FLOAT,
        last_updated TIMESTAMP(6) WITHOUT TIME ZONE,
        percent_change_1h FLOAT,
        percent_change_24h FLOAT,
        percent_change_7d FLOAT,
        24h_volume_usd FLOAT
    );

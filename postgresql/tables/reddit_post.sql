CREATE TABLE
    coin.dim_reddit_post
    (
        post_id TEXT,
        created TIMESTAMP,
        subreddit TEXT,
        author TEXT,
        title VARCHAR,
        selftext VARCHAR   
    );
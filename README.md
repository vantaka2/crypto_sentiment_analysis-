# crypto sentiment analysis

This is a personal project to gather &amp; measure sentiment from various social media / news sites and compare that to the price fluctuations of crypto currencies.

### Server information:
All the tools are being run on 2 AWS servers. 

1 m5.large & 1 t2.micro.

The m5.large is running apache airflow & is doing the data proessing.  

The t2.micro is running the most recent version of postgres

## Current state:

### Tools
Apache Airflow is being used to schedule recurring jobs to gather & manipulate data from various API's. 

The data is all stored within a postgres database. 

The web application is created using plotly Dash. 

### Data
Coinmarketcap API -  https://coinmarketcap.com/api/

Reddit Post - post's from /r/cryptocurrency. 


### Dashboard
Currently the web application is hosted on Heroku on the free teir, You can acess it at: http://crypto-sentiment-keerthan.herokuapp.com/

And the code behind the frontend application can be found at: https://github.com/vantaka2/crypto_dash_app

The current state of the web application can be seen in the gif below. 

![Alt Text](https://github.com/vantaka2/crypto_sentiment_analysis-/blob/develop/gif_1.gif)

## future:

### Tools
Kafka w/ Kafka streams for real time twitter data. 

### data
Gather News article data - https://newsapi.org/

Twitter data - twitter API

Facebook data

Instagram Data

coin specific subreddits 

### Dashboard
Add new view for a in-dept analysis of a coin. 

Add ability to input coin holdings & graph portfolio 
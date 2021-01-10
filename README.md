# Equitable Produce Marketing for Indian Farmers
Raising farmer incomes by making agricultural markets more transparent and predictable   

## Contents
**[Motivation](#Motivation)**<br>
**[Application](#Application)**<br>
**[Initial Scope](#initial-scope)**<br>
**[Usage](#Usage)**<br>

## Motivation
My family owns a [Kinnow](https://en.wikipedia.org/wiki/Kinnow) orchard in India, and we've always struggled to market the produce well. Come November, we either sell the orchard's projected output to middlemen for a lukewarm lump sum, or worse, market it ourselves. This involves frequent, fraught decisions about where and when to send the fruit, based on verbal inquiries into prices and acceptable quantities at different markets.

This haphazard, suboptimal approach is by no means an isolated phenomenon. Wholesale markets across the country are opaque, largely inaccessible, and consistently [volatile](https://thewire.in/agriculture/onion-farmers-remain-at-mercy-of-market-ups-and-downs-as-prices-crash-again). Given the uncertainty, farmers are forced to rely on a handful of [local moneylenders and middlemen](https://scroll.in/article/828159/in-punjab-farmers-angry-with-system-of-commission-agents-find-hope-in-aaps-manifesto), who are almost always in a position to dictate prices. As a result, farmers inevitably end up selling their produce for just [10-15%](https://twitter.com/MirrorNow/status/1070703842004738048) of the final sale price.

Greater transparency should help ease these inequities. Consistent, easy access to prices, quantities, and trends across markets will encourage farmers to cut through layers of middlemen and market their produce themselves. Those who can afford the transportation costs will get better prices, while those who can't will have more negotiating power. Knowing arrival patterns will inform timelines for harvesting and help reduce wastage. Any additional predictability that can be provided will go a long way in giving farmers more agency.

### Data

Data on wholesale market conditions is hard to come by. The only reliable source is the government, which publishes daily prices and arrivals on the [Agmarknet portal](http://agmarknet.gov.in/). While coverage is extensive - a number of commodities across most markets - it is also spotty. Data on Kinnows for instance, isn't available for large markets like Chandigarh and Delhi.

The portal doesn't have an API or functioning exports, so the data has to be scraped. Given how interactive the site is, a selenium scraper is needed to populate relevant fields and navigate pages. This isn't ideal, but given the state of government infrastructure, it's what we have. The script runs every evening, scrapes the data, and writes the output to a Postgres RDS instance. In order to minimize compute time and ensure reliability, it's scheduled to run on serverless infrastructure provided by AWS Lambda. 

## Application
A set of scripts to get you started with pulling this data. Ideally, we'd like to build a simple, interactive platform that provides farmers access to current conditions, trends, and projections across a range of markets and commodities.

### Services
Transparency:
- Current prices, arrivals, and price variations across markets 
- Recent price and arrival trends by region and market
- Long-term price and arrivals trends by region and market
- Data availability across states and districts
 
Predictability:
- Aggregate price levels relative to previous seasons
- Expected prices at specific markets for the coming week


## Initial Scope
Before building out the application, I want to ensure that 1) the data lines up with conditions on the ground, and 2) that this information actually leads to higher earnings for the farmer. Given the number of middlemen one still has to deal with at the wholesale markets, niether of these things is a given.

I'm currently testing a bare bones version on our farm, and things are looking encouraging on both fronts. 

The test version covers data pipelines and visualizations of market conditions; the predictive component will be added once the veracity of the data has been confirmed.


## Usage
### Setup
#### Environment
- Install dependencies:  `pip install -r requirements.txt` 
- If you're going to run the scraper within your environment, make sure [Chromedriver](http://chromedriver.chromium.org/) is installed and the scripts are pointing to its location
- If you'd rather run the scraper using AWS Lambda, I would recommend testing the code in a simulated docker environment first

#### DB
- Set up a DB. I've used [postgres](https://aws.amazon.com/getting-started/tutorials/create-connect-postgresql-db/), but feel free to use whatever you like. You just have to update the sqlalchemy engine creator in helpers.py
- Store DB credentials in `secrets.json`. Make sure these are ignored by the .gitignore. Or even better, use environment variables.
- Create DB tables by running `python tablecreator.py`

### Scraper
- If you'd rather run the scraper from a local machine or an EC2 instance 
    - Run `python scrape.py`
    - Start and end dates can be specified `python scrape.py --start 2018-12-08 --end 2018-12-10`
    - You can set up a cron job to execute the code at specified times

- If you prefer to use Lambda (recommended)
    - Excellent instructions are available [here](https://robertorocha.info/setting-up-a-selenium-web-scraper-on-aws-lambda-with-python/)


- If you'd like to pull data on other commodities or states, just update the arguments in scrape.py

### Services
Visualizations of market conditions are demonstrated in VizDemo.ipynb

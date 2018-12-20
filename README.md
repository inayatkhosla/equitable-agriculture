# Equitable Produce Marketing for Indian Farmers
Raising farmer incomes by making agricultural markets more transparent and  predictable   

## Table of Contents
**[Motivation](#Motivation)**<br>
**[Initial Scope](#initial-scope)**<br>
**[Data](#Data)**<br>
**[Functionality](#Functionality)**<br>
**[Usage](#Usage)**<br>

## Motivation
My family owns a [Kinnow](https://en.wikipedia.org/wiki/Kinnow) orchard in India, and we've always struggled to market the produce well. Come November, we either sell the orchard's projected output to middlemen for a lukewarm lump sum, or worse, market it ourselves. This involves frequent, fraught decisions about where and when to send the fruit, based on verbal inquiries into prices and acceptable quantities at different markets.

This haphazard, suboptimal approach is by no means an isolated phenomenon. Wholesale markets across the country are opaque, largely inaccessible, and consistently [volatile](https://thewire.in/agriculture/onion-farmers-remain-at-mercy-of-market-ups-and-downs-as-prices-crash-again). Given the uncertainty, farmers are forced to rely on a handful of [local moneylenders and middlemen](https://scroll.in/article/828159/in-punjab-farmers-angry-with-system-of-commission-agents-find-hope-in-aaps-manifesto), who are almost always in a position to dictate prices. As a result, farmers inevitably end up selling their produce for just [10-15%](https://twitter.com/MirrorNow/status/1070703842004738048) of the final sale price.

Greater transparency will help ease these inequities. Consistent, easy access to prices, quantities, and trends across markets will encourage farmers to cut through layers of middlemen and market their produce themselves. Those who can afford the transportation costs will get better prices, while those who can't will have more negotiating power. Knowing arrival patterns will inform timelines for harvesting and help reduce wastage. Any additional predictability that can be provided will go a long way in giving farmers more agency.

## Initial Scope
Before building out an application, I wanted to ensure that 1) the data sources line up with conditions on the ground, and 2) that this approach actually leads to higher earnings for the farmer. I'm currently testing a bare bones version on our farm, and things are looking encouraging on both fronts. 

## Data

Data on wholesale market conditions is hard to come by. The only reliable source is the government, which publishes daily prices and arrivals on the [Agmarknet portal](http://agmarknet.gov.in/). While coverage is extensive - a broad range of commodities across most markets - it is also spotty. Data on Kinnows for instance, isn't available for large markets like Chandigarh and Delhi. But things do seem to be improving.

The portal doesn't have an API, and while export functionality exists, it doesn't work, so the data has to be scraped. The site is extensively interactive - a selenium scraper is needed to populate relevant fields and navigate within and across pages. This isn't really sustainable for a proper platform, but it works for now. The script runs every evening, scrapes the data, and writes the ouptut to a Postgres RDS instance. In order to ensure reliability and minimize compute time, it's scheduled to run on serverless infrastructure provided by AWS Lambda. 

## Functionality
The following dynamic views are available:
- Data availability for prices and arrivals across states and districts
- Current prices, arrivals, and price variations across markets
- Recent price and arrival trends across regions and specific markets
- Long-term price and arrivals trends across regions and specific markets

If the data is confirmed to be reliable, predictive components will be added:
- Aggregate price levels relative to previous seasons
- Expected prices at specific markets for the coming week


## Usage
### Setup
#### Environment
- Install dependencies `pip install -r requirements.txt`. This contains some machine learning libraries that are currently extraneous, but will come in handy later  
- If you're going to run the scraper within your environment, make sure [Chromedriver](http://chromedriver.chromium.org/) is installed
- If you'd rather run the scraper using AWS Lambda, I would recommend testing the code in a simulated [docker](https://docs.docker.com/install/) environment first

#### DB
- Set up a [postgres DB](https://aws.amazon.com/getting-started/tutorials/create-connect-postgresql-db/) instance
- Store DB credentials in `secrets.json`. `db_info_example.json` is provided for reference
- Create DB tables by running `python tablecreator.py`

### Scraper
- If you'd rather run the scraper from a local machine or an EC2 instance 
    - Run `python scrape.py`
    - You can set up a cron job that executes the code at specified intervals

- If you prefer to use Lambda
    - Excellent instructions are available [here](https://robertorocha.info/setting-up-a-selenium-web-scraper-on-aws-lambda-with-python/)
    - The lambda directory contains everything you need. cd into the folder
    - I had to use a slightly different set of commands to get docker to package the code
        * `make fetch-dependencies`
        * `make docker-build`
        * `make docker-run` -  Make sure everything runs fine
        * `make build-lambda-package`
    - The last command will output a zip folder than can be uploaded to S3 for Lambda to read

### Viz
Visualizations are demonstrated in VizDemo.ipynb

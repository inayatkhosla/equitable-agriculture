import time

import scrapers as s


def lambda_handler(*args, **kwargs):
    states = ['Punjab', 'Haryana', 'Rajasthan', 'Himachal Pradesh']
    commodity = 'Kinnow'
    for state in states:
        print(state)
        try:
            mps = s.MandiPriceScraper(commodity, state)
            mps.run()
            time.sleep(5)
            mqs = s.MandiQuantityScraper(commodity, state)
            mqs.run()
            time.sleep(5)
        except Exception as e: 
            print(e)

    

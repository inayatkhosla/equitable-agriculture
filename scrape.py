import time
import argparse
parser = argparse.ArgumentParser()

from lib import scrapers as s


#parser.add_argument("--serverless", help="lambda flag",
#                    action="store_true")
parser.add_argument("--start", help="scrape start date")
parser.add_argument("--end", help="scrape end date")


states = ['Punjab', 'Haryana', 'Rajasthan', 'Himachal Pradesh']
commodity = 'Kinnow'

def main():
    args = parser.parse_args()
    for state in states:
        print(state)
        try:
            mps = s.MandiPriceScraper(commodity, state, args.start, args.end, serverless=False)
            mps.run()
            time.sleep(5)
            mqs = s.MandiQuantityScraper(commodity, state, args.start, args.end, serverless=False)
            mqs.run()
            time.sleep(5)
        except Exception as e: 
            print(e)
            #print(state + ' failed, moving on')
            #continue


if __name__ == "__main__":
    main()
    

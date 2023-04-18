import sys
from time import sleep
import logging

import database
import get_ads
from time_handling import timestamp_now, write_timestamp, read_timestamp

from settings import LOG_LEVEL, LOG_DATE_FORMAT, LOG_FORMAT, MAX_UPDATES, SLEEP_TIME_MINUTES

log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)


def keep_updated():
    last_timestamp = read_timestamp()
    counter = 0
    while True:
        new_timestamp = timestamp_now()
        log.info(f"Getting ads after timestamp '{last_timestamp}'")
        if ads := get_ads.get_ads_since_time(last_timestamp):
            database.update(ads)
        else:
            log.info(f"No ads found after timestamp '{last_timestamp}'")

        # Write timestamp when database operations are completed
        write_timestamp(new_timestamp)
        counter += 1
        log.info(f"Completed update {counter} of {MAX_UPDATES}")
        if counter == MAX_UPDATES:
            break
        log.info(f"Waiting {SLEEP_TIME_MINUTES} minutes before collecting ads again")
        sleep(SLEEP_TIME_MINUTES * 60)

    log.info('Finished')


if __name__ == '__main__':
    """
    
    Important: 
    You must run bootstrap.py first to create a database table and 
    load current ads into that table (if applicable)
    
    """
    keep_updated()

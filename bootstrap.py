import sys
import logging
import time_handling
from get_ads import get_all_ads
from database import load_all, DBConnectionHandler

from settings import LOG_LEVEL, LOG_DATE_FORMAT, LOG_FORMAT, DB_TABLE_NAME, PLACES, OCCUPATIONS, DB_FILE_NAME

log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

if __name__ == '__main__':
    """
    This is executed once to create the database and 
    load all ads into it 
    To keep the database updated, run main.py   
    """
    with DBConnectionHandler() as db:
        db.create_db_table()
    log.info(f'Database created: "{DB_FILE_NAME}"')

    if PLACES or OCCUPATIONS:
        # if you want to keep a database with a subset of all ads based on geography or occupations,
        # there is no need to download all ads, but timestamp needs to be in the past
        # so that the first update will work
        timestamp = time_handling.write_timestamp('2022-01-01T00:00:00')
    else:
        timestamp = time_handling.write_timestamp()
        all_ads = get_all_ads()
        load_all(all_ads)
        log.info(f'Loaded {len(all_ads)} into the database table "{DB_TABLE_NAME}". Timestamp: {timestamp}')

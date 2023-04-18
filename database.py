from datetime import datetime
import sys
import sqlite3
import json
import logging

from settings import LOG_LEVEL, LOG_DATE_FORMAT, LOG_FORMAT, DB_TABLE_NAME, DB_FILE_NAME

log = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=LOG_LEVEL, format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

def load_all(all_ads):
    with DBConnectionHandler() as db:
        db.insert_multiple_ads(all_ads)

def update(list_of_updated_ads):
    start = datetime.now()
    new = 0
    updated = 0
    deleted = 0
    with DBConnectionHandler() as db:
        log.info(f"Updating {len(list_of_updated_ads)} ads")
        for ad in list_of_updated_ads:
            ad_id = ad['id']
            if ad['removed']:
                db.delete_ad(ad_id)
                deleted += 1
            elif db.ad_exists(ad_id):
                db.update_ad(ad)
                updated += 1
            else:
                db.insert_one_ad(ad)
                new += 1

    log.info(
        f"{len(list_of_updated_ads)} ads processed. New: {new}, updated: {updated}, deleted: {deleted}. Time: {datetime.now() - start}")

class DBConnectionHandler:
    """
    Creates database connection
    provides CRUD methods
    Closes connection when leaving the scope of this context manager
    """

    def __init__(self):
        self.connection = sqlite3.connect(DB_FILE_NAME)
        self.cursor = self.connection.cursor()
        log.info(f"initialize connection to database '{DB_FILE_NAME}'")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        This code will always execute, even on errors (like 'finally')
        """
        self.connection.commit()
        self.count()
        self.cursor.close()
        self.connection.close()
        log.info(f"Committed transactions and closed DB connection")

    def create_db_table(self):
        # columns: id, email, city, occupation
        # TODO: When adding more fields, add new columns with their types:
        self.cursor.execute(f"CREATE TABLE {DB_TABLE_NAME} (id TEXT, email TEXT, city TEXT, occupation TEXT)")
        log.info(f"Created database table: {DB_TABLE_NAME}")

    def get_fields_from_ad(self, ad):
        ad_id = ad['id']
        email = ad.get('application_details', {}).get('email', ' ')
        city = ad.get('workplace_address', {}).get('municipality', ' ')
        occupation = ad.get('occupation', {}).get('label', ' ')
        # TODO: When adding more fields, get data from add to new variable and return it
        return ad_id, email, city, occupation

    def insert_one_ad(self, ad):
        ad_id, email, city, occupation = self.get_fields_from_ad(ad)
        # TODO: When adding more fields, 'values' must have correct number of '?'
        sql_query = f"INSERT INTO {DB_TABLE_NAME} values (?, ?, ?, ?)"
        # Use the columns defined when the database table was created
        # TODO: When adding more fields, add them in the correct order in 'payload'
        self.execute_sql(sql_query, payload=(ad_id, email, city, occupation), ad_id=ad_id)

    def insert_multiple_ads(self, list_of_ads):
        for ad in list_of_ads:
            self.insert_one_ad(ad)
        log.debug(f'Insert multiple ads: ({len(list_of_ads)} ads)')

    def update_ad(self, ad):
        # TODO: When adding more fields, add variable here
        ad_id, email, city, occupation = self.get_fields_from_ad(ad)
        if not self.ad_exists(ad_id):
            log.warning(f"Trying to update ad that is not in the database. Id: {ad_id} ")
            return
        # Use the columns defined when the database table was created
        # TODO: When adding more fields, add them here as new_column=?
        sql_query = f"UPDATE {DB_TABLE_NAME} SET email=?, city=?, occupation=? WHERE id=?"
        self.execute_sql(sql_query, payload=(email, city, occupation, ad_id), ad_id=ad_id)

    def delete_ad(self, ad_id):
        sql_query = f'DELETE FROM {DB_TABLE_NAME} WHERE id=?'
        self.execute_sql(sql_query, payload=(ad_id,), ad_id=ad_id)

    def ad_exists(self, ad_id):
        """
        If ad id is not found, it returns None
        otherwise it returns the ad as a dictionary
        """
        sql = f"SELECT city FROM {DB_TABLE_NAME} WHERE id=?"
        self.cursor.execute(sql, (ad_id,))
        if self.cursor.fetchall():
            return True
        else:
            log.debug(f"Ad with id {ad_id} was not found")
            return False

    def count(self):
        self.cursor.execute(f"SELECT count(id) FROM {DB_TABLE_NAME}")
        log.info(f"Database has {self.cursor.fetchall()[0][0]} ads")

    def execute_sql(self, sql_query: str, payload: tuple, ad_id: str):
        self.cursor.execute(sql_query, payload)
        log.debug(f"{sql_query} - Ad id: {ad_id}")

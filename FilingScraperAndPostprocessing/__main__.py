
from src.logs import Downloader
from src.utils import logger, sql_cursor, sql_connection

def main():
    try:
        Downloader().download_companies(do_save_full_document=False)
    except Exception:

        logger.exception("Error in parsing data")

    # tidy up database before closing
    sql_cursor.execute("delete from metadata where sec_cik like 'dummy%'")
    sql_connection.close()


if __name__ == '__main__':
    main()









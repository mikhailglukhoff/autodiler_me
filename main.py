import asyncio
import time

from functions import *

logging.basicConfig(filename='errors.log', level=logging.INFO, filemode='w')

iter_time_list = []
clean_time_list = []
sql_time_list = []


async def main(page_number=1):
    base_url = f"{constants.url}{{}}"
    url = base_url.format(page_number)
    logging.info('Processing page: %s', page_number)
    # print('PAGE: ', page_number)
    start = time.time()
    try:
        page = await find_ads_in_page(url)
    except asyncio.TimeoutError:
        logging.error('Timed out to fetch page %s. Retrying...', page_number)
        # print(f'Timeout occurred while fetching page {page_number}. Retrying...')
        await asyncio.sleep(5)  # Подождать 5 секунд перед повторной попыткой
        await main(page_number)  # Повторно вызвать main с текущим номером страницы
        return

    if page is None:
        logging.error('Data is None. Retrying...', page_number)
        await asyncio.sleep(5)
        await main(page_number)
        # return

    if page is False:
        logging.error('No ads on page %s. Ending...', page_number)
        return  # Просто завершить выполнение функции main

    end_parsing = time.time()
    iter_time = end_parsing - start
    iter_time_list.append(iter_time)
    avg_iter_time = sum(iter_time_list) / len(iter_time_list)

    logging.info('AVG parsing time %s', round(avg_iter_time, 2))

    clean_data = await clean_parsed_data(page)

    end_clean = time.time()
    clean_time = end_clean - end_parsing
    clean_time_list.append(clean_time)
    avg_clean_time = sum(clean_time_list) / len(clean_time_list)

    logging.info('AVG cleaning time %s', round(avg_clean_time, 3))

    await upload_to_psql(clean_data)

    end_sql = time.time()
    sql_time = end_sql - end_clean
    sql_time_list.append(sql_time)
    avg_sql_time = sum(sql_time_list) / len(sql_time_list)

    logging.info('AVG PostgreSQL time %s', round(avg_sql_time, 2))

    next_page_number = page_number + 1
    await main(next_page_number)


asyncio.run(main())

total_time = sum(iter_time_list) + sum(clean_time_list) + sum(sql_time_list)
logging.info('Scraping process completed successfully in, sec: %s', round(total_time, 2))
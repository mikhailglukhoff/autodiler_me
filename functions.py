import asyncio
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import constants
import queries


async def is_ad_available(soup):
    breadcrumbs = soup.find('div', class_=constants.is_available_class_name)
    if breadcrumbs:
        return False
    return True


async def find_ads_in_page(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            page = []
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            if response.status == 200:
                if await is_ad_available(soup):
                    try:
                        for link in soup.find_all('div', constants.class_name):
                            id_info = link.find_all('a', class_=constants.header_info_class_name)
                            for i in id_info:
                                href = i.get('href')
                            is_payed_info = link.find_all('div', class_=constants.payed_info_class_name)
                            header_info = link.find_all('a', class_=constants.header_info_class_name)
                            extra_info = link.find_all('span', class_=constants.extra_info_class_name)
                            price_info = link.find_all('div', class_=constants.price_info_class_name)
                            location_info = link.find_all('div', class_=constants.location_info_class_name)
                            date_info = link.find_all('div', class_=constants.date_info_class_name)

                            id_ad = [href]
                            payed_ad = [payed.text for payed in is_payed_info]
                            head_ad = [head.text for head in header_info]
                            extra_ad = [extra.text for extra in extra_info]
                            price_ad = [price.text for price in price_info]
                            location_ad = [location.text for location in location_info]
                            date_ad = [date.text for date in date_info]

                            ad_info = id_ad + payed_ad + head_ad + extra_ad + price_ad + location_ad + date_ad
                            page.append(ad_info)
                        return page
                    except ValueError as e:
                        logging.error('An error occurred while getting an url %s: %s', constants.url, e)
                else:
                    return False


async def clean_parsed_data(page):
    clean_data = []
    if page is not None:
        for item in page:
            try:
                ad_dict = {}

                # clean id_ad
                split_unique_id = item[0].split('-')
                unique_id = split_unique_id[-1]
                ad_dict.update({constants.psql_data['column_names'][0]: unique_id})

                # detect paied ads
                is_payed = item[1] == constants.is_payed
                ad_dict.update({constants.psql_data['column_names'][1]: is_payed})

                # split ad info
                parts_of_head_info = [None, None, None]
                split_info = item[2].split(' - ', 2)
                for i, info in enumerate(split_info):
                    parts_of_head_info[i] = info
                ad_dict.update({
                    constants.psql_data['column_names'][2]: parts_of_head_info[0],
                    constants.psql_data['column_names'][3]: parts_of_head_info[1],
                    constants.psql_data['column_names'][4]: parts_of_head_info[2]
                })
                # get mileage
                mileage = ''.join(filter(str.isdigit, item[3]))
                if mileage and mileage != '':  # Проверяем, что строка не пустая
                    try:
                        mileage = int(mileage)
                        ad_dict.update({constants.psql_data['column_names'][5]: mileage})
                    except ValueError:
                        ad_dict.update({constants.psql_data['column_names'][5]: None})
                else:
                    ad_dict.update({constants.psql_data['column_names'][5]: None})

                # get year of manufacture
                year = int(''.join(filter(str.isdigit, item[4])))
                ad_dict.update({constants.psql_data['column_names'][6]: year})

                # get fuel type
                fuel = str(item[5].replace(':', '').strip())
                ad_dict.update({constants.psql_data['column_names'][7]: fuel})

                # get price
                price = ''.join(filter(str.isdigit, item[6]))
                if price and price != '':
                    try:
                        price = int(price)
                        ad_dict.update({constants.psql_data['column_names'][8]: price})
                    except ValueError:
                        ad_dict.update({constants.psql_data['column_names'][8]: None})
                else:
                    ad_dict.update({constants.psql_data['column_names'][8]: None})

                # get location
                location = str(item[7])
                ad_dict.update({constants.psql_data['column_names'][9]: location})

                # get publication date
                date = str(item[8])
                date_obj = None
                try:
                    date_obj = datetime.strptime(date, "%d.%m.%y")
                except ValueError:
                    date_split = date.split(' ', 2)
                    for mapping in constants.date_split_values:
                        for key, values in mapping.items():
                            if date_split[2] in values:
                                if key == 'today':
                                    date_obj = datetime.now()
                                elif key == 'yesterday':
                                    date_obj = datetime.now() - timedelta(days=1)
                                elif key == 'few days before':
                                    days_before = int(date_split[1])
                                    date_obj = datetime.now() - timedelta(days=days_before)

                date_obj_format = date_obj.strftime("%Y-%m-%d")
                date_formatted = datetime.strptime(date_obj_format, "%Y-%m-%d")
                ad_dict.update({constants.psql_data['column_names'][10]: date_formatted})

                clean_data.append(ad_dict)
            except ValueError as e:
                logging.error('ValueError in item %s: %s', item, e)
            except AttributeError as e:
                logging.error('Atrribute error in item %s: %s', item, e)
        return clean_data
    else:
        return None


async def upload_to_psql(clean_data):
    conn = None
    logging.info('Connecting to the database...')
    load_dotenv()

    connection_string = (
        f"postgresql://{os.getenv('POSTGRESQL_USER')}:{quote_plus(os.getenv('POSTGRESQL_PASSWORD'))}@"
        f"{os.getenv('POSTGRESQL_HOST')}:{os.getenv('POSTGRESQL_PORT')}/{os.getenv('POSTGRESQL_DBNAME')}"
    )
    try:
        conn = await asyncpg.connect(connection_string, timeout=5)
        await conn.executemany(
            queries.upload_query,
            [(ad[constants.psql_data['column_names'][0]],
              ad[constants.psql_data['column_names'][1]],
              ad[constants.psql_data['column_names'][2]],
              ad[constants.psql_data['column_names'][3]],
              ad[constants.psql_data['column_names'][4]],
              ad[constants.psql_data['column_names'][5]],
              ad[constants.psql_data['column_names'][6]],
              ad[constants.psql_data['column_names'][7]],
              ad[constants.psql_data['column_names'][8]],
              ad[constants.psql_data['column_names'][9]],
              ad[constants.psql_data['column_names'][10]])
             for ad in clean_data]
        )
        logging.info('Data uploaded successfully.')
    except asyncio.exceptions.TimeoutError as e:
        logging.error('Connection to database is timed out: %s', e)
    except asyncpg.PostgresError as e:
        logging.error('PSQL ERROR: %s', e)
    finally:
        if conn is not None:
            await conn.close()
            logging.info('Database connection closed.')

url = 'https://autodiler.me/automobili/pretraga?brandsText=&citiesText=&sortBy=dateDesc&formStyle=basic&pageNumber='

is_available_class_name = 'oglasi-content-text oglasi-content-list_no-ads'
class_name = 'oglasi-item-tekst oglasi-item-tekst-automobili'
payed_info_class_name = 'ad-status'
header_info_class_name = 'oglasi-item-heading'
extra_info_class_name = 'oglasi-item-description_spec-value'
price_info_class_name = 'cena'
location_info_class_name = 'oglasi-mesto'
date_info_class_name = 'oglasi-vreme'

is_payed = 'PLAĆEN OGLAS'

date_split_values = [
    {'today': ('sek', 'min', 'h')},
    {'yesterday': 'dan'},
    {'few days before': 'dana'}
]

psql_data = {
    'table_name': 'auto_data',
    'column_names': (
        'unique_id',
        'is_payed',
        'brand',
        'model',
        'info',
        'mileage',
        'year',
        'fuel',
        'price',
        'location',
        'date'
    )
}

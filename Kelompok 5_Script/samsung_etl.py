import pandas as pd
import numpy as np
import requests

def scrape_produk_samsung():

    url = 'https://shopee.co.id/api/v4/shop/rcmd_items?bundle=shop_page_category_tab_main&limit=100&offset=0&shop_id=52635036&sort_type=1&upstream='
    response = requests.get(url).json()

    names = []
    prices = []
    currencies = []
    stocks = []
    rating_stars = []

    for item in response['data']['items']:
        names.append(item['name'])
        prices.append(item['price'])
        currencies.append(item['currency'])
        stocks.append(item['stock'])
        rating_stars.append(item['item_rating']['rating_star'])

    data = {
        'nama': names,
        'harga': prices,
        'currency': currencies,
        'stock': stocks,
        'rating': rating_stars
    }
    df = pd.DataFrame(data)

    return df

def Data_Quality(load_df):
    if load_df.empty:
        print('No Data Extracted')
        return False
     
    if load_df.isnull().values.any():
        raise Exception("Null values found")

def Transform_df(load_df):
    transformed_df = load_df.copy()
    transformed_df['rating'] = transformed_df['rating'].round(2)
    
    return transformed_df[['nama', 'harga', 'currency', 'stock', 'rating']]

def samsung_etl():
    load_df=scrape_produk_samsung()
    Data_Quality(load_df)
    Transformed_df=Transform_df(load_df)    
    print(Transformed_df)
    return Transformed_df

samsung_etl()
   
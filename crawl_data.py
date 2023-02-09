import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from pyspark.sql import SparkSession


HEADERS ={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"}

def crawl_source(url):
    res = requests.get(url, headers=HEADERS)
    print(res)
    content_html = BeautifulSoup(res.content, "html.parser")
    return content_html

ten = []
danh_gia = []
gia = []
da_ban = []
mien_ship = []

def save_data_to_csv(html_src):
    for book in html_src.findAll('a', {'class': 'product-item'}):
        name = book.select_one(".name > h3").get_text()
        rate = str(book.select_one(".total > span")).replace("</span>", "").replace("<span>", "")
        sell = str(book.select_one(".fCfYNm")).replace('<div class="styles__StyledQtySold-sc-732h27-2 fCfYNm">', "").replace("</div>", "")
        price = book.select_one(".price-discount__price").get_text().replace(".", "")
        ship = book.select_one(".badge-under-rating").get_text()

        ten.append(name)
        danh_gia.append(rate)
        da_ban.append(sell)
        if ship != "":
            mien_ship.append("yes")
        else:
            mien_ship.append("no")
        gia.append(price)

url = 'https://tiki.vn/nha-sach-tiki/c8322?page='

i = 2

while (i <= 50):
    src = crawl_source(url + str(i))
    save_data_to_csv(src)
    i += 1

dict_data = { 
        'Ten': ten,
        'Đánh giá': danh_gia, 
        'Giá': gia,
        'Đã bán': da_ban,
        'Miễn ship': mien_ship
}

df = pd.DataFrame(dict_data)
print(dict_data)
df.to_csv("Tiki.csv")
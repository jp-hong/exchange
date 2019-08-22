
import requests
import pandas as pd
from fake_useragent import UserAgent
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def daum_exchanges():
    
    headers = {
        "User-Agent": UserAgent().chrome,
        "Referer": "https://finance.daum.net/exchanges",
    }

    url = "https://finance.daum.net/api/exchanges/summaries"

    r = requests.get(url, headers=headers)
    data = r.json()["data"]

    df_data = [
        {
            "country": item["country"],
            "currencyName": item["currencyName"],
            "basePrice": item["basePrice"],
            "changePrice": item["changePrice"],
            "cashBuyingPrice": item["cashBuyingPrice"],
            "cashSellingPrice": item["cashSellingPrice"]
        } for item in data
    ]

    df = pd.DataFrame(df_data)
    return df


base = declarative_base()

class ExchangeDaum(base):
    __tablename__ = "daum"

    ID = Column(Integer, primary_key=True)
    COUNTRY = Column(String(16), nullable=False)
    CURRENCY_NAME = Column(String(16), nullable=False)
    BASE_PRICE = Column(Float, nullable=False)
    CHANGE_PRICE = Column(Float, nullable=False)
    CASH_BUYING_PRICE = Column(Float, nullable=False)
    CASH_SELLING_PRICE = Column(Float, nullable=False)

    def __init__(self, country, cname, bprice, cprice, cbprice, csprice):
        self.COUNTRY = country
        self.CURRENCY_NAME = cname
        self.BASE_PRICE = bprice
        self.CHANGE_PRICE = cprice
        self.CASH_BUYING_PRICE = cbprice
        self.CASH_SELLING_PRICE = csprice
                     
    def __repr__(self):
        return "<ExchangeDaum {}, {}>".format(self.country, self.cname)  

    
class SaveDatabase:

    def __init__(self, base, df, ip="15.164.3.132", pw="0000", database="exchange"):
        self.mysql_client = create_engine(
            "mysql://root:{}@{}/{}?charset=utf8".format(pw, ip, database)
        )
        self.base = base
        self.df = df
        
    def mysql_save(self):
        self.base.metadata.create_all(self.mysql_client)
        
        data = [
            ExchangeDaum(
                self.df.country.iloc[i],
                self.df.currencyName.iloc[i],
                self.df.basePrice.iloc[i],
                self.df.changePrice.iloc[i],
                self.df.cashBuyingPrice.iloc[i],
                self.df.cashSellingPrice.iloc[i],
            ) for i in range(len(self.df))
        ]

        maker = sessionmaker(bind=self.mysql_client)
        session = maker()
        session.add_all(data)
        session.commit()
        session.close()

        
df = daum_exchanges()
sd = SaveDatabase(base, df)
sd.mysql_save()
print("saved!")

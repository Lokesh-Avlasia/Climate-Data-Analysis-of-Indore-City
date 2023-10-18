import requests
from bs4 import BeautifulSoup
import pandas as pd
import matplotlib.pyplot as plt
import statistics as st
import random 
import threading
import numpy as np

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 5.0; SM-G920A) AppleWebKit (KHTML, like Gecko) Chrome Mobile Safari (compatible; AdsBot-Google-Mobile; +http://www.google.com/mobile/adsbot.html)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 (compatible; AdsBot-Google-Mobile; +http://www.google.com/mobile/adsbot.html)",
    "AdsBot-Google (+http://www.google.com/adsbot.html)",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Desktop agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.118 Safari/537.36 (compatible; Google-Read-Aloud; +https://developers.google.com/search/docs/advanced/crawling/overview-google-crawlers)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36 Google Favicon",
    "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko; googleweblight) Chrome/38.0.1025.166 Mobile Safari/535.19"
]

class data_collection:

    def scraper(self,month,year):
        """
            This function is responsible to scrape the data from website and 
            store data of the month (eg. January) with year (eg. 2022) to 'monthly_data_list'
        """

        url = f"https://en.tutiempo.net/climate/{month}-{year}/ws-427540.html"

        user_agent = random.choice(USER_AGENTS)

        headers = {
            'User-Agent': user_agent
            }

        req = requests.get(
                    url,
                    headers = headers
                    )

        soup = BeautifulSoup(
                    req.text, 
                    "html.parser")

        print(soup.select("h3",class_="bluecab")[0].text)

        month_name = soup.select("h3",class_="bluecab")[0].text.split(" ")[-2]
        print(month_name)
        month_name_list = [ "January",
                            "February",
                            "March",
                            "April",
                            "May",
                            "June",
                            "July",
                            "August",
                            "September",
                            "October",
                            "November",
                            "December"]

        month_num = month_name_list.index(month_name) + 1
        year_num = soup.select("h3",class_="bluecab")[0].text.split(" ")[-1]

        all=[]

        for row in (soup.select('table')[3]).select("tr"):
            row1 = []
            if(len(row.text)>10):
                for i in row.select("td"):
                    row1.append(i.text)
                all.append(row1)


            df = pd.DataFrame(all[1:])
            
        dict_ = {'date':df[0],
                 'T':df[1] ,
                 'TM':df[2],
                 'Tm':df[3],
                 'SLP':df[4],
                 'H':df[5],
                 'PP':df[6],
                 'VV':df[7],
                 'V':df[8],
                 'VM':df[9],
                 'Month':month_num,
                 'years':year_num}
        
        df = pd.DataFrame(dict_)
        self.monthly_data_list.append(df.iloc[:-2,:])

        
    def fetch_monthly_data(self):
        """
            This function is responsible for implementing multithreading in web scraping across various months and years, 
            managing data in 'monthly_data_list'.
        """

        self.monthly_data_list = []
        threads = []
        
        years = [str(x) for x in range(1973,2021 + 1,1)]
        months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for year in years:
            for month in months:
                thread = threading.Thread(target=self.scraper, args=(month, year))
                thread.start()
                threads.append(thread)

        for thread in threads:
            thread.join()
    
    def clean_data(self):
        """
            This function is responsible for cleaning the data, handling missing values, and storing the result in 'monthly_cleaned_data_list'
        """

        self.monthly_cleaned_data_list = []
        for sample_data in self.monthly_data_list:
            sample_df = sample_data.replace("-",np.nan,regex = True)
            sample_df = sample_df.replace("\xa0",np.nan,regex = True)
            sample_df.iloc[:,1:-2] = sample_df.iloc[:,1:-2].astype(float)
            sample_df.iloc[:,[0,-1,-2]] = sample_df.iloc[:,[0,-1,-2]].astype(int)
            sample_df = sample_df.fillna(sample_df.fillna(0).mean().round(2))

            self.monthly_cleaned_data_list.append(sample_df)

    def reset_index(self,data):    
        """
            This function is responsible to reset the indices of resultant dataframe
        """

        index = pd.Index(list(range(0, len(data.index),1)))
        data = data.set_index(index)
        return data

    def combine_df(self):
        """
            This function is responsible to combine all the dataframes stored in 'monthly_cleaned_data_list'
        """

        all_years = pd.DataFrame()
        
        for month_data in self.monthly_cleaned_data_list:
            all_years = pd.concat([all_years,month_data])
            
        all_years = self.reset_index(all_years)
        return all_years


if __name__ == "__main__":
    obj = data_collection()
    obj.fetch_monthly_data()   
    obj.clean_data()
    all_years_ = obj.combine_df()

    all_years_.to_csv("all_years.csv")


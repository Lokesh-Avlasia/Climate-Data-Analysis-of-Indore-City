import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import matplotlib.pyplot as plt
import statistics as st

class data_collection:



    def scrapper(self,M,Y):
        url= "https://en.tutiempo.net/climate/"+ M +"-"+ Y +"/ws-427540.html"

        req = requests.get(url)

        soup = BeautifulSoup(req.text, "html.parser")

        print(soup.select("h3",class_="bluecab")[0].text)

        all=[]

        for row in (soup.select('table')[3]).select("tr"):
            row1 = []
            if(len(row.text)>10):
                for i in row.select("td"):
                    row1.append(i.text)
                all.append(row1)


            df = pd.DataFrame(all[1:])


        dict = {'date':df[0],'T':df[1] ,'TM':df[2],'Tm':df[3],'SLP':df[4],'H':df[5],'PP':df[6],'VV':df[7],'V':df[8],'VM':df[9]}
        df = pd.DataFrame(dict)
        
        return df

    def scrapper(self,M,Y):
        url= "https://en.tutiempo.net/climate/"+ M +"-"+ Y +"/ws-427540.html"

        req = requests.get(url)

        soup = BeautifulSoup(req.text, "html.parser")

        print(soup.select("h3",class_="bluecab")[0].text)

        all=[]

        for row in (soup.select('table')[3]).select("tr"):
            row1 = []
            if(len(row.text)>10):
                for i in row.select("td"):
                    row1.append(i.text)
                all.append(row1)


            df = pd.DataFrame(all[1:])


        dict = {'date':df[0],'T':df[1] ,'TM':df[2],'Tm':df[3],'SLP':df[4],'H':df[5],'PP':df[6],'VV':df[7],'V':df[8],'VM':df[9]}
        df = pd.DataFrame(dict)
        
        return df

    def data_cleaner(self,df):
        df.dropna(how='all',inplace = True)

        index = pd.Index(list(range(0, len(df.index),1)))
        df = df.set_index(index)
        

        missing_Val = []
        

        for r in df.index:
            if(df.iloc[r,0] == '\xa0' ):
                missing_Val.append(r)
        df.drop(missing_Val,inplace=True)       
        # missing_Val

        for c in range(len(df.columns)):

            for r in range(len(df.index)):
                if(df.iloc[r,c] == '-' ):
                    temp = []
                    for i in df.iloc[:,c]:
                        try:
                            temp.append(float(i))
                        except:
                            pass
                    df.iloc[r,c] = st.mean(temp)
                    
        df.iloc[:,1:] = df.iloc[:,1:].astype(float)
        df.iloc[:,0] = df.iloc[:,0].astype(int)
        
        return df


    def add_month(self,final):
        
        month = []
        a=0
        for i in range(len(final.index)):
            if (final.iloc[i,0] == 1):
                a=a+1
            month.append(a)

        final['Month'] = month
        
        return final

    def reset_index(self,data):    
        index = pd.Index(list(range(0, len(data.index),1)))
        data = data.set_index(index)
        return data


    def all_month_in_Year(self,Y):

        final = pd.DataFrame()
        months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for M in months:
            df = self.scrapper(M,Y)
            df = self.data_cleaner(df)
            final = pd.concat([final,df],axis=0)

        final = self.reset_index(final)

        final = self.add_month(final)

        return final

    def add_years(self,all_years):
        yr_list = [] 
        yr = 1972
        for i in range(len(all_years.index)):
            if all_years.index[i] == 0:
                yr = yr+1
            yr_list.append(yr)

        all_years['years'] = yr_list 
        
        return all_years


if __name__== "__main__":
    all_years = pd.DataFrame()
    years = [str(x) for x in range(1973,2021 + 1,1)]

    for year in years:
        all_years = pd.concat([all_years,data_collection().all_month_in_Year(year)],axis=0)
        
    all_years = data_collection().add_years(all_years)
    all_years = data_collection().reset_index(all_years)

    all_years.to_csv("all_years.csv")


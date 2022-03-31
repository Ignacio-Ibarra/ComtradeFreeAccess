import pandas as pd
import requests
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from tqdm import tqdm
import time
import random
from datetime import datetime
import datetime
from dateutil.rrule import rrule, MONTHLY


# params = {'max':'100000',       #default
#           'type': 'C',        #C = commodities; S= services
#           'freq': 'M',    #A = annual; M = monthly
#           'px'  : 'HS',          #Classification  codes HS is "as reported"
#           'ps'  : '201712',
#           'r'   : '156',
#           'p'   : '0',        #p partner area (default = all): partner area. The area receiving the trade, based on the reporting areas data. 0 is "World"
#           'rg'  : '2',       #rg trade flow (default = all): The most common area 1 (imports) and 2 (exports)
#           'cc'  : 'AG6',    #cc classification code (default = AG2): a commodity code valid in the selected classification.
#           'uitoken': 'dc4cef183726fde0e7fee58baeaca3ae', #guest user
#           'fmt':'json'     #fmt format: csv or json (default)
#           'max' : '100000'
#                      }

class ComtradeFreeAccess:

    def __init__(self):
        self.__basic_url = "https://comtrade.un.org/api/get?"


    def make_url(self, param_dict):
        url_parse = urlparse(self.__basic_url)
        query = url_parse.query
        url_dict = dict(parse_qsl(query))
        url_dict.update(param_dict)
        url_new_query = urlencode(url_dict)
        url_parse = url_parse._replace(query=url_new_query)
        new_url = urlunparse(url_parse)
        return new_url

    def make_request(self, param_dict):
        req_url = self.make_url(param_dict)
        my_headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
                      "Accept":"text/html,application/xhtml+xml,application/xml; q=0.9,image/webp,image/apng,/;q=0.8"}
        js = requests.get(url=req_url, headers=my_headers).json()
        jsvalidation = js['validation']['status']
        print(param_dict,"\n")
        if jsvalidation['value']== 0:
            return js
        elif jsvalidation['value']==5003:
            data = pd.DataFrame()
            years = param_dict['ps'].split(",")
            for year in param_dict['ps'].split(","):
                params_temp = param_dict
                params_temp['ps'] = str(year)
                req_url = self.make_url(params_temp)
                js = requests.get(url=req_url, headers=my_headers).json()
                data = data.append(pd.DataFrame(js['dataset']), ignore_index=True)
            return data
        else:
            print(f"ERROR: Validation is {jsvalidation['name']}")

    def download_comtrade(self, params_dict):
        js = self.make_request(params_dict)
        if isinstance(js, pd.DataFrame):
            return js
        else:      
            return pd.DataFrame(js['dataset'])


    
    def last_period_data(self, pos_list, frequency="A", period=None, flow=1):
        data = pd.DataFrame()
        chunk_size = 20  #minimize request sending
        # print(len(pos_list))
        for i in tqdm(range(0, len(pos_list), chunk_size)):
            q = pos_list[i:i+chunk_size]
            params = {
                  'max':'100000',       #default
                  'type': 'C',        #C = commodities; S= services
                  'freq': frequency,    #A = annual; M = monthly
                  'px'  : 'HS',          #Classification  codes HS is "as reported"
                  'ps'  : period,
                  'r'   : 'all',
                  'p'   : '0',        #p partner area (default = all): partner area. The area receiving the trade, based on the reporting areas data. 0 is "World"
                  'rg'  : flow,       #rg trade flow (default = all): The most common area 1 (imports) and 2 (exports)
                  'cc'  : ",".join(q),    #cc classification code (default = AG2): a commodity code valid in the selected classification.
                  'fmt':'json'     #fmt format: csv or json (default)
                    }
            data = data.append(self.download_comtrade(params), ignore_index=True)
            return data

    def define_year_param(yearlist):    #loy refers to list o years - used in params_dict
        return ",".join([str(x) for x in yearlist])

    def list_of_years(self, start_date, end_date):
        if end_date == None: 
            end_date = datetime.now().year
        if start_date and end_date: 
            return list(range(start_date,end_date+1,1))
    
    
    def list_of_months(self, start_date=None, end_date=None):
        if start_date == None: 
            start_date = str(datetime.now().year)+str(datetime.now().month).zfill(2)
        if end_date == None: 
            end_date = str(datetime.now().year)+str(datetime.now().month).zfill(2)
        if start_date and end_date:
            start_year, start_month, end_year, end_month = start_date[:4], start_date[4:], end_date[:4], end_date[4:]
            datetimelist = list(rrule.rrule(rrule.MONTHLY, dtstart=date(int(start_year.lstrip('0')), int(start_month.lstrip('0')), 1), until=date(int(end_year.lstrip('0')), int(end_month.lstrip('0')), 1))) 
            return [str(x.year)+str(x.month).zfill(2) for x in datetimelist]

    def grouping(self, datelist): 
        if len(datelist)<=5: 
            return [",".join(datelist)]
        else: 
            return [",".join([str(x) for x in datelist[i:i+5]]) for i in range(0,len(datelist),5)]


#     def estimate_potential_rows(self, poslist, frequency="A", start_date=None, end_date=None, flow=1):
#         npos = len(poslist)
#         if frequency == "A":
#             dates = self.list_of_years(start_date, end_date)
#             ndates = len(dates)
#             period = dates[-1]
#             print(f"Estimating rows in period {period}\n")
#             lastpdata = self.last_period_data(poslist, frequency=frequency, period=period, flow=flow)
#             ncountries = lastpdata.rt3ISO.nunique()
#             if flow == "all": 
#                 nflow = 4
#             else:
#                 nflow = 1
#             potential_rows = npos*ndates*ncountries*nflow
#             print(f"Estimated rows: {potential_rows}\n")
#             return lastpdata
#         elif frequency == "M":
#             dates = self.list_of_months(start_date, end_date)
#             ndates = len(dates)
#             period = dates[-1]
#             lastpdata = self.last_period_data(poslist, frequency=frequency, period=period, flow=flow)
#             ncountries = lastpdata.rt3ISO.nunique()
#             if flow == "all": 
#                 nflow = 4
#             else:
#                 nflow = 1
#             potential_rows = npos*ndates*ncountries*nflow
#             print(f"Estimated rows: {potential_rows}\n")
#             return lastpdata
#         else:
#             print("Define a correct frequency")
#         #print(f"Getting number of countries involved in this trade\n")
        
        


#         #MAXROWS = 100000
#         #MAXREQH = 100



#     def bulk_download(ptype="C", frequency="A", classif="HS"): 
    
#     #C = commodities; S= services
#     #A = annual; M = monthly
#     #classif: HS is "as reported"
#         if ptype=="C":
#             pty="COMMODITIES"
#         else:
#             pty="SERVICES"
        
#         if frequency=="A":
#             freque="ANNUAL"
#         else:
#             freque="MONTHLY"
        
#         """ availability """
#         print("Loading data availability...\n") 
#         availability = pd.read_csv("https://comtrade.un.org/api/refs/da/view?fmt=csv", dtype={'r':'string'})
#         availability = availability[(availability.type==pty) & (availability.freq==freque) & (availability.px==classif)].reset_index(drop=True)
#         Avail_rCodes = [x for x in availability.r.unique()]
#         print("Done\n")
        
#         """ countries_id """
#         print("Loading countries list...\n") 
#         countries_js = requests.get(url = "http://comtrade.un.org/data/cache/partnerAreas.json").json()['results']
#         countries_metadata = pd.DataFrame(countries_js)
#         countries_metadata['id'] = countries_metadata['id'].astype('str')
#         countries_metadata = countries_metadata[countries_metadata.id.isin(Avail_rCodes)].reset_index(drop=True)
#         countries_tuples = [(id,text) for id,text in zip(countries_metadata.id, countries_metadata.text.to_list())]
#         countries_tuples = [t for t in countries_tuples if 'nes' not in t[1] and t[0] not in ['all','0','841']]
#         print("Done\n")
        
           
#         all_data = pd.DataFrame()
#         for key, text in tqdm(countries_tuples): 
#             df = pd.DataFrame()
#             start = int(availability[availability.r==key].ps.min())
#             if start<2000:
#                 start = 2000
#             end = int(availability[availability.r==key].ps.max())
#             print("Downloading {} data from {} to {}".format(text, start, end))
#             loy = list_of_years(start,end)
#             year_groups = grouping(loy) 
            
#             for g in year_groups: 
#                 params = {'max':'100000',       #default
#                           'type': ptype,        #C = commodities; S= services
#                           'freq': frequency,    #A = annual; M = monthly
#                           'px': classif,          #Classification  codes HS is "as reported"
#                           'ps': g,
#                           'r': str(key),
#                           'p': '0',        #p partner area (default = all): partner area. The area receiving the trade, based on the reporting areas data. 0 is "World"
#                           'rg': '2',       #rg trade flow (default = all): The most common area 1 (imports) and 2 (exports)
#                           'cc' : 'AG6',    #cc classification code (default = AG2): a commodity code valid in the selected classification.
#                           'uitoken': 'dc4cef183726fde0e7fee58baeaca3ae', #guest user
#                           'fmt':'json'     #fmt format: csv or json (default)
#                          }
#                 data = download_comtrade(params)['dataset']
#                 df = df.append(data, ignore_index=False)
                
#                 time.sleep(round(random.uniform(1,2), 3)) #random pause
                
#             df.to_excel('exports_{}_{}-{}.xlsx'.format(text,start,end), index=False)
#             print("file saved to exports_{}_{}-{}.xlsx".format(text,start,end))
#             all_data = all_data.append(df, ignore_index=False)
#         all_data.to_csv('all_data_comtrade.csv', index=False)
#         print("finished successfully")
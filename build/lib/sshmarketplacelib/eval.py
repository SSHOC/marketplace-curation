'''
Created on Nov 15, 2021

@author: cesare
'''
import pandas as pd
import requests
import re
import pickle
import os.path
import os
import json
import numpy as np
import yaml
import multiprocessing
from multiprocessing.pool import Pool
import errno

from . import mpdata as mpd

class URLCheck(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        if (os.path.isdir('data/')):
            self.datadir='data/';
        else:
            if (os.path.isdir('../data/')):
                self.datadir='../data';
            else:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), "data")
        
        
        if (os.path.isfile('config.yaml')):
            self.configfile="config.yaml"
        else:
            if (os.path.isfile('../config.yaml')):
                self.configfile="../config.yaml"
            else:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), "config.yaml")
        
        try:
            with open(self.configfile, 'r') as stream:
                try:
                    self.conf=yaml.safe_load(stream)
                except yaml.YAMLError as exc:
                    print(exc)
        except FileNotFoundError:
            print('Warning config.yaml file not present! Please create it and set the values, store it in the main directory')
        self.MPAPIserver=self.conf['API']['SERVER']
        self.MPserver=self.conf['MARKETPLACE']['SERVER']
        self.server=self.conf['MARKETPLACE']['SERVER']
        self.userId=self.conf['API']['USER']
        self.passW=self.conf['API']['PASSWORD']
        self.allCategories=self.conf['CATEGORIES']
        self.dataset_entrypoints=self.conf['DATASET_ENTRYPOINTS']
        for key, value in self.dataset_entrypoints.items():   
            self.dataset_entrypoints[key] = self.MPAPIserver+value
        self.empty_description=self.conf['EMPTY_DESCRIPTION_VAL']
        self.list_of_properties_url=self.conf['DATASET_ENTRYPOINTS']['list_of_properties']
        self.url_all_properties=['accessibleAt','terms-of-use-url', 'access-policy-url', 'privacy-policy-url', 'see-also', 'user-manual-url', 'service-level-url', 'thumbnail']
        self.url_dynamic_properties=['terms-of-use-url', 'access-policy-url', 'privacy-policy-url', 'see-also', 'user-manual-url', 'service-level-url', 'thumbnail']

        self.mpdata = mpd.MPData()
        
    
    def getHTTP_Status(self, var):
        df_tool_work_aa_http_status = []
        regex = re.compile(
            r'^(?:http|ftp)s?://' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
            r'localhost|' #localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
            r'(?::\d+)?' # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if ( (var is not None) and var != "" and re.match(regex, var)):
            try:
                r =requests.get(var,timeout=3)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(r.status_code)})
            except requests.exceptions.ConnectionError:
                #  print(var)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(503)})
            except requests.exceptions.ConnectTimeout:
                #  print(var)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(408)})
            except requests.exceptions.ReadTimeout:
                #  print(var)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(408)})
            except requests.exceptions.RequestException:
                #   print(var)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(500)})
            except TypeError:
                #    print(var)
                df_tool_work_aa_http_status.append({'url': var, 'status': int(400)})
        else:
            # print(var ,0)
            df_tool_work_aa_http_status = df_tool_work_aa_http_status.append({'url': var, 'status': int(400)})
        return (df_tool_work_aa_http_status)
    
    
    
    def checkURLValues(self, itemcategories, props=''):
        
        """
        
        Check HTTP Status for all URLs in the given categories and returns a dataframe with results.
        
        Parameters:
        -----------
        
        itemcategories : String
            The categories to be checked 
        props : String
            The properties to be checked
        
        
        """
        
        properties=[]
        #df_urls=[]
        dfs=[]
        pool = Pool()
        cores=multiprocessing.cpu_count()
        if props.strip()!='':
            properties=props.replace(" ", "").split(',')
        else:
            properties=self.url_all_properties
        if itemcategories.strip()!='':
            categories=itemcategories.replace(" ", "").split(',')
            for ca in categories:
                if ca.strip() not in self.allCategories:
                    print ('Wrong Category: '+ca)
        else:
            if itemcategories.strip()=='all':
                categories=self.allCategories
            else:
                print ('No category defined!')
                return
        for cate in categories:
            if os.path.isfile(self.datadir+cate+'.pickle'):
                temp= pd.read_pickle(self.datadir+cate+'.pickle')
                category=temp.columns[-1]
                items= pd.json_normalize(temp[category])
                dfs.append(items)
        df_items= pd.concat(dfs)
        df_prop_data=self.mpdata.getAllProperties()
        df_url_work_all = pd.DataFrame (columns = ['persistentId','property','value'])
        pivotField='value'
        #print (pivotField)
        for prpr in properties:
            print ('inspecting '+prpr)
            if prpr in self.url_dynamic_properties:
                myd=df_prop_data[df_prop_data['type.code']==prpr]
                act_myd=pd.merge(left=myd, right=df_items, left_on='ts_persistentId', right_on='persistentId')
                df_tool_work_urls=act_myd[['ts_persistentId', 'ts_category', 'ts_label', 'type.code', 'value']]
                df_tool_work_urls=df_tool_work_urls.rename(columns = {'ts_persistentId': 'persistentId', 'ts_category': 'category','ts_label': 'label','type.code': 'property'}, inplace=False)
            else:
                if prpr=='accessibleAt':
                    #pivotField='accessibleAt'
                    df_tool_work=df_items.explode(prpr)
                    df_tool_work_urls=df_tool_work[df_tool_work[prpr].str.len()>0]
                    df_tool_work_urls=df_tool_work_urls[['persistentId','category', 'label','accessibleAt']]
                    #df_urls=df_tool_work_urls[prpr].values
                    df_tool_work_urls=df_tool_work_urls.rename(columns = {'accessibleAt': 'value'}, inplace=False)
                    df_tool_work_urls['property']=prpr
                    df_tool_work_urls=df_tool_work_urls[['persistentId', 'category', 'label', 'property', 'value']]
                    #df_url_work_all=df_url_work_all.append(df_tool_work_urls)
            if not df_tool_work_urls.empty:
                df_url_work_all=pd.concat([df_url_work_all, df_tool_work_urls])
                #print(df_url_work_all.shape)
        df_urls=df_url_work_all['value'].values
        df_tool_work_aa_http_status = pd.DataFrame (columns = ['url','status'])
        #print (__name__)
        if __name__ == 'sshmarketplacelib.eval':
            with Pool(cores) as p:
                listofresults=p.map(self.getHTTP_Status, df_urls)
        for el in listofresults:
            if el:
                df_tool_work_aa_http_status=df_tool_work_aa_http_status.append(el[0], ignore_index=True)
 
        
        df_http_status_sub=df_tool_work_aa_http_status[df_tool_work_aa_http_status['status'] != 1]
        #df_http_status_err=df_http_status_sub[df_http_status_sub['status'] != 200]
        df_list_of_url_status=pd.merge(left=df_url_work_all, right=df_http_status_sub, left_on=pivotField, right_on='url')
        df_list_of_url_status.rename(columns = {'status_y': 'URLStatus'}, inplace = True)
        if df_list_of_url_status.empty:
            print("Result is empty")
            return df_list_of_url_status
        #create MPUrl
        df_list_of_url_status['tempurl'] = df_list_of_url_status['category'].apply(lambda y: y+'/' if len(y)>0 else y)
        df_list_of_url_status['MPUrl']=df_list_of_url_status['tempurl']+df_list_of_url_status['persistentId']
        df_list_of_url_status=df_list_of_url_status.drop(columns='tempurl',axis=1)
        return df_list_of_url_status[['MPUrl','persistentId', 'category', 'label', 'property','url', 'status']];
    
    
    def checkURLValuesInDataset(self, dataset, props=''):
        
        """
        
        Check HTTP Status for URLs contained in the dataset and returns a dataframe to the caller.
        
        Parameters:
        -----------
        
        dataset : DataFrame
            The dataset containing the URLs to be checked 
        props : String
            The properties to be checked
        
        
        """
        properties=[]
        #pool = Pool()
        cores=multiprocessing.cpu_count()
        df_urls=[]
        dfs=[]
        if props.strip()!='':
            properties=props.replace(" ", "").split(',')
        else:
            properties=self.url_all_properties
        if isinstance(dataset, pd.DataFrame):
            if dataset.empty:
                print("Error: dataset is empty")
                return pd.DatFrame()
        else:
            print("Error: dataset must be a dataframe")
            return pd.DatFrame()
        df_items= dataset
        df_prop_data=self.mpdata.getAllProperties()
        df_url_work_all = pd.DataFrame (columns = ['persistentId','property','value'])
        pivotField='value'
        #print (pivotField)
        for prpr in properties:
            print ('inspecting '+prpr)
            if prpr in self.url_dynamic_properties:
                myd=df_prop_data[df_prop_data['type.code']==prpr]
                act_myd=pd.merge(left=myd, right=df_items, left_on='ts_persistentId', right_on='persistentId')
                #print(act_myd['source.label'])
                df_tool_work_urls=act_myd[['ts_persistentId', 'ts_category', 'ts_label', 'type.code', 'value']]
                df_tool_work_urls=df_tool_work_urls.rename(columns = {'ts_persistentId': 'persistentId', 'ts_category': 'category','ts_label': 'label','type.code': 'property'}, inplace=False)
                #df_urls=df_tool_work_urls['value'].values
                #df_url_work_all=df_url_work_all.append(df_tool_work_urls)
            else:
                if prpr=='accessibleAt':
                    #pivotField='accessibleAt'
                    df_tool_work=df_items.explode(prpr)
                    df_tool_work_urls=df_tool_work[df_tool_work[prpr].str.len()>0]
                    df_tool_work_urls=df_tool_work_urls[['persistentId','category', 'label','accessibleAt']]
                    #df_urls=df_tool_work_urls[prpr].values
                    df_tool_work_urls=df_tool_work_urls.rename(columns = {'accessibleAt': 'value'}, inplace=False)
                    df_tool_work_urls['property']=prpr
                    df_tool_work_urls=df_tool_work_urls[['persistentId', 'category', 'label', 'property', 'value']]
                    #df_url_work_all=df_url_work_all.append(df_tool_work_urls)
                else:
                    if prpr=='actor.website':
                        df_tool_work_urls=df_items[df_items[prpr].str.len()>0]
                        df_tool_work_urls=df_tool_work_urls[['persistentId','category', 'label', 'actor.name','actor.website']]
                        df_tool_work_urls=df_tool_work_urls.rename(columns = {'actor.website': 'value'}, inplace=False)
                        df_tool_work_urls['property']=prpr
                        df_tool_work_urls=df_tool_work_urls[['persistentId', 'category', 'label', 'property', 'actor.name', 'value']]
            if not df_tool_work_urls.empty:
                df_url_work_all=pd.concat([df_url_work_all, df_tool_work_urls])
                #print(df_url_work_all.shape)
        df_urls=df_url_work_all['value'].values
        df_tool_work_aa_http_status = pd.DataFrame (columns = ['url','status'])
        if __name__ =='sshmarketplacelib.eval':
            with Pool(cores) as p:
                listofresults=p.map(self.getHTTP_Status, df_urls)
        for el in listofresults:
            #print (el)
            if el:
                df_tool_work_aa_http_status=df_tool_work_aa_http_status.append(el[0], ignore_index=True)
        #end
        #return df_tool_work_aa_http_status
            
        df_http_status_sub=df_tool_work_aa_http_status[df_tool_work_aa_http_status['status'] != 1]
        #df_http_status_err=df_http_status_sub[df_http_status_sub['status'] != 200]
        df_list_of_url_status=pd.merge(left=df_url_work_all, right=df_http_status_sub, left_on=pivotField, right_on='url')
        df_list_of_url_status.rename(columns = {'status_y': 'URLStatus'}, inplace = True)
        if df_list_of_url_status.empty:
            print("Result is empty")
            return df_list_of_url_status
        #create MPUrl
        df_list_of_url_status['tempurl'] = df_list_of_url_status['category'].apply(lambda y: y+'/' if len(y)>0 else y)
        df_list_of_url_status['MPUrl']=df_list_of_url_status['tempurl']+df_list_of_url_status['persistentId']
        df_list_of_url_status=df_list_of_url_status.drop(columns='tempurl',axis=1)
        return df_list_of_url_status[['MPUrl','persistentId', 'category', 'label', 'property','url', 'status']];
        
        
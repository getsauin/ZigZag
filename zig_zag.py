#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 15:00:56 2021

@author: sAuMac
"""

import logging
import yfinance as yf
import datetime as dt
import time
import csv
import pandas as pd
import numpy as np

stocks = []
ohlc = {}
ohlc_updated = {}
zig_zag_df = pd.DataFrame()
file_list = []
    
# Initialize all member variables 
def init():
    logging.basicConfig(filename="zigzag.log",
                        format='%(asctime)s %(message)s',
                        filemode='w')
    # Creating an object
    global logger
    logger = logging.getLogger()
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)
    logger.info("Logging is initialised ... ")
    
# Reading stocks from file
def read_stock_list():
    logger.info("hey there")
    with open("stocks.txt",'r',encoding = 'utf-8') as file:
        for row in file:
            stocks.append(row.strip())

# Fetch stock ohlc and populated in dictionary
def fetch_ohlc_of_stock():
    start = dt.datetime.today() - dt.timedelta(500)
    end = dt.datetime.today()
    for stock in stocks:
        ohlc[stock] = yf.download(stock, start, end, interval = '1d')
        logger.info("Successfully fetched ohlc for " + stock) 
        
def process_stock():
    for stock, ohlc_df in ohlc.items():
        logger.info("Processing started for " + stock)
        ohlc_updated[stock] = generate_trend(ohlc_df)
        today_close = ohlc_df['Close'][len(ohlc_df['Close']) - 1]
        add_2_final_output(stock, ohlc_updated[stock], today_close)
        logger.info("Processing completed for " + stock)
    

def generate_trend(i_df):
    depth = 4
    i_df['Highest'] = i_df['High'].shift(depth)
    i_df['Lowest'] = i_df['Low'].shift(depth)
    i_df['Rolling_High'] = i_df['High'].rolling(depth * 2 + 1).max()
    i_df['Rolling_Low'] = i_df['Low'].rolling(depth * 2 + 1).min()
    
    i_df['L_max_min_temp'] = np.where(i_df['Highest'] == i_df['Rolling_High'], 
                                      "L_max", np.where(i_df['Lowest'] == i_df['Rolling_Low'], 
                                                        "L_min",""))
    i_df['L_max_min'] = i_df['L_max_min_temp'].shift(-4)
    df_t = i_df[i_df.L_max_min != ""].copy()
    df_t = df_t[df_t['L_max_min'].notna()]
    #print(df_t.to_string())
    res_list = remove_continous_max_min(df_t)
    
    # print (df_t)
    zig_zag_df = pd.DataFrame()
    for dl in res_list:
        zig_zag_df = zig_zag_df.append(dl, ignore_index=False)
    # print (zig_zag_df)
    return zig_zag_df
    
    
def remove_continous_max_min(i_df):
    data_l = []
    total_rows = len(i_df['L_max_min'])
    max_min_idx = 0
    while (max_min_idx < total_rows):
        length = 1
        idx_2 = max_min_idx + 1
        #print ("total_rows:- ", total_rows, " max_min_idx:- ", max_min_idx)
        while (idx_2 < total_rows):
            if (i_df['L_max_min'][max_min_idx] == i_df['L_max_min'][idx_2]):
                length = length + 1
                idx_2 = idx_2 + 1
            else:
                row = i_df.iloc[[max_min_idx]]
                if (length == 1):
                    #print ("Idx: ", max_min_idx)
                    data_l.append(i_df.iloc[[max_min_idx]])
                break
        if (idx_2 - max_min_idx == 1 and idx_2 >= total_rows):
            data_l.append(i_df.iloc[[max_min_idx]])
            #print ("Idx::- ", max_min_idx)
        if (length > 1):
            frm = max_min_idx
            to = max_min_idx + (length - 1)
            if (i_df['L_max_min'][max_min_idx] == "L_min"):
                min_idx = np.argmin(i_df['Low'][frm:(to + 1)]) + frm
                data_l.append(i_df.iloc[[min_idx]])
                #print (i_df['L_max_min'][max_min_idx], " is continous for length =",length," idx - from ", frm, " to ", to, " minIdx=", min_idx)
            else:
                max_idx = np.argmax(i_df['High'][frm:(to + 1)]) + frm
                data_l.append(i_df.iloc[[max_idx]])
                #print (i_df['L_max_min'][max_min_idx], " is continous for length =",length," idx - from ", frm, " to ", to, " maxIdx=", max_idx)
            max_min_idx = max_min_idx + (length - 1)
        max_min_idx = max_min_idx + 1
    return data_l
    
def add_2_final_output(stock, i_zigzag_df, today_close):
    total_max_min = len(i_zigzag_df)
    first_min = i_zigzag_df['Low'][total_max_min - 1]
    second_min = i_zigzag_df['Low'][total_max_min - 3]
    first_max = i_zigzag_df['High'][total_max_min - 2]
    second_max = i_zigzag_df['High'][total_max_min - 4]
    logger.info("Today Close: %.4f", today_close)
    logger.info("First Min Date: " + (str(i_zigzag_df.iloc[total_max_min - 1].name).split()[0]) + ", Val: " + str(first_min))
    logger.info("First Max Date: " + (str(i_zigzag_df.iloc[total_max_min - 2].name).split()[0]) + ", Val: " + str(first_max))
    logger.info("Second Min Date: " + (str(i_zigzag_df.iloc[total_max_min - 3].name).split()[0]) + ", Val: " + str(second_min))
    logger.info("Second Max Date: " + (str(i_zigzag_df.iloc[total_max_min - 4].name).split()[0]) + ", Val: " + str(second_max))
    logger.info("Stop Loss: %.4f", first_min)
    if ((i_zigzag_df['L_max_min'][total_max_min - 1]) == "L_min"):
        if ((first_min < second_min) and (first_max < second_max) and (today_close > first_max)):
            logger.info("**** BUY TRIGGERED FOR **** " + stock)
            f_tuple = (stock, 
                              str(i_zigzag_df.iloc[total_max_min - 1].name).split()[0], first_min,
                                (str(i_zigzag_df.iloc[total_max_min - 2].name).split()[0]), first_max,
                                (str(i_zigzag_df.iloc[total_max_min - 3].name).split()[0]), second_min,
                                (str(i_zigzag_df.iloc[total_max_min - 4].name).split()[0]), second_max,
                                first_min)
            file_list.append(f_tuple)
        stop_loss = first_min

def generate_output():
    if (len(file_list) > 1):
        filename = "StockZigZag_Trend_" + time.strftime("%Y%m%d-%H%M%S") + ".csv"
        with open(filename, mode='w') as csv_file:
            fwriter = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for tup in file_list:
                fwriter.writerow(tup)
    
# Using the special variable
# __name__
if __name__ == "__main__":
    init()
    read_stock_list()
    fetch_ohlc_of_stock()
    process_stock()
    generate_output()


    
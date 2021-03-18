#!/usr/bin/env python3.7.9
# -*- Coding: UTF-8 -*-

'''
Functions to work with Dataloggers
author: Vinicius Roggério da Rocha
e-mail: vinicius.rocha@inpe.br
version: 0.0.1
date: 2021-02-25
'''

import glob
import numpy as np
import pandas as pd

class RawData():
    '''
    Functions to deal with Dataloggers data
    '''

    @staticmethod
    def list_files(pattern):
        '''
        Create file's list from path/pattern
        '''
        files = sorted([f for f in glob.glob(pattern, recursive=True)])
        return files

    @staticmethod
    def create_df(cols_names):
        '''
        Create dataframe
        '''
        df = pd.DataFrame(columns=cols_names)
        return df

    @staticmethod
    def fst_char(m):
        '''
        Get first character from file
        '''
        infile = open(m, 'r')
        text = infile.read()
        for line in text:
            lines = text[0]
            for i in range(len(text)):
                if text[i] == '\n':
                    lines += text[i+1]
                return(lines)
            m.close()

    @staticmethod
    def get_columns():
        '''
        Get columns names
        '''
        col_names = ['ID','Year','Day','Min','Global_Avg','Global_Std',
         'Global_Max','Global_Min','Diffuse_Avg','Diffuse_Std','Diffuse_Max',
         'Diffuse_Min','PAR_Avg','PAR_Std','PAR_Max','PAR_Min','Lux_Avg',
         'Lux_Std','Lux_Max','Lux_Min','Temp_Sfc','Humid','Press','Prec',
         'Ws10_Avg','Wd10_Avg','Wd10_Std','CosAngZen','Direct_Avg',
         'Direct_Std','Direct_Max','Direct_Min','LW_Avg','LW_Std','LW_Max',
         'LW_Min','Temp_Global','Temp_Direct','Temp_Diffuse','Temp_Dome',
         'Temp_Case', 'sensor1', 'sensor2', 'sensor3', 'sensor4']
        return col_names

    @staticmethod
    def read_withoutheader(filename, col_names):
        '''
        Read data from piranometer - CR23X Datalogger
        Older - without header at file
        '''
        df = pd.read_csv(filename, header=None, names=col_names)
        # Select rows that start with '1' .reset_index(drop=True)
        df['ID'] = df['ID'].astype(str)
        df = df[df['ID'].str.startswith('1')]
        df = df[df['ID'].str.len() == 3]
        # Create column Timestamp
        df['Date'] = pd.to_datetime(df['Day'], format='%j').dt.strftime('%m-%d')
        df['Time'] = pd.to_datetime(df['Min'], unit='m').dt.strftime('%H:%M')
        df['Timestamp'] = df['Year'].astype(int).astype(str) + '-' +\
         df['Date'].astype(str) + ' ' + df['Time'].astype(str) + ':00'
        return df

    @staticmethod
    def read_withheader(filename):
        '''
        Read data from piranometer - CR3000 Datalogger
        Newer - with header
        '''
        df = pd.read_csv(filename, skiprows=1)
        df = df.drop(df.index[[0,1]]).reset_index(drop=True)
        # Rename column from glo_avg to Global_Avg
        if any('glo_avg' in s for s in df.columns.values):
            df = df.rename(columns={'glo_avg': 'Global_Avg'})
        df = df.rename(columns={'TIMESTAMP': 'Timestamp'})
        return df

    @staticmethod
    def clean_df(df, column_name):
        '''
        Replace NAN strings by NaN
        '''
        df[column_name] = df[column_name].replace('NAN', np.nan)
        return df

    @staticmethod
    def mean_df(df, column_name):
        '''
        Calculate means at each 5 minutes
        New timestamp like index
        '''
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S')
        df = df.set_index('Timestamp')
        df[column_name] = df[column_name].apply(pd.to_numeric, errors='coerce')
        df_means = df.resample('5Min').mean()
        return df_means

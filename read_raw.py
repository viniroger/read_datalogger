#!/usr/bin/env python3.7.9
# -*- Coding: UTF-8 -*-

from helpers.rawdata import RawData

# Define place
import sys
place = sys.argv[1] #'BRB'

# Create DF to recieve all data
cols_names = ['Timestamp', 'Global_Avg']
df_all = RawData.create_df(cols_names)

# Loop for all files
pattern = 'data_in/*.dat'
files = RawData.list_files(pattern)
for filename in files:
    print(filename)
    # Check type of file
    first_char = RawData.fst_char(filename)
    if first_char == '\"':
        df = RawData.read_withheader(filename)
    else:
        col_names = RawData.get_columns()
        df = RawData.read_withoutheader(filename, col_names)
    # Append to final df
    df_all = df_all.append(df[['Timestamp', 'Global_Avg']])
    #print(df)
    #exit()

# Replace NAN strings by NaN
df_all = RawData.clean_df(df_all, 'Global_Avg')

# Save 1min freq data
filename = 'data_out/{0}.csv'.format(place)
df_all.to_csv(filename, sep=';', index=False)

# Calculate means at each 5 minutes
df_means = RawData.mean_df(df_all, 'Global_Avg')
# Save df to CSV
filename = 'data_out/{0}_5min.csv'.format(place)
df_means.to_csv(filename, sep=';')

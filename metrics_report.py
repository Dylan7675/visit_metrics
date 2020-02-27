import pandas as pd
import glob
from collections import OrderedDict
from datetime import datetime

def main():

    annual_data = pd.DataFrame()

    all_df = {1 : None,
              2 : None,
              3 : None,
              4 : None,
              5 : None,
              6 : None,
              7 : None,
              8 : None,
              9 : None,
              10 : None,
              11 : None,
              12 : None
}
    
    all_df = OrderedDict(all_df)

    csv_titles = {1 : "Jan_Data.csv",
                  2 : "Feb_Data.csv",
                  3 : "Mar_Data.csv",
                  4 : "Apr_Data.csv",
                  5 : "May_Data.csv",
                  6 : "June_Data.csv",
                  7 : "July_Data.csv",
                  8 : "Aug_Data.csv",
                  9 : "Sep_Data.csv",
                  10 : "Oct_Data.csv",
                  11 : "Nov_Data.csv",
                  12 : "Dec_Data.csv"
}
    # grabbing all csv files in current dir
    files = glob.glob('*.csv')

    for file in files:
        month = int(file.split("_")[0])
        all_df[month] = append_csv(file, all_df[month])

    # cleaning timestamp, sorting data by timestamp, compiling into annual report
    for key in all_df.keys():
        if all_df[key] is not None:
            all_df[key] = clean_timestamp(all_df[key])
            all_df[key] = all_df[key].sort_values(by='timestamp')
            all_df[key].to_csv(csv_titles[key], index=None)
            annual_data = annual_data.append(all_df[key])

    annual_data.to_csv("Annual_Report.csv", index=None)


def append_csv(file, month_df):

    """This function compiles daily csv's into a df per month.
       Input: file - str ; title of csv file
              month_df - dataframe ; df for the month which to append to
       Returns: month_df with appended data
    """ 

    if month_df is None:
        month_df = pd.read_csv(file)
    else:
        df = pd.read_csv(file, dtype={'timestamp': str}, skiprows=0)
        month_df = month_df.append(df, ignore_index=True)

    if len(month_df.index) == 0:
        month_df = None

    return month_df


def clean_timestamp(df):

    """This function converts the timestamp values to a numerical timestamp.
       Input: df - dataframe ; dataframe of monthly data
       Returns: df - dataframe ; dataframe of monthly data with cleaned timestamp
    """

    col = df['timestamp'].tolist()

    for index,val in enumerate(col):
        col[index] = datetime.strptime(" ".join(val.split()[1:5]), "%b %d %Y %H:%M:%S")
        
    temp_df = pd.DataFrame(col, columns=['timestamp'])

    df['timestamp'] = temp_df['timestamp']

    return df    


if __name__ == '__main__':

    main()

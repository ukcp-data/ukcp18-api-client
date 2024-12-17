"""
UKCP 18 Rainfall processing

Author Changgui Wang

run command:
python  ukcp18_download_proc_profile_new_save_run1_12.py projection_id mem_id

required input form user:
profile_selected_month: "YearsMonths_byBinCounts_Rand_OtherYears_12_m5.csv")
region mask_nc: "UKWC_Cleaned_land-cpm_uk_2.2km.nc"
both of these two file are read from INPUT directory set at very begining of the script, such as 
INPUT_PATH = "/mnt/metdata/ukcp18"
#INPUT_PATH = "/mnt/MetData/ukcp18"

import local rouine: convert_ll2str.py 

"""

from pathlib import Path
import os
import sys
import numpy as np
import pandas as pd
import concurrent.futures
import xarray
import dask
import cftime
import timeit
from download import download_url_filename
from convert_ll2str import *
import concurrent.futures
import functools
#xarray.core.rolling.DataArrayRolling._setup_windows = lambda *args: None


HOME = str(Path.home())

BINS = {1: [2.0, 4, 7,  10, 14, 18, 24, 30, 40, 55,  70, 90, 110, 135],
        3: [2, 6, 10, 15, 20, 30, 40, 60, 80, 110, 140, 175, 215, 265],
        6: [2, 7, 13, 19, 28, 40, 55, 80, 115, 160, 210, 260, 320, 390]}

RPS = {1: [19, 24, 32, 36, 42], 3: [29, 35, 44, 49, 57],
       6: [35, 42, 53, 59, 67], 9: [39, 46, 59, 65, 75],
       12: [42, 50, 63, 70, 80], 24: [50, 58, 74, 83, 95]}

rp_years = [5, 10, 30, 50, 100]

global OUTPUT_PATH

INPUT_PATH = "/mnt/metdata/ukcp18"
#INPUT_PATH = "/mnt/MetData/ukcp18"
SAVE_PATH = "/mnt/metdata/ukcp18"
#SAVE_PATH = INPUT_PATH

#OUTPUT_PATH = os.path.join(HOME, "Downloads", "output", "profile")
#INPUT_PATH= os.path.join(HOME, "Downloads")   #"/mnt/metdata/ukcp18"
NUMBER_OF_WC = 13
# accum_duration_start = {3: 22, 24: 1, 6: 9, 9: 16, 12: 13}
#accum_duration_start = {1: 23, 3: 22, 6: 19, 9: 16, 12: 13, 24: 1}
accum_duration_start = {6: 19, 9: 16, 12: 13, 24: 1}
MASK = None     #  #mask_wcid = MASK.where(MASK.WCID == 1, drop=True)
remove_items = ['ensemble_member_id', 'grid_latitude_bnds', 'grid_longitude_bnds',
                'time_bnds','rotated_latitude_longitude', 'year', 'yyyymmddhh', 'ensemble_member']
squeez_corrds = ["bnds", "ensemble_member"]

#pd.set_option("display.max_columns", None) #, "display.max_rows", None)

def main(infile, year, mon, m):
    """
    Read ukcp18 climate change data precipitation
    infile: input file list
    start: start year
    """

    with xarray.open_mfdataset(infile, engine='netcdf4',
                               combine='nested', concat_dim='time', parallel=True,
                               data_vars='minimal', coords='minimal', compat='override') as ds:

        #year_day_idx = pd.MultiIndex.from_arrays([ds_orig.time.year.values,
        #                                          ds_orig.time.month_number.values, ds_orig.time.dt.day.values])
        #ds_orig.coords['year_month_day'] = ('time', year_day_idx)

        ds = ds.stack(location=("grid_latitude", "grid_longitude"))
        ids = get_cell_ids(ds.location.values)
        ds.coords['location_id'] = ('location', ids)
        #ds["WCID"] = (['location'], MASK.WCID.data)
        ds = ds.where(ds.bnds == 0, drop=True)
        for item in remove_items:
            del ds[item]
        for item in squeez_corrds:
            ds = ds.squeeze(item)

        ds = ds.where(MASK["WCID"] >= 0, drop=True)

        starttime = timeit.default_timer()

        with dask.config.set(**{'array.slicing.split_large_chunks': False}):
            for wid in range(NUMBER_OF_WC):
                ds_mask = ds.where(MASK.WCID == wid, drop=True)
                for duration, v in accum_duration_start.items():
                    start = get_start_year(year, mon, v)
                    precip = ds_mask.where(ds['time'] >= start, drop=True)
                    #lopp_over_wcid(precip, year, mon, m, wid, duration)
                    if duration > 1:
                        da_window = rolling_window_sum(precip, duration, 'pr')
                        da_window = da_window.rename({"pr": "pr_sum"})
                        da_window = da_window.assign(pr=precip.pr)
                        da_window['time'] = da_window["time"].dt.strftime("%Y-%m-%d %H:%M")

                        df_window = da_window.to_dataframe()

                        df_window.index = df_window.index.droplevel(['grid_latitude', 'grid_longitude'])

                        get_pr_profile(df_window, m, mon, duration, wid)

                        #if duration == 3 or duration == 6:
                        #    get_bin_counts(df_window, year, mon, m, duration, wid)
                    else:
                        df_window = precip.to_dataframe()
                        df_window = df_window[df_window['month_number'] == mon]
                        df_window.index = df_window.index.droplevel(['grid_latitude', 'grid_longitude'])
                        get_pr_profile(df_window, m, mon, duration, wid)

                    precip = None
                    df_window_duration = None

        print("The time difference is :", timeit.default_timer() - starttime)

        ds.close()


def lopp_over_wcid(precip, year, mon, m, wid, duration):

    if duration > 1:
        da_window = rolling_window_sum(precip, duration, 'pr')
        window_duration = precip.rolling(time=duration).construct("window_dim")

        df_window = da_window.to_dataframe()
        df_window = df_window[df_window['month_number'] == mon]
        df_window.index = df_window.index.droplevel(['grid_latitude','grid_longitude'])

        df_window_duration = window_duration.to_dataframe()
        df_window_duration = df_window_duration[df_window_duration['month_number'] == mon]
        df_window_duration.index = df_window_duration.index.droplevel(['grid_latitude','grid_longitude'])

        get_pr_profile(df_window_duration, m, mon, duration, wid)

        #if duration == 3 or duration == 6:
        #    get_bin_counts(df_window, year, mon, m, duration, wid)
    else:
        df_window = precip.to_dataframe()
        df_window = df_window[df_window['month_number'] == mon]
        df_window.index = df_window.index.droplevel(['grid_latitude', 'grid_longitude'])
        get_pr_profile(df_window, m, mon, duration, wid)

def get_dry_days(da_day_sum, ds_month9, year, mon, m, wid):
    """Dry day counts fro xr data set"""

    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        da_day_dry = da_day_sum.where(da_day_sum < 0.1)
        da_dry_count = da_day_dry.count(dim="year_month_day")
        save_dry_counts(da_dry_count, year, mon, m, wid)

        if mon == 9:
            #da_day_sum9 = ds_month9.groupby('year_month_day').sum()
            da_day_dry9 = ds_month9.where(ds_month9 < 0.1)
            da_dry_count9 = da_day_dry9.count(dim="year_month_day")
            save_dry_counts(da_dry_count9, year, 13, m, wid)


def get_month_total(ds, year, mon, m, wid):
    """Dry day counts fro xr data set"""

    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        start = cftime.Datetime360Day(year, mon, 1, 0, 30, 0)
        ds_month = ds.where(ds['time'] >= start, drop=True)
        da_month_sum = ds_month.sum(dim='time')
        save_month_total(da_month_sum, year, mon, m, wid)

        if mon == 9:
            start9 = cftime.Datetime360Day(year, mon, 15, 0, 30, 0)
            ds_month9 = ds_month.where(ds['time'] <= start9, drop=True)
            da_sum = ds_month9.sum(dim='time')
            save_month_total(da_sum, year, 13, m, wid)


def get_pr_profile(df_win_wid, m, mon, duration, wid):

    cols = ['Projection_slice_ID', 'Member', 'end date', 'lon', 'lat', 'Total accum', 'Hyet']

    PR = list(RPS[duration])

    if duration > 1:

        select_list = ['Time', 'location_id', 'longitude', 'latitude', 'pr', 'pr_sum']
        final_list = ['Time', 'longitude', 'latitude', 'pr_sum', 'Hyet']


        df_win_list = []
        df_win_wid.insert(0, 'Time', df_win_wid.index)
        df_win_wid.sort_values(by=['location_id', 'Time', 'pr_sum'], inplace=True)

        for i in range(1, len(PR) + 1):
            filename = os.path.join(OUTPUT_PATH,
                                    f"Profile_{rp_years[i - 1]}y_{duration}h_ens{m}_proj{PROJECTION_ID}.csv")

            if i < len(PR):
                df_wink = df_win_wid.loc[(df_win_wid['pr_sum'] > PR[i - 1]) 
                                         & (df_win_wid['pr_sum'] <= PR[i])
                                         & (df_win_wid['month_number'] == mon) ]
            else:
                df_wink = df_win_wid.loc[ (df_win_wid['pr_sum'] > PR[i - 1]) 
                                        & (df_win_wid['month_number'] == mon)]

            if len(df_wink) > 0:
                df_wink = df_wink.loc[:, select_list]

                location = df_wink['location_id'].tolist()

                temp_df = df_win_wid[['Time', 'location_id', 'pr', 'pr_sum']]
                temp_df = temp_df[temp_df['location_id'].isin(location)]
                #temp_df1 = np.array(temp_df.groupby('location_id')['pr'].agg(list).values.tolist())
                #temp_time = np.array(temp_df.groupby('location_id')['Time'].agg(list).values.tolist())
                temp_time = np.array(temp_df['Time'].values)
                temp_df1 = np.array(temp_df['pr'].values)
                temp_sum = np.array(temp_df['pr_sum'].values)
                temp_location = np.array(temp_df['location_id'].values)
                index_num = 0

                profile_list = []
                for index, row in df_wink.iterrows():
                    this_time = row['Time']
                    this_location = row['location_id']
                    this_sum = row['pr_sum']
                    #time_index = np.where(temp_time == this_time)
                    sum_index = np.where(temp_sum == this_sum)
                    #location_index = np.where(temp_location == this_location)
                    #sum_index1 = sum_index[0][0]
                    #data_profile = temp_df1[sum_index1 - duration + 1: sum_index1 + 1].tolist()
                    sum_index1 = sum_index[0][:]

                    for sum_ind in sum_index1:
                        data_profile = temp_df1[sum_ind - duration + 1: sum_ind + 1].tolist()
                        if temp_location[sum_ind] == this_location and temp_time[sum_ind] == this_time:
                            break

                    profile_list.append(data_profile)
                    index_num += 1

                df_wink['Hyet'] = profile_list

                df_wink = df_wink.loc[:, final_list]
                df_wink.columns = ['end date', 'lon', 'lat', 'Total accum', 'Hyet']

                df_wink.insert(0, 'WCID', wid)
                df_wink.insert(0, 'Member', m)
                df_wink.insert(0, 'Projection_slice_ID', PROJECTION_ID)
                save(filename, df_wink)

                df_wink =None

    else:
        for i in range(1, len(PR) + 1):
            filename = os.path.join(OUTPUT_PATH,
                                    f"Profile_{rp_years[i - 1]}y_{duration}h_ens{m}_proj{PROJECTION_ID}.csv")
            if i < len(PR):
                df_wink = df_win_wid[(df_win_wid['pr'] > PR[i - 1]) & (df_win_wid['pr'] <= PR[i])]
            else:
                df_wink = df_win_wid[df_win_wid['pr'] > PR[i - 1]]

            df_wink = df_wink.loc[:, ['latitude', 'longitude', 'pr']]

            df_wink.insert(0, 'Time', df_wink.index)
            df_wink.dropna(subset=["pr"], inplace=True)
            df_wink.columns = ['end date', 'lon', 'lat', 'Total accum']

            df_wink.insert(0, 'WCID', wid)
            df_wink.insert(0, 'Member', m)
            df_wink.insert(0, 'Projection_slice_ID', PROJECTION_ID)

            save(filename, df_wink)
            df_wink =None


def get_rp_df(da_mask, i, PR):
    if i < len(PR):
        da_profile = da_mask.where((da_mask > PR[i - 1]) & (da_mask <= PR[i]), drop=True)
    else:
        da_profile = da_mask.where((da_mask > PR[i - 1]), drop=True)

    df_da = da_profile.to_dataframe()
    del df_da['ensemble_member_id']
    df_pr = df_da[['latitude', 'longitude', 'location_id', 'pr']]
    df_pr.index = df_pr.index.droplevel([0, 2, 3])

    return df_pr

def get_bin_counts(df, year, mon, m, duration, wid):
    """Get bin counts"""
    bins = BINS[duration]
    filename = os.path.join(OUTPUT_PATH, f"Rainfall_bin_counts_{duration}h_ens{m}_proj{PROJECTION_ID}.csv")
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Bin counts']

    total_count = []

    for i in range(1, len(bins)):
        df_count = df[(df['pr'] > bins[i - 1]) & (df['pr'] < bins[i])]
        total_count.append(df_count.shape[0])
    data_list = [PROJECTION_ID, m, year, mon, wid, total_count]
    df = pd.DataFrame([data_list], columns=cols)
    save(filename, df)

    data_list =None
    df = None
    total_count = None

    if mon == 9:
        df.insert(0, 'Time', df.index)
        start9 = cftime.Datetime360Day(year, mon, 15, 0, 30, 0)
        df = df[df['Time'] <= start9]
        for i in range(1, len(bins)):
            df_count = df[(df['pr'] > bins[i - 1]) & (df['pr'] < bins[i])]
            total_count.append(df_count.shape[0])
        data_list = [PROJECTION_ID, m, year, mon, wid, total_count]
        df = pd.DataFrame([data_list], columns=cols)
        save(filename, df)

        data_list =None
        df = None
        total_count = None

def save_month_total(da, year, mon, m, wid):
    filename = os.path.join(OUTPUT_PATH, f"Total_rainfall_ens{m}_proj{PROJECTION_ID}.csv")
    ds_mask = da.mean()
    dry_list = [PROJECTION_ID, m, year, mon, wid, ds_mask.pr.values.tolist()]
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Mean total rainfall']
    df = pd.DataFrame([dry_list], columns=cols)
    save(filename, df)


def save_dry_counts(ds, year, mon, m, wid):
    filename = os.path.join(OUTPUT_PATH, f"Dry_days_counts_ens{m}_proj{PROJECTION_ID}.csv")

    ds_mask = ds.mean()

    dry_list = [PROJECTION_ID, m, year, mon, wid, ds_mask.pr.values.tolist()]
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Mean dry day counts']
    df = pd.DataFrame([dry_list], columns=cols)
    save(filename, df)


def get_dry_sept(ds, year):
    """Dry day counts fro xr data set"""
    with dask.config.set(**{'array.slicing.split_large_chunks': False}):

        start = cftime.Datetime360Day(year, 9, 1, 0, 30, 0)
        ds_month = ds.where(ds['time'] >= start, drop=True)
        da_day_sum = ds_month.groupby('year_month_day').sum()
        da_day_dry = da_day_sum.where(da_day_sum < 0.1)
        da_dry_count = da_day_dry.count(dim="year_month_day")


        #return da_dry_count

def get_file_name(year, mon, m):
    start_date = f"{year:04d}{mon:02d}01"
    file_main = f"pr_rcp85_land-cpm_uk_2.2km_{m:02d}_1hr_{start_date}"
    file_name = os.path.join(INPUT_PATH, file_main + f"-{year:04d}{mon:02d}30.nc")

    return file_name


def get_start_year(year, mon, v):
    if year != 1980:
        year1 = year
        mon1 = mon - 1
        if mon == 1:
            year1 = year - 1
            mon1 = 12
        start = cftime.Datetime360Day(year1, mon1, 30, v, 0, 0)
    else:
        mon = 12
        start = cftime.Datetime360Day(year, mon, 1, 0, 30, 0)

    return start


def call_main(df, mem_id):
    global OUTPUT_PATH
    mem = [mem_id] #, 5, 6 ] 

    for m in mem:
        OUTPUT_PATH = f"{SAVE_PATH}/profile_new2/proj{PROJECTION_ID}/output_mem{m}"
        if not os.path.isdir(OUTPUT_PATH):
            os.makedirs(OUTPUT_PATH)
        for index, row in df.iterrows():
            year = row['Year']
            mon = row['Month']
            print(m, year, mon)
            year1 = year
            mon1 = mon - 1
            if mon == 1:
                year1 = year - 1
                mon1 = 12

            file_name = get_file_name(year, mon, m)
            pre_file_name = get_file_name(year1, mon1, m)

            # downlaod file
            #if os.path.isfile(file_name):
            #    print('file exist')
            #else:
            #    download_url_filename(year, mon, m)

            #if os.path.isfile(pre_file_name):
            #    print('file exist')
            #else:
            #    download_url_filename(year1, mon1, m)
            if (year==1980 and mon==12) or (year==2020 and mon==12) or (year==2060 and mon==12):
                infile =[file_name]
            else:
                infile = [pre_file_name, file_name]
            main(infile, year, mon, m)
            # os.remove(pre_file_name)

#check if dir exist if not create it
def check_dir(file_name):
    directory = os.path.dirname(file_name)
    if not os.path.exists(directory):
        os.makedirs(directory)


def save(file_name, df):
    check_dir(file_name)
    if os.path.isfile(file_name):
        df.to_csv(file_name, mode='a', header=False, index=False, float_format="%.2f")
    else:
        df.to_csv(file_name, mode='a', index=False, float_format="%.2f")


def rolling_window_sum(ds, window_size, item):
    """rolling wind for a cube by define window size"""

    #ds_window = cube.rolling(time=window_size).sum().dropna(dim='time', how='all')
    ds_window = ds.rolling(time=window_size, min_periods=window_size).construct("new").sum("new", skipna=True)

    return ds_window


if __name__ == "__main__":

    global PROJECTION_ID
    try:
        PROJECTION_ID = int(sys.argv[1])
        mem_id = int(sys.argv[2])
    except IndexError:
        print("Please provide member (1, 4...15) and projection id (1,2 or 3)")

    profile_selected_month = pd.read_csv(os.path.join(INPUT_PATH, "YearsMonths_byBinCounts_Rand_OtherYears_12_m5.csv"))
    mask_nc = os.path.join(INPUT_PATH, "UKWC_Cleaned_land-cpm_uk_2.2km.nc")

    MASK_ORIG = xarray.open_dataset(mask_nc)
    MASK = MASK_ORIG.stack(location=("grid_latitude", "grid_longitude"))

    PROJECTION_ID = 1

    projection_profile = profile_selected_month[profile_selected_month['Projection_slice_ID']
                                                == PROJECTION_ID]
    
   
    call_main(projection_profile, mem_id)

 
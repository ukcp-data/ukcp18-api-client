import ukcp18_opendap_download as opendap
import os
import re
import numpy as np
import pandas as pd
import xarray as xr
import dask
import dask.config
import cftime
import timeit
import convert_ll2str as c2str

from datetime import datetime
from typing import Tuple, Dict
from dateutil.relativedelta import relativedelta

def process_ukcp_data(input_file_path: str,
                      output_file_path: str, 
                      config: Dict,
                      year: int, 
                      month: int,
                      member_id: int,
                      var_id: str,
                      proj_id: str,
                      mask_1D: xr.Dataset):
    """
    Read UKCP18 (climate model) daily precipitation data 

    input_file_path: path to input file url (API)
    output_file_path: path to output files
    year: year to analyse (e.g. 2035)
    month: month to analyse (e.g. 8 for August)
    var_id: cf-compliant variable ID (e.g. pr, tas)
    member_id: ensemble member ID (01,04,05,...,15)
    mask_1D: xarray Dataset containing 1D land-ocean mask 
    """

    with opendap.initiate_opendap_multiple_files(input_file_path, var_id) as ds:
        print("Finished reading in data from Opendap!")

        with dask.config.set(**{'array.slicing.split_large_chunks': True}):
            ds = ds.stack(location=("grid_latitude", "grid_longitude"))
            ids = c2str.get_cell_ids(ds.location.values)
            ds.coords['location_id'] = ('location', ids)
            ds = ds.where(ds.bnds == 0, drop=True)
            for item in config["remove_items"]:
                del ds[item]
            for item in config["squeeze_coords"]:
                ds = ds.squeeze(item)

            ds = ds.where(mask_1D["WCID"] >= 0, drop=True)

            starttime = timeit.default_timer()

            for wcid in range(config["NUMBER_OF_WC"]):
                print(f"Working on water company {str(wcid)}")
                ds_mask = ds.where(mask_1D.WCID == wcid, drop=True)
                for duration_str, start_hour in config["accum_duration_start"].items():
                    duration = int(duration_str)
                    start = get_start_year(year, month, start_hour)
                    precip = ds_mask.where(ds['time'] >= start, drop=True)
                    
                    print("Starting dry days calculation!")
                    get_dry_days(precip, 
                                 year, 
                                 month, 
                                 member_id, 
                                 wcid, 
                                 proj_id, 
                                 output_file_path)
                    print("Starting total rainfall calculation!")
                    get_month_total(precip, 
                                    year, 
                                    month, 
                                    member_id, 
                                    wcid, 
                                    proj_id, 
                                    output_file_path)

                    print(f"Calculating {str(duration)}-h accumulated precip, starting at {str(start_hour)}Z")
                    if duration > 1:
                        ds_window = rolling_window_sum(precip, duration)
                        ds_window = ds_window.rename({"pr": "pr_sum"})
                        ds_window = ds_window.assign(pr=precip.pr)
                        ds_window['time'] = ds_window["time"].dt.strftime("%Y-%m-%d %H:%M")
                        df_window = ds_window.to_dataframe()
                        df_window.index = df_window.index.droplevel(['grid_latitude', 'grid_longitude'])

                        # UNCOMMENT TO CALCULATE PRECIPITATION PROFILES
                        # get_pr_profile(df_window, member_id, month, duration, wcid, output_file_path, proj_id, config)

                        if duration == 3 or duration == 6:
                            get_bin_counts(df_window, 
                                           year, 
                                           month, 
                                           member_id, 
                                           duration, 
                                           wcid, 
                                           output_file_path,
                                           proj_id, 
                                           config)

                    else:
                        df_window = precip.to_dataframe()
                        df_window = df_window[df_window['month_number'] == month]
                        df_window.index = df_window.index.droplevel(['grid_latitude', 'grid_longitude'])
                        get_pr_profile(df_window, 
                                       member_id, 
                                       month, 
                                       duration, 
                                       wcid, 
                                       output_file_path,
                                       proj_id, 
                                       config)

                    precip = None

            print("This code took :", timeit.default_timer() - starttime," (s) to run...")

            ds.close()


def get_pr_profile(df_prcp_water_company: pd.DataFrame, 
                   member_id: int, 
                   month: int, 
                   duration: int, 
                   wcid: int,
                   output_file_path: str,
                   proj_id: str,
                   config: Dict):
    """ 
    Identify the grid points within specified rainfall bounds (RP5, RP10, RP30, RP50, RP100) for the rolling window 
    Code is run for the grid points belonging to a single water company ('WC')
    For each of these cases, retrieve all the data leading up to the validity time (e.g. 6-h before T+0 for a 6-h window)
    Save this information to a dataframe and write out to a csv; repeat for water company, rolling window and RP 
    """

    # rainfall thresholds (upper,lower) for each RP within the rolling window (1-h, 3-h, 6-h)
    RPS = {int(key): value for key, value in config["RPS"].items()}
    PR = list(RPS[duration])

    if duration > 1:

        select_list = ['Time', 'location_id', 'longitude', 'latitude', 'pr', 'pr_sum']
        final_list = ['Time', 'longitude', 'latitude', 'pr_sum', 'Hyet']

        # include 'Time' as a df column rather than only the index
        df_prcp_water_company.insert(0, 'Time', df_prcp_water_company.index)
        # sort by 'location_id', then 'Time' and then 'pr_sum' (modifying the existing df)
        df_prcp_water_company.sort_values(by=['location_id', 'Time', 'pr_sum'], inplace=True)

        # loop over RP thresholds as defined in 'rp_years'
        for i in range(1, len(PR) + 1):
            filename = os.path.join(output_file_path,
                                    f"Profile_{config["rp_years"][i - 1]}y_{duration}h_ens{member_id}_proj{proj_id}.csv")

            # filter the df based on 2 conditions, and return a df containing only the filtered rows
            # 'pr_sum' > PR[i-1] but <= PR[i] + 'month_number' == specified month  
            if i < len(PR):
                df_prcp_threshold = df_prcp_water_company.loc[(df_prcp_water_company['pr_sum'] > PR[i - 1]) 
                                                              & (df_prcp_water_company['pr_sum'] <= PR[i])
                                                              & (df_prcp_water_company['month_number'] == month) ]
            # for the highest RP there is only a lower limit (i.e. >= precip_threshold)
            else:
                df_prcp_threshold = df_prcp_water_company.loc[ (df_prcp_water_company['pr_sum'] > PR[i - 1]) 
                                                              & (df_prcp_water_company['month_number'] == month)]

            if len(df_prcp_threshold) > 0:
                # filter the df to only include the selected columns, for all rows [:] (see 'select_list')
                df_prcp_threshold = df_prcp_threshold.loc[:, select_list]
                # create a list of all the location IDs 
                location = df_prcp_threshold['location_id'].tolist()

                # create a temporary df containing the columns below, from the original, unfiltered df (~5 million)
                temp_df = df_prcp_water_company[['Time', 'location_id', 'pr', 'pr_sum']]
                # subset by the locations that are in the list we created above (~ 20,000)
                temp_df = temp_df[temp_df['location_id'].isin(location)]
                # extract the values of each column individually and assign to new (temporary) variables 
                # each of these contains ~20,000 elements (for this example, 6-h rolling window)
                temp_time = np.array(temp_df['Time'].values)
                temp_df1 = np.array(temp_df['pr'].values)
                temp_sum = np.array(temp_df['pr_sum'].values)
                temp_location = np.array(temp_df['location_id'].values)
                index_num = 0

                profile_list = []
                # loop through all the rows of the processed df ('df_pr_threshold')
                # identify all rows where the 'pr_sum' variable matches one of the values in 'temp_sum'
                # 'sum_index1' contains the index of each row (from the 'temp_df' dataframe)
                for index, row in df_prcp_threshold.iterrows():
                    this_time = row['Time']
                    this_location = row['location_id']
                    this_sum = row['pr_sum']
                    sum_index = np.where(temp_sum == this_sum)
                    sum_index1 = sum_index[0][:]

                    for sum_ind in sum_index1:
                        # 'data_profile' represents a profile of the rainfall data for the duration (6-h)
                        # leading up to and including the index 'sum_ind'
                        # This code grabs the rainfall values leading up to the validity time 
                        data_profile = temp_df1[sum_ind - duration + 1: sum_ind + 1].tolist()
                        # check the location + time corresponding to the current index ('sum_ind')
                        # if both location + time from the current df ('df_prcp_threshold') match the original df ('temp_df')
                        # the loop breaks (condition satisfied): this code ensures that only the first match is processed 
                        if temp_location[sum_ind] == this_location and temp_time[sum_ind] == this_time:
                            #print(f"Match found at index {sum_ind}")
                            break

                    profile_list.append(data_profile)
                    index_num += 1

                # add the previous 6-h of rainfall data (hyetograph) to the dataframe in the 'Hyet' column
                df_prcp_threshold['Hyet'] = profile_list

                # tidy the dataframe by keeping only the columns we need for future analysis 
                df_prcp_threshold = df_prcp_threshold.loc[:, final_list]
                df_prcp_threshold.columns = ['end date', 'lon', 'lat', 'Total accum', 'Hyet']

                # insert additional columns with WCID, ensemble member and projection slice information 
                df_prcp_threshold.insert(0, 'WCID', wcid)
                df_prcp_threshold.insert(0, 'Member', member_id)
                df_prcp_threshold.insert(0, 'Projection_slice_ID', proj_id)
                save(filename, df_prcp_threshold)

                df_prcp_threshold = None

    else:
        for i in range(1, len(PR) + 1):
            filename = os.path.join(output_file_path,
                                    f"Profile_{config["rp_years"][i - 1]}y_{duration}h_ens{member_id}_proj{proj_id}.csv")
            if i < len(PR):
                df_prcp_threshold = df_prcp_water_company[(df_prcp_water_company['pr'] > PR[i - 1]) & (df_prcp_water_company['pr'] <= PR[i])]
            else:
                df_prcp_threshold = df_prcp_water_company[df_prcp_water_company['pr'] > PR[i - 1]]

            df_prcp_threshold = df_prcp_threshold.loc[:, ['latitude', 'longitude', 'pr']]

            df_prcp_threshold.insert(0, 'Time', df_prcp_threshold.index)
            df_prcp_threshold.dropna(subset=["pr"], inplace=True)
            df_prcp_threshold.columns = ['end date', 'lon', 'lat', 'pr_sum']

            df_prcp_threshold.insert(0, 'WCID', wcid)
            df_prcp_threshold.insert(0, 'Member', member_id)
            df_prcp_threshold.insert(0, 'Projection_slice_ID', proj_id)

            save(filename, df_prcp_threshold)
            df_prcp_threshold = None


def str_to_cftime360(time_str: str) -> cftime:
    """ 
    Apply string to cftime360 conversion to each item in an iterable 
    Turn '2024-09-15 00:30' into '2024-09-15-00-30' and then split by '-'
    Final result: ['2024', '09', '15', '00', '30']
    """
    year, month, day, hour, minute = map(int, time_str.replace(":", "-").replace(" ", "-").split("-"))
    return cftime.Datetime360Day(year, month, day, hour, minute)


def get_bin_counts(df_prcp: pd.DataFrame, 
                   year: int, 
                   month: int, 
                   member_id: int, 
                   duration: int, 
                   wcid: int,
                   output_file_path: str,
                   proj_id: str,
                   config: Dict):
    """
    Calculate rainfall counts for specified bins relevant to the chosen event duration (e.g. 1-h, 3-h, 6-h)
    For September, the first and second halves are counted separately for a reason that Kay explained to be (but I've forgotten)
    """
    BINS = {int(key): value for key, value in config["BINS"].items()}
    bins = BINS[duration]
    filename = os.path.join(output_file_path, f"Rainfall_bin_counts_{duration}h_ens{member_id}_proj{proj_id}.csv")
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Bin counts']

    total_count = []

    if month == 9:
        df_prcp_copy = df_prcp
        df_prcp_copy.insert(0, 'Time', df_prcp_copy.index)
        df_prcp_copy['Time'] = df_prcp_copy['Time'].apply(str_to_cftime360)
        mid_sept_date = cftime.Datetime360Day(year, month, 15, 0, 30, 0)
        start_sept_date = cftime.Datetime360Day(year, month, 1, 0, 30, 0)
        df_prcp_copy = df_prcp_copy[(df_prcp_copy['Time'] >= start_sept_date) & (df_prcp_copy['Time'] <= mid_sept_date)]
        for i in range(1, len(bins)):
            df_count = df_prcp_copy[(df_prcp_copy['pr_sum'] > bins[i - 1]) & (df_prcp_copy['pr_sum'] < bins[i])]
            total_count.append(df_count.shape[0])
        data_list = [proj_id, member_id, year, month, wcid, total_count]
        total_count_df = pd.DataFrame([data_list], columns=cols)
        save(filename, total_count_df)

        data_list = None
        total_count = None
        total_count_df = None
    
    else:
        for i in range(1, len(bins)):
            df_count = df_prcp[(df_prcp['pr_sum'] > bins[i - 1]) & (df_prcp['pr_sum'] < bins[i])]
            total_count.append(df_count.shape[0])
        data_list = [proj_id, member_id, year, month, wcid, total_count]
        total_count_df = pd.DataFrame([data_list], columns=cols)
        save(filename, total_count_df)

        data_list = None
        total_count = None
        total_count_df = None


def get_dry_days(ds_precip: xr.Dataset,
                 year: int, 
                 month: int, 
                 member_id: int, 
                 wcid: int,
                 proj_id: str,
                 output_file_path: str):
    """
    Dry day counts from UKCP18 daily precipitation data (xr.ds)
    Calculate the number of dry days per month for each grid point, and then calculate the mean
    Calls `save_dry_counts` to write data out to csv file 
    """

    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        if month == 9:
            start_sept = cftime.Datetime360Day(year, month, 15, 0, 30, 0)
            ds_sept = ds_precip.where((ds_precip['time'] <= start_sept) & (ds_precip['time'].dt.month == month), drop=True)
            daily_precip = ds_sept['pr'].resample(time="D").sum()
            da_dry_days_sept = daily_precip.where(daily_precip < 0.1)
            da_dry_day_count_sept = da_dry_days_sept.count(dim="time")
            save_dry_counts(da_dry_day_count_sept, year, 13, member_id, wcid, proj_id, output_file_path)
        else:
            ds_month = ds_precip.where(ds_precip['time'].dt.month == month, drop=True)
            daily_precip = ds_month['pr'].resample(time="D").sum()
            da_dry_days = daily_precip.where(daily_precip < 0.1)
            da_dry_day_count = da_dry_days.count(dim="time")
            save_dry_counts(da_dry_day_count, year, month, member_id, wcid, proj_id, output_file_path)


def get_month_total(ds_precip: xr.Dataset, 
                    year: int, 
                    month: int, 
                    member_id: int, 
                    wcid: int,
                    proj_id: str,
                    output_file_path: str):
    """
    This function calculates total monthly precip
    Calls `save_month_total' to write data out to csv file 
    """

    with dask.config.set(**{'array.slicing.split_large_chunks': False}):
        if month == 9:
            start_sept = cftime.Datetime360Day(year, month, 15, 0, 30, 0)
            ds_sept = ds_precip.where((ds_precip['time'] <= start_sept) & (ds_precip['time'].dt.month == month), drop=True)
            ds_sept_total = ds_sept.sum(dim='time')
            save_month_total(ds_sept_total, year, 13, member_id, wcid, proj_id, output_file_path)
        else:
            start = cftime.Datetime360Day(year, month, 1, 0, 30, 0)
            ds_month = ds_precip.where(ds_precip['time'] >= start, drop=True)
            ds_month_total = ds_month.sum(dim='time')
            save_month_total(ds_month_total, year, month, member_id, wcid, proj_id, output_file_path)


def save_dry_counts(ds_dry_days: xr.Dataset, 
                    year: int, 
                    month: int, 
                    member_id: int, 
                    wcid: int,
                    proj_id: str,
                    output_file_path: str):
    """ 
    Calculate the mean number of dry days in a given month over all grid points 
    Save this dry day count data to a csv file 
    Called by `get_dry_days`
    """
    filename = os.path.join(output_file_path, f"Dry_days_counts_ens{member_id}_proj{proj_id}.csv")
    ds_mask = ds_dry_days.mean()
    dry_list = [proj_id, member_id, year, month, wcid, ds_mask.values.tolist()]
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Mean dry day counts']
    dry_days_df = pd.DataFrame([dry_list], columns=cols)
    save(filename, dry_days_df)


def save_month_total(ds_month_total: xr.Dataset, 
                     year: int, 
                     month: int, 
                     member_id: int, 
                     wcid: int,
                     proj_id: str,
                     output_file_path: str):
    """ 
    Save total monthly precip to a csv file 
    Called by `get_month_total` 
    """
    filename = os.path.join(output_file_path, f"Total_rainfall_ens{member_id}_proj{proj_id}.csv")
    ds_mask = ds_month_total.mean()
    month_total_list = [proj_id, member_id, year, month, wcid, ds_mask.pr.values.tolist()]
    cols = ['Projection_slice_ID', 'Member', 'Year', 'Month', 'WCID', 'Mean total rainfall']
    month_total_df = pd.DataFrame([month_total_list], columns=cols)
    save(filename, month_total_df)


def get_previous_month_url(url: str) -> Tuple[int, int, str]: 
    """ 
    Extract month and year from a UKCP url, and output another url for the previous month 
    """
    match = re.search(r"(\d{4})(\d{2})\d{2}-\d{8}", url)
    if match:
        year = int(match.group(1))  # Extracted year
        month = int(match.group(2))  # Extracted month

        # Create a datetime object for the extracted year and month
        current_date = datetime(year, month, 1)

        # Subtract one month
        previous_month_date = current_date - relativedelta(months=1)

        # Get year and month of the previous month
        previous_year = previous_month_date.year
        previous_month = previous_month_date.month

        # Format the result as "YYYY-MM"
        previous_month_str = previous_month_date.strftime("%Y%m")
        updated_url = re.sub(r"(\d{4}\d{2})\d{2}-\d{8}", f"{previous_month_str}01-{previous_month_str}30", url)
        return previous_year, previous_month, updated_url
    else:
        raise ValueError("Date not found in URL!")


def get_start_year(year: int, 
                   month: int, 
                   hour: int):
    """ 
    This function provides a buffer around the selected date
    Starts the analysis on the 30th of the previous month
    (i.e. 30th June 1981 if the user chose July 1981)
    """
    if year != 1980:
        year1 = year #1981
        month1 = month - 1 #6
        if month == 1:
            year1 = year - 1
            month1 = 12
        start = cftime.Datetime360Day(year1, month1, 30, hour, 0, 0)
    else:
        month = 12
        start = cftime.Datetime360Day(year, month, 1, 0, 30, 0)

    return start


def call_main(proj_df: pd.DataFrame, 
              ukcp_url: str,
              config: Dict,
              output_file_path: str,
              member_id: int,
              var_id: str,
              proj_id: str,
              mask_1D: xr.Dataset
              ):
    """ 
    Call the main function to read in UKCP data for the chosen year and month 
    """

    year, month, previous_month_url = get_previous_month_url(ukcp_url)

    df_row = proj_df[(proj_df['Month'] == month) & (proj_df['Year'] == year)]
    if df_row.empty:
        print(f"No data found for Month: {month}, Year: {year}")

    if (year==1980 and month==12) or (year==2020 and month==12) or (year==2060 and month==12):
        input_file_path = [ukcp_url]
    else:
        input_file_path = [previous_month_url, ukcp_url]

    process_ukcp_data(input_file_path, 
                      output_file_path,
                      config,
                      year, 
                      month, 
                      member_id, 
                      var_id,
                      proj_id, 
                      mask_1D)


def check_dir(file_name: str):
    """ 
    check if a directory exists, and create one if not 
    """
    directory = os.path.dirname(file_name)
    if not os.path.exists(directory):
        os.makedirs(directory)


def save(file_name: str, 
         df: pd.DataFrame
         ):
    """ 
    save pandas dataframe as a csv 
    """
    check_dir(file_name)
    if os.path.isfile(file_name):
        df.to_csv(file_name, mode='a', header=False, index=False, float_format="%.2f")
    else:
        df.to_csv(file_name, mode='a', index=False, float_format="%.2f")


def rolling_window_sum(ds: xr.Dataset, 
                       window_size: int
                       ) -> xr.Dataset:
    """
    rolling window calculation for an xr.ds by defined window size (1-h, 3-h, 6-h, etc)
    """
    print(f"Starting calculation of rolling {str(window_size)}-h accumulated precip!")
    ds_window = ds.rolling(time=window_size, min_periods=window_size).construct("new").sum("new", skipna=True)
    print(f"Finished calculating rolling {str(window_size)}-h accumulated precip!")

    return ds_window
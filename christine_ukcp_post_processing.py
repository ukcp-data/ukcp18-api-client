"""
4_post_process.py
==================================

Purpose:  Python script for post-processing downloaded UKCP
          Local data for NI catchments. 
--------
Author:   Christine McKenna
--------
Created:  6 Oct 2023
--------
Pre-requisites: Conda installed packages -
                Create conda environment from 
                2023s0915-env_all_other.yml
--------
Usage: $ python 4_post_process.py <ens_id>

"""

# Import libraries
from os import listdir, makedirs
from os.path import join, exists
import xarray as xr
import numpy as np
import pandas as pd
import sys


# ====================== Define functions ======================


def netcdf_to_dataframe(ncfiles, var_id):
    """
    Load in each of the NetCDF files for a given variable (e.g., pr) and 
    concatenate into a pandas dataframe of dimensions (time, catchment_id)

    :param ncfiles: NetCDF files to process [list of strings]
    :param var_id: Variable to process [string]
    :return: pandas DataFrame object
    """

    # To hold dataframes for each file
    df_list = []
    nfiles = len(ncfiles)

    # Load in files one by one and append to df_list
    for i, file in enumerate(ncfiles):

        # To keep track of progress
        print(var_id,' file ',i+1,' of ',nfiles)

        # Open file as an xarray dataset. Do not decode time
        # coord values as default method does it incorrectly
        ds = xr.open_dataset(file, chunks=-1, decode_cf=False)

        # Correctly decode time coord into cftime format
        ds = ds.assign_coords(time=(np.round(ds.time,1)))
        ds = xr.decode_cf(ds)

        # Convert to pandas dataframe 
        df = ds.to_dataframe(dim_order=['time','catchment_ID'])
        
        # Get dataframe in right format
        df = df[var_id].unstack()

        # Append dataframe to list
        df_list.append(df)

    # Concatenate files in time into one dataframe
    df_concat = pd.concat(df_list)

    return df_concat


def dataframes_to_csv(df_pr, df_tas, outdir, ofile):
    """
    Takes dataframes of shape (time, catchment_id) for pr and
    tas variables and gets into form (time, pr, tas) per
    catchment ID for saving as csv.

    :param df_pr: pandas DataFrame of pr 
    :param df_tas: pandas DataFrame of tas
    :param outdir: Base directory for saving csv files [string]
    :param ofile: Base name for csv file
    :return: None
    """

    # Loop variables
    catch_IDs = df_pr.columns
    ncatch = len(catch_IDs)

    # Loop over catchments, combining dfs and saving to csv
    for i, catch_ID in enumerate(catch_IDs):

        print('Combining and saving for catchment ',i+1,' of ',ncatch)

        # Extract pr and tas data for current catchment
        df_pr_i = df_pr[catch_ID]
        df_tas_i = df_tas[catch_ID]

        # Combine into a single dataframe
        df_i = pd.concat([df_pr_i, df_tas_i], axis=1)
        df_i.columns = ['pr (mm/hour)', 'tas (celsius)']

        # Save as csv
        ofile_i = ofile+str(catch_ID)+'.csv'
        outdir_i = join(outdir, str(catch_ID))
        if not exists(outdir_i):
            makedirs(outdir_i)
        ofilepath = join(outdir_i, ofile_i) 
        df_i.to_csv(ofilepath)


def main(ens_id):
    """
    Main controller function.

    :param ens_id: Ensemble member ID [String]
    :return: None
    """

    # Setup input files
    indir = join('/mnt/metdata/2023s0915/OutputData', ens_id)
    pr_files = [join(indir, 'pr', f) for f in listdir(join(indir, 'pr'))]
    tas_files = [join(indir, 'tas', f) for f in listdir(join(indir, 'tas'))]

    # Setup output files
    outdir = join('/mnt/metdata/2023s0915/OutputData', 'catchment_csv')
    ofile = 'pr_tas_rcp85_land-cpm_uk_2.2km_'+ens_id+'_1hr_'+\
            '1980-2080_NIcatch_ave_ID'

    # Load in netcdf files and convert to dataframe
    print('\n\n=====================================================')
    print('\nLoading pr NetCDF files into a single DataFrame :\n\n')
    df_pr = netcdf_to_dataframe(pr_files, 'pr')
    print('\n\n=====================================================')
    print('\nLoading tas NetCDF data into a single DataFrame :\n\n')
    df_tas = netcdf_to_dataframe(tas_files, 'tas')

    # Combine df_pr and df_tas and save as csv
    print('\n\n=====================================================')
    print('\nCombining pr and tas DataFrames and saving to csv:\n\n')
    dataframes_to_csv(df_pr, df_tas, outdir, ofile)
    print('')


if __name__ == '__main__':

    args = sys.argv[1:2]
    main(*args)

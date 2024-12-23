#!/bin/bash
#
# Date created: Wed Oct 04 2023 16:24
#
# Purpose: Runs the 2_extract_UKCP_data_by_catchment.py script for 
#          each monthly file of hourly data in the UKCP Local 
#          CEDA archive from date_i to date_N (not inclusive). 
#          The python script extracts the UKCP data from CEDA
#          via OpenDAP, calculates catchment area-averages, and
#          saves the result to the MetData drive as a netcdf file.
#
#          This bash script is run for one variable (var_id) and 
#          ensemble member (ens_id) at a time. These are to be 
#          specified by the user before running.
#          
# Author: Christine McKenna
# Property: JBA Consulting
# ----------------------------------------------------------


# User changeable parameters
ens_id=01
var_id=pr


# Define set parameters
date_i=200012
date_N=200112

url0=https://dap.ceda.ac.uk/badc/ukcp18/data/land-cpm
url1=uk/2.2km/rcp85/${ens_id}/${var_id}/1hr/v20210615
url2=${var_id}_rcp85_land-cpm_uk_2.2km_${ens_id}_1hr
base_url=${url0}/${url1}/${url2}
cfile=/mnt/metdata/2024s1475/InputData/YearsMonths_byBinCounts_Rand_OtherYears.csv
outdir=/mnt/metdata/2024s1475/Dry_Days_Total_Rainfall_Dec2024/${ens_id}/${var_id}
base_ofile=${outdir}/${var_id}_rcp85_land-cpm_uk_2.2km_${ens_id}_1hr
mkdir -p $outdir


# Loop over monthly files, extracting and processing data
while [ $date_i != $date_N ]; do

  echo ""
  echo "---------------------------------------------------------"

  # Get date range for current month
  echo ""
  echo "Month: $date_i"
  date1=${date_i}01
  date2=${date_i}30

  # Get input and output file details
  url=${base_url}_${date1}-${date2}.nc
  ofile=${base_ofile}_${date1}-${date2}_NIcatch_ave.nc

  ## Run python script to extract and process data
  echo ""
  echo "Extracting and processing data for: $url"
  echo ""
  python ./ukcp18_download_and_processing.py $var_id $url $cfile $ofile

  # Move the current date foreward by 1 month
  date_i=$(date -d "${date_i}01 +1 month" +"%Y%m") 

done




#!/bin/bash
#
# Date created: Fri Oct 06 2023 14:45
#
# Purpose: Re-runs the 2_extract_UKCP_data_by_catchment.py script 
#          for any files that return errors when running 
#          2a_run_extract_UKCP_data_by_catchment_py_script.sh
#          via nohup. Only works for errors occurring due to
#          blips in the OpenDAP server connection. 
#
#          This bash script is run for one variable (var_id) and 
#          ensemble member (ens_id) at a time. These and the name
#          of the nohup file containing errors from a previous
#          batch job are to be specified by the user before running.
#
# Author: Christine McKenna
# Property: JBA Consulting
# ----------------------------------------------------------


# User changeable parameters
ens_id=15
var_id=tas
nohupfile=nohup_15_tas_rr.out


# Define set parameters
nohupfile_e=nohup_${ens_id}_${var_id}_errors.txt
efiles=${ens_id}_${var_id}_error_files.txt
cfile=/mnt/metdata/2023s0915/InputData/catchments/NIcatchments_raster.csv
outdir=/mnt/metdata/2023s0915/OutputData/${ens_id}/${var_id}


# Find files for which errors were encountered and save as list in $efiles
grep -B 5 "syntax error" $nohupfile > $nohupfile_e
grep -Eo "https://[a-zA-Z0-9./?=_%:-]*.nc" $nohupfile_e > $efiles


# Check number of files in $outdir
echo ""
echo ""
echo ""
echo "Number of files currently in $outdir (should be 1200):"
ls $outdir | wc -l
echo ""


# Loop over files encountered errors for, extracting and processing data
echo ""
echo "Now rerun extraction script for any error files ..."
echo ""
while read url; do

  echo ""
  echo "---------------------------------------------------------"

  # Get output file details
  ofile=${outdir}/$(basename "${url%.*}")_NIcatch_ave.nc

  # Run python script to extract and process data
  echo ""
  echo "Extracting and processing data for: $url"
  echo ""
  python ./2_extract_UKCP_data_by_catchment.py $var_id $url $cfile $ofile

done < $efiles       


# Check now have the right number of files in $outdir
echo ""
echo "---------------------------------------------------------"
echo ""
echo "Number of files in $outdir after running script:"
ls $outdir | wc -l
echo ""


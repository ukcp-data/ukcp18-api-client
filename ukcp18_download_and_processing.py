import ukcp18_processing_functions as processing
import pandas as pd
import os
import xarray as xr 
import json

def load_config(config_path: str):
    with open(config_path, 'r') as file: 
        config = json.load(file)
        return config 


def call_api_and_process_ukcp_data(ukcp_url: str, 
                                   out_file_path: str,
                                   projection_id: int,
                                   ensemble_member_id: int,
                                   variable_id: str,
                                   config_file_path: str): 
    """ 
    Process UKCP18 daily precipitation data for a given date (month + year), ensemble member and time slice 
    """

    config = load_config(config_file_path)
    mask_nc_filename = "UKWC_Cleaned_land-cpm_uk_2.2km.nc"
    csv_filename = "YearsMonths_byBinCounts_Rand_OtherYears.csv"

    profile_selected_month = pd.read_csv(os.path.join("./", csv_filename))
    mask_nc = os.path.join("./", mask_nc_filename)
    mask_orig = xr.open_dataset(mask_nc)
    mask_1D = mask_orig.stack(location=("grid_latitude", "grid_longitude"))

    projection_profile = profile_selected_month[profile_selected_month['Projection_slice_ID']
                                                == projection_id]
    
    processing.call_main(projection_profile, 
                         ukcp_url,
                         config, 
                         out_file_path, 
                         ensemble_member_id,
                         variable_id, 
                         projection_id,
                         mask_1D)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Download and process UKCP18 daily precipitation data using the CEDA API')
    parser.add_argument('--ukcp_url', metavar='path', required=True, 
                        help='URL to the UKCP18 data')
    parser.add_argument('--out_file_path', metavar='path', required=True,
                        help='output file path')
    parser.add_argument('--projection_id', metavar='value (int)', required=True,
                        help='climate simulation projection slice ID')
    parser.add_argument('--ensemble_member_id', metavar='value (int)', required=True,
                        help='ensemble member ID (1,4,5,...,12,13,15)')
    parser.add_argument('--variable_id', metavar='value (str)', required=True,
                        help='cf-compliant UKCP variable ID (e.g. pr,tas)')
    parser.add_argument('--config_file_path', metavar='path', required=True,
                        help='path to config file containing variables for function calls')
    args = parser.parse_args()
    call_api_and_process_ukcp_data(ukcp_url=args.ukcp_url, 
                                   out_file_path=args.out_file_path,
                                   projection_id=args.projection_id, 
                                   ensemble_member_id=args.ensemble_member_id,
                                   variable_id=args.variable_id,
                                   config_file_path=args.config_file_path)
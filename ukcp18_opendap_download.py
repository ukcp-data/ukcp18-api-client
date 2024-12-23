"""
ukcp18_opendap_download.py
===================

Python script for reading a NetCDF file remotely from the CEDA archive. It demonstrates fetching
and using a download token to authenticate access to CEDA Archive data, as well as how to load
and subset the Dataset from a stream of data (diskless), without having to download the whole file.

You will be prompted to provide your CEDA username and password the first time the script is run and
again if the token cached from a previous attempt has expired.

"""

import os
import xarray as xr
import json
import requests

from base64 import b64encode
from datetime import datetime, timezone
from getpass import getpass
from netCDF4 import Dataset
from urllib.parse import urlparse

# URL for the CEDA Token API service
TOKEN_URL = "https://services-beta.ceda.ac.uk/api/token/create/"
# Location on the filesystem to store a cached download token
TOKEN_CACHE = os.path.expanduser(os.path.join("~", ".cedatoken"))


def load_cached_token():
    """
    Read the token back out from its cache file.

    Returns a tuple containing the token and its expiry timestamp
    """

    # Read the token back out from its cache file
    try:
        with open(TOKEN_CACHE, "r") as cache_file:
            data = json.loads(cache_file.read())

            token = data.get("access_token")
            expires = datetime.strptime(data.get("expires"), "%Y-%m-%dT%H:%M:%S.%f%z")
            return token, expires

    except FileNotFoundError:
        return None, None


def get_token():
    """
    Fetches a download token, either from a cache file or
    from the token API using CEDA login credentials.

    Returns an active download token
    """

    # Check the cache file to see if we already have an active token
    token, expires = load_cached_token()

    # If no token has been cached or the token has expired, we get a new one
    now = datetime.now(timezone.utc)
    if not token or expires < now:

        if not token:
            print(f"No previous token found at {TOKEN_CACHE}. ", end="")
        else:
            print(f"Token at {TOKEN_CACHE} has expired. ", end="")
        print("Generating a fresh token...")

        print("Please provide your CEDA username: ", end="")
        username = input()
        password = getpass(prompt="CEDA user password: ")

        credentials = b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "ascii"
        )
        headers = {
            "Authorization": f"Basic {credentials}",
        }
        response = requests.request("POST", TOKEN_URL, headers=headers)
        if response.status_code == 200:

            # The token endpoint returns JSON
            response_data = json.loads(response.text)
            token = response_data["access_token"]

            # Store the JSON data in the cache file for future use
            with open(TOKEN_CACHE, "w") as cache_file:
                cache_file.write(response.text)

        else:
            print("Failed to generate token, check your username and password.")

    else:
        print(f"Found existing token at {TOKEN_CACHE}, skipping authentication.")

    return token, expires


def open_datasets(urls: list[str],
                  download_token=None
                  ):
    """ 
    Open a list of NetCDF datasets from specified URLs. 
    """

    datasets = []
    headers = None

    if download_token:
        headers = {"Authorization": f"Bearer {download_token}"}

    for url in urls:
        response = requests.request("GET", url, headers=headers, stream=True)
        if response.status_code != 200:
            print(
                f"Failed to fetch data. The response from the server was {response.status_code}"
            )
            return
        
        filename = os.path.basename(urlparse(url).path)
        print(f"Opening Dataset from file {filename} ...")
        datasets.append(Dataset(filename, memory=response.content))

    return datasets


def initiate_opendap_multiple_files(urls: list[str], 
                                    var_id: str
                                    ) -> xr.Dataset:
    """ 
    Initiate an API call to download UKCP18 data for multiple year-month selections 

    Returns an xarray.dataset, concatenated if necessary and chunked using dask to reduce memory usage
    """
    token, expires = get_token()
    if token:
        # Now that we have a valid token, we can attempt to open the Dataset from a URL.
        # This will only work if the token is associated with a CEDA user that has been granted
        # access to the data (i.e. if they can already download the file in a browser).
        # 
        print(f"Fetching information about variable '{var_id}':")
        if token:
            print((
                f"Using download token '{token[:5]}...{token[-5:]}' for authentication."
                f" Token expires at: {expires}."
            ))
        else:
            print("No DOWNLOAD_TOKEN found in environment.")

        nc_datasets = open_datasets(urls, download_token=token)

        xarray_datasets = []
        for nc_data in nc_datasets:
            if nc_data is None:
                continue
            ds = xr.open_dataset(xr.backends.NetCDF4DataStore(nc_data), chunks={"time": 30})
            xarray_datasets.append(ds)

        # combine datasets using xarray if required, and re-chunk
        if len(xarray_datasets) > 1:
            combined_ds = xr.concat(xarray_datasets, dim="time").chunk({"time": 60})
            return combined_ds
        elif xarray_datasets:
            return xarray_datasets[0]
        else:
            print("No datasets were opened!")
            return None
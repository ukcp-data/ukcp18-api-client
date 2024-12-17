import sys
sys.path.append("/home/shardy08/ukcp-api-client")

import os
os.environ["API_KEY"] = "NHdXiyGN-PrrrliBga-Anf5gHxrH4W1H"

from ukcp_api_client.client import UKCPApiClient
api_key = os.environ["API_KEY"]

# Variables to change
api_inputs = {
    # variables to change
    "time":"TemporalAverage=ann",
    #"area":"point|262500.0|187500.0",
    "area":"bbox|474459.24|241777.72|486311.19|246518.35",
    "collection":"land-prob",
    #"type":"absolute",
   # "ensemble":"land-rcm",
    "format":"netcdf",
    "timeslice":"1980|1981",
    "variable":"pr",
    "baseline":"b8100",
    "scenario":"rcp85",
#      unlikely to need to change

}

cli = UKCPApiClient(outputs_dir="my-outputs", api_key=api_key)

request_url = ("https://ukclimateprojections-ui.metoffice.gov.uk/wps?service=wps&request=Execute&version=1.0.0&Identifier=LS1_Plume_01&Format=text/xml&Inform=true&Store=false&Status=false&" +
               "DataInputs="+api_inputs["time"]+";" +
               "Area="+api_inputs["area"]+";" +
               "Collection="+api_inputs["collection"]+";" +
               #"ClimateChangeType="+api_inputs["type"]+";" +
               #"EnsembleMemberSet="+api_inputs["ensemble"]+";" +
               "DataFormat="+api_inputs["format"]+";" +
               "TimeSlice="+api_inputs["timeslice"]+";" +
               "Variable="+api_inputs["variable"]+";" +
               "Baseline="+api_inputs["baseline"]+";" +
               "Scenario="+api_inputs["scenario"]
               )

request_url_comparisons = "https://ukclimateprojections-ui.metoffice.gov.uk/wps?service=WPS&request=Execute&version=1.0.0&Identifier=LS3_Subset_01&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=TemporalAverage=jan;Area=bbox|474459.24|241777.72|486311.19|246518.35;Collection=land-rcm;ClimateChangeType=absolute;EnsembleMemberSet=land-rcm;DataFormat=csv;TimeSlice=2075|2076;Variable=psl"

print(request_url)

file_urls = cli.submit(request_url)

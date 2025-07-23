import dagster as dg

import os
import requests
import base64

from dagster_yyc_traffic.defs.partitions import daily_partition


@dg.asset(
        partitions_def=daily_partition
)
def traffic_incidents_file(context: dg.AssetExecutionContext) -> None:
    """
        The raw json files for traffic incidents dataset. Source from Calgary's Open Data Portal.
    """
    partition_date_str = context.partition_key
    
    app_token = os.getenv("APP_TOKEN")
    api_key_id = os.getenv("API_KEY_ID")
    api_key_secret = os.getenv("API_KEY_SECRET")

    headers ={}

    #check if app_token exists
    if app_token:
        headers["X-App-Token"] = app_token
    
    #check if api key and secret exist
    if api_key_id and api_key_secret:
        credentials = f"{api_key_id}:{api_key_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        headers["Authorization"] = f"Basic {encoded_credentials}"
    else:
        raise Exception("api key and/or api secrets is missing!")

    response = requests.get(
        f"https://data.calgary.ca/resource/35ra-9556.json?$where=date_trunc_ymd(modified_dt)='{partition_date_str}'",
        headers=headers
    )

    response.raise_for_status()

    with open(f"/app/data/raw/traffic_incidents_{partition_date_str}.json", "wb") as output_file:
        output_file.write(response.content)
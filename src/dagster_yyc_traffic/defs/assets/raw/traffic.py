import dagster as dg

import os
import requests
import base64

from dagster_yyc_traffic.defs.partitions import daily_partition
from dagster_yyc_traffic.defs.utils.api import CalgaryOpenDataApi


@dg.asset(
        partitions_def=daily_partition
)
def traffic_incidents_file(context: dg.AssetExecutionContext, calgary_open_data_api: CalgaryOpenDataApi) -> None:
    """
        The raw json files for traffic incidents dataset. Source from Calgary's Open Data Portal.
    """
    partition_date_str = context.partition_key
    
    result = calgary_open_data_api.traffic_incidents(date=partition_date_str)

    with open(f"/app/data/raw/traffic_incidents_{partition_date_str}.json", "wb") as output_file:
        output_file.write(result)
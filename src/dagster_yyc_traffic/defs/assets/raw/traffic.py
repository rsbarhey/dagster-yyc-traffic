import dagster as dg

import os
import requests

from dagster_yyc_traffic.defs.partitions import daily_partition


@dg.asset(
        partitions_def=daily_partition
)
def traffic_incidents_file(context: dg.AssetExecutionContext) -> None:
    """
        The raw json files for traffic incidents dataset. Source from Calgary's Open Data Portal.
    """
    partition_date_str = context.partition_key

    raw_data = requests.get(
        f"https://data.calgary.ca/resource/35ra-9556.json?$where=date_trunc_ymd(modified_dt)='{partition_date_str}'"
    )

    with open(f"/app/data/raw/traffic_incidents_{partition_date_str}.json", "wb") as output_file:
        output_file.write(raw_data.content)
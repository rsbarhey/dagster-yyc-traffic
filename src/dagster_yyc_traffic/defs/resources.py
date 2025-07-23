import dagster as dg
from dagster_yyc_traffic.defs.utils.api import CalgaryOpenDataApi

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "calgary_open_data_api": CalgaryOpenDataApi(
                app_token= dg.EnvVar("APP_TOKEN"),
                api_key_id= dg.EnvVar("API_KEY_ID"),
                api_key_secret=dg.EnvVar("API_KEY_SECRET")
            )
        }
    )
import requests
import base64
import dagster as dg

class CalgaryOpenDataApi(dg.ConfigurableResource):
    app_token: str
    api_key_id: str
    api_key_secret: str
    
    @property
    def url(self) -> str:
        return "https://data.calgary.ca"
    
    def traffic_incidents(self, date: str) -> bytes:
        headers = {}
        #check if app_token exists
        if self.app_token:
            headers["X-App-Token"] = self.app_token

        #check if api key and secret exist
        if self.api_key_id and self.api_key_secret:
            credentials = f"{self.api_key_id}:{self.api_key_secret}"
            encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
            headers["Authorization"] = f"Basic {encoded_credentials}"
        else:
            raise Exception("api key and/or api secrets is missing!")
        response = requests.get(
            f"{self.url}/resource/35ra-9556.json?$where=date_trunc_ymd(modified_dt)='{date}'",
            headers=headers
        )
        response.raise_for_status()

        return response.content
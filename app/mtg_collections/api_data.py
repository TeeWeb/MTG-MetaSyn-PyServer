from abc import ABC
import requests

###
# Abstract class for API data interfaces
###
class IAPIData(ABC):

    def __init__(self, endpoint):
        self.get_endpoint = endpoint

    def get(self) -> requests.Response:
        print('Requesting =>', self.get_endpoint)
        try:
            r = requests.get(self.get_endpoint, stream=True)
            return r
        except Exception as e:
            print(e)

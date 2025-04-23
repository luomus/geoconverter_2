import requests
from functools import lru_cache
import settings

@lru_cache
def get_settings():
  return settings.Settings()

def is_valid_download_request(id, person_token = None):
    url = get_settings().DW_URL + "/download-validate/" + id

    if get_settings().CONVERTER_ENV == 'testing':
      return True

    payload = None

    if person_token:
        payload = { "personToken": person_token }
  
    r = requests.get(url, params=payload)
    
    if r.status_code != 200:
      return False
    
    return True
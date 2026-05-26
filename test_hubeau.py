import requests

url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/obs_elab"
params = {"format": "json", "size": 3}
r = requests.get(url, params=params)
print(r.status_code)
print(r.text[:500])
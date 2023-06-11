import requests


#api_key = 'QC2536HY7XSBEJW4ZUTGF8PAZ'
api_key = "SF84FVM6HPDSD8MCSHHV7WA9Z"
#api_key = "YFR62GXR297CJW2NLGQFYPDAW"

def get_weather_data():
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/Paris/last7days?&key={api_key}"
    response = requests.get(url)
    # print the status code and response text
    data = response.json()

    return data

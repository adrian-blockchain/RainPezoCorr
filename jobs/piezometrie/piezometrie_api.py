from datetime import datetime, timedelta
import requests


code_bss = '01837A0096/F2'
size = 1000


def get_piezometrie_data():
    end_date_piezo = datetime.now()
    start_date = end_date_piezo - timedelta(days=13)

    # Convert datetime objects to string in the required format
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_piezo_str = end_date_piezo.strftime('%Y-%m-%d')

    url = f"http://hubeau.eaufrance.fr/api/v1/niveaux_nappes/chroniques?code_bss={code_bss}&date_debut_mesure={start_date_str}&date_fin_mesure={end_date_piezo_str}&size={size}"
    response = requests.get(url)
    data = response.json()
    return data
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import googlemaps
import os

app = FastAPI()

API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
gmaps = googlemaps.Client(key=API_KEY)

def get_address_from_csv(row, df):
    df_row = df.iloc[row]
    tradingName = df_row['tradingname']
    street = df_row['rua']
    number = df_row['numero']
    neighboor = df_row['bairro']
    city = df_row['cidade']
    state = df_row['estado']

    address = f'{city}-{state}, {street} Nº {number}'
    return (tradingName, address)

def get_coordinates_from_address(addr):
    geocode_results = gmaps.geocode(addr)
    coords = [(result['geometry']['location']['lat'], result['geometry']['location']['lng']) for result in geocode_results]
    return coords

def get_details_from_coordinates(coord, company_name):
    latitude, longitude = coord
    places = gmaps.places_nearby(location=(latitude, longitude), radius=1000, keyword=company_name)
    
    results = []
    for place in places['results']:
        place_id = place.get('place_id')
        if place_id:
            place_details = gmaps.place(place_id=place_id)['result']
            obj = {
                'status': place_details.get('business_status'),
                'phone': place_details.get('international_phone_number'),
                'address': place_details.get('formatted_address'),
                'name': place_details.get('name')
            }
            results.append(obj)
    return results

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    df['FoundPhone'] = 'Não encontrado'

    num_rows = len(df)
    num_it = min(num_rows, 1000)

    try:
        for i in range(num_it):
            (company_name, address) = get_address_from_csv(i, df)
            coords = get_coordinates_from_address(address)

            for coord in coords:
                info = get_details_from_coordinates(coord, company_name)
                if info:
                    phone = info[0]['phone']
                    if phone:
                        df.at[i, 'FoundPhone'] = phone
                    break  # Stop processing further coordinates if phone is found
            else:
                print(f'{company_name} não encontrada')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar linha {i}: {str(e)}")

    output_file = '/tmp/clinics-updated.csv'
    df.to_csv(output_file, index=False)
    return FileResponse(output_file, media_type='application/csv', filename='clinics-updated.csv')

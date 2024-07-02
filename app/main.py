from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import googlemaps
import os
import asyncio
from tenacity import retry, wait_fixed, stop_after_delay

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

async def get_coordinates_from_address_async(addr):
    loop = asyncio.get_event_loop()
    geocode_results = await loop.run_in_executor(None, gmaps.geocode, addr)
    coords = [(result['geometry']['location']['lat'], result['geometry']['location']['lng']) for result in geocode_results]
    return coords

async def get_details_from_coordinates_async(coord, company_name):
    latitude, longitude = coord
    loop = asyncio.get_event_loop()
    places = await loop.run_in_executor(None, gmaps.places_nearby, (latitude, longitude), 1000, company_name)
    
    results = []
    for place in places['results']:
        place_id = place.get('place_id')
        if place_id:
            place_details = await loop.run_in_executor(None, gmaps.place, place_id)
            obj = {
                'status': place_details.get('business_status'),
                'phone': place_details.get('international_phone_number'),
                'address': place_details.get('formatted_address'),
                'name': place_details.get('name')
            }
            results.append(obj)
    return results

@retry(wait=wait_fixed(20), stop=stop_after_delay(600))  # Retry every 20 seconds, stop after 10 attempts (10 * 20s = 200s)
async def process_row(i, df):
    try:
        (company_name, address) = get_address_from_csv(i, df)
        coords = await get_coordinates_from_address_async(address)

        for coord in coords:
            info = await get_details_from_coordinates_async(coord, company_name)
            if info:
                phone = info[0]['phone']
                if phone:
                    df.at[i, 'FoundPhone'] = phone
                break  # Stop processing further coordinates if phone is found
        else:
            print(f'{company_name} não encontrada')

    except googlemaps.exceptions.Timeout:
        print(f"Timeout ao processar linha {i}. Tentando novamente em 20 segundos...")
        raise  # Re-raise the exception to trigger retry

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar linha {i}: {str(e)}")

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    df['FoundPhone'] = 'Não encontrado'

    num_rows = len(df)
    num_it = min(num_rows, 1000)

    async def process_with_retry(i):
        await process_row(i, df)

    tasks = [process_with_retry(i) for i in range(num_it)]
    await asyncio.gather(*tasks)

    output_file = '/tmp/clinics-updated.csv'
    df.to_csv(output_file, index=False)
    return FileResponse(output_file, media_type='application/csv', filename='clinics-updated.csv')

from fastapi import FastAPI, File, UploadFile
import pandas as pd
import googlemaps
import os

app = FastAPI()

API_KEY = 'YOUR_API_KEY'
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
    coords = []
    geocode_results = gmaps.geocode(addr)

    for geocode_result in geocode_results:
        latitude = geocode_result['geometry']['location']['lat']
        longitude = geocode_result['geometry']['location']['lng']
        coords.append((latitude, longitude))
    
    return coords

def get_details_from_coordinates(coord, company_name):
    latitude, longitude = coord

    places = gmaps.places_nearby(location=(latitude, longitude), radius=1000, keyword=company_name)
    results = []
    for place in places['results']:
        place_id = place['place_id'] if 'place_id' in place else None
        if place_id:
            place_details = gmaps.place(place_id=place_id)['result']
            status = place_details['business_status'] if 'business_status' in place_details else None
            phone = place_details['international_phone_number'] if 'international_phone_number' in place_details else None
            addr = place_details['formatted_address'] if 'formatted_address' in place_details else None
            name = place_details['name'] if 'name' in place_details else None
            obj = {
                'status': status,
                'phone': phone,
                'address': addr,
                'name': name
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
                    phone = info[0]['phone'] if 'phone' in info[0] else None
                    if phone:
                        df.at[i, 'FoundPhone'] = phone
                else:
                    print(f'{company_name} nao encontrada')
    except Exception as e:
        return {"error": str(e)}

    output_file = 'clinics-updated.csv'
    df.to_csv(output_file, index=False)
    return FileResponse(output_file, media_type='application/csv', filename=output_file)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import FastAPI, File, UploadFile
import pandas as pd
import googlemaps
import os

app = FastAPI()

API_KEY = 'YOUR_API_KEY'
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
    coords = []
    geocode_results = gmaps.geocode(addr)

    for geocode_result in geocode_results:
        latitude = geocode_result['geometry']['location']['lat']
        longitude = geocode_result['geometry']['location']['lng']
        coords.append((latitude, longitude))
    
    return coords

def get_details_from_coordinates(coord, company_name):
    latitude, longitude = coord

    places = gmaps.places_nearby(location=(latitude, longitude), radius=1000, keyword=company_name)
    results = []
    for place in places['results']:
        place_id = place['place_id'] if 'place_id' in place else None
        if place_id:
            place_details = gmaps.place(place_id=place_id)['result']
            status = place_details['business_status'] if 'business_status' in place_details else None
            phone = place_details['international_phone_number'] if 'international_phone_number' in place_details else None
            addr = place_details['formatted_address'] if 'formatted_address' in place_details else None
            name = place_details['name'] if 'name' in place_details else None
            obj = {
                'status': status,
                'phone': phone,
                'address': addr,
                'name': name
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
                    phone = info[0]['phone'] if 'phone' in info[0] else None
                    if phone:
                        df.at[i, 'FoundPhone'] = phone
                else:
                    print(f'{company_name} nao encontrada')
    except Exception as e:
        return {"error": str(e)}

    output_file = 'clinics-updated.csv'
    df.to_csv(output_file, index=False)
    return FileResponse(output_file, media_type='application/csv', filename=output_file)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

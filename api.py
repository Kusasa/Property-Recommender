# -----------------------------Import Libraries-----------------------------
from propertyRecommender import *
from typing import Union
from fastapi import FastAPI
from fastapi.responses import JSONResponse


# -----------------------------Start App------------------------------------
app = FastAPI()


# -----------------------------Define functions----------------------------------
@app.get("/property_recommender/v1.0/")
def read_root():
    return "This tool fetches and provides you with a list of residential properties which are viable for you to rent, based on your Workplace location, Travel time to your workplace, Rental property needs."

@app.get("/property_recommender/v1.0/travel_bounds")
def travel_bounds(address: Union[str, None] = None, mode: Union[str, None] = None, maximum_travel_time_minutes: Union[int, None] = None):
    lat, lon = geocoding(address)
    office_gdf, travelTimeBounds_gdf = travelBounds(lat,lon,mode,maximum_travel_time_minutes)
    return JSONResponse(content = travelTimeBounds_gdf.to_wkt().to_dict('records'))

@app.get("/property_recommender/v1.0/mean_price")
def mean_price(address: Union[str, None] = None, mode: Union[str, None] = None, maximum_travel_time_minutes: Union[int, None] = None, max_price: Union[int, None] = None \
    , min_bedrooms: Union[int, None] = None, max_bedrooms: Union[int, None] = None, min_bathrooms: Union[int, None] = None, max_bathrooms: Union[int, None] = None, property_type_code: Union[str, None] = None):
    lat, lon = geocoding(address)
    office_gdf, travelTimeBounds_gdf = travelBounds(lat,lon,mode,maximum_travel_time_minutes)
    filtered_properties_gdf = fetch_ppData(travelTimeBounds_gdf,max_price,min_bedrooms,max_bedrooms,min_bathrooms,max_bathrooms,property_type_code)
    mean, std = meanPrice(filtered_properties_gdf)
    return {"Mean Price (R)": str(mean), "Price Standard Deviation (R)": str(std)}

@app.get("/property_recommender/v1.0/target_areas")
def target_areas(address: Union[str, None] = None, mode: Union[str, None] = None, maximum_travel_time_minutes: Union[int, None] = None, max_price: Union[int, None] = None \
    , min_bedrooms: Union[int, None] = None, max_bedrooms: Union[int, None] = None, min_bathrooms: Union[int, None] = None, max_bathrooms: Union[int, None] = None, property_type_code: Union[str, None] = None):
    lat, lon = geocoding(address)
    office_gdf, travelTimeBounds_gdf = travelBounds(lat,lon,mode,maximum_travel_time_minutes)
    filtered_properties_gdf = fetch_ppData(travelTimeBounds_gdf,max_price,min_bedrooms,max_bedrooms,min_bathrooms,max_bathrooms,property_type_code)
    targetAreas_df = targetedAreas(filtered_properties_gdf)
    return JSONResponse(content = targetAreas_df.to_dict('records'))

@app.get("/property_recommender/v1.0/target_properties")
def target_properties(address: Union[str, None] = None, mode: Union[str, None] = None, maximum_travel_time_minutes: Union[int, None] = None, max_price: Union[int, None] = None \
    , min_bedrooms: Union[int, None] = None, max_bedrooms: Union[int, None] = None, min_bathrooms: Union[int, None] = None, max_bathrooms: Union[int, None] = None, property_type_code: Union[str, None] = None):
    lat, lon = geocoding(address)
    office_gdf, travelTimeBounds_gdf = travelBounds(lat,lon,mode,maximum_travel_time_minutes)
    filtered_properties_gdf = fetch_ppData(travelTimeBounds_gdf,max_price,min_bedrooms,max_bedrooms,min_bathrooms,max_bathrooms,property_type_code)
    return JSONResponse(content = filtered_properties_gdf.to_wkt().to_dict('records'))
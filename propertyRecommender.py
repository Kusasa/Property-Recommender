# -----------------------------Import Libraries-----------------------------
from bs4 import BeautifulSoup
import contextily as ctx
import geopandas as gpd
import json
from matplotlib import pyplot as plt
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
import numpy as np
import pandas as pd
import requests
from Secrets import *
import sys

pd.set_option('display.max_colwidth', None)

max_rec = 0x100000
#resource.setrlimit(resource.RLIMIT_STACK, [0x100 * max_rec, resource.RLIM_INFINITY])   # 0x100 is a guess at the size of each stack frame.
sys.setrecursionlimit(max_rec)


# -----------------------------Import Reference Data-------------------------
# Get general suburbs data
response_json = requests.get(generalSurburbData).json()

with open("sa_suburbs.geojson","w") as outfile:
  json.dump(response_json, outfile)

sa_burbs_gdf = gpd.read_file("sa_suburbs.geojson")
sa_burbs_gdf = sa_burbs_gdf.dissolve(by='suburbname').reset_index().to_crs(3857)

# Get PP suburbs data
response_text = requests.get(ppSurburbData).text

with open("pp_suburbs.csv","w") as outfile:
  outfile.write(response_text)

suburbs_df = pd.read_csv("pp_suburbs.csv").drop(columns=['Unnamed: 0'])


# -----------------------------Default Parameters-------------------------------
apiKey =  secretToken
property_types = {
    	'Houses':5,
    	'Flats & Apartments':2,
    	'Townhouse & Clusters':10,
    	'Land':7,
    	'Farms & Smallholdings':1,
    	'Garden Cottage':3
    	}


# -----------------------------Define functions----------------------------------
def geocoding(address):
    address_String = address.replace(" ","%20").replace(",","%2C")  + "%2CSouth%20Africa"
    url = f"https://api.geoapify.com/v1/geocode/search?text={address_String}&format=json&apiKey={apiKey}"
    response = requests.get(url)

    x_coord = response.json()['results'][0]['lon']
    y_coord = response.json()['results'][0]['lat']

    return y_coord, x_coord

def travelBounds(lat,lon,mode,maximum_travel_time_minutes):
    # Office shapefile for the sake of plotting
    office_gdf = gpd.GeoDataFrame({'Name': ['Office']},
                                  geometry = gpd.points_from_xy(x=[float(lon)], y=[float(lat)]), crs='EPSG:4326').to_crs(3857)
    
    # Fetch the travel area
    maximum_travel_time_seconds = 60 *  maximum_travel_time_minutes
    url = f"https://api.geoapify.com/v1/isoline?lat={lat}&lon={lon}&type=time&mode={mode}&range={maximum_travel_time_seconds}&apiKey={apiKey}"
    response = requests.get(url)
    
    travelTimeBounds_gdf = gpd.GeoDataFrame.from_features(response.json()['features'])[['id', 'geometry']]
    travelTimeBounds_gdf.crs = 4326
    travelTimeBounds_gdf = travelTimeBounds_gdf.to_crs(epsg=3857)

    return office_gdf, travelTimeBounds_gdf

def fetch_ppData(travelBounds,max_price,min_bedrooms,max_bedrooms,min_bathrooms,max_bathrooms,property_type_code):
    # Clips suburbs geometry by the TravelTime Bound
    sa_burbs_gdf['geometry'] = sa_burbs_gdf.buffer(0.0)
    bounded_burbs_gdf = gpd.clip(sa_burbs_gdf,travelBounds.buffer(0.0)).reset_index(drop=True)
    bounded_burbs_gdf.crs = 3857
    
    # Inner join target suburb geometry to private property data
    bounded_burbs_gdf['suburbname'] = bounded_burbs_gdf.suburbname.str.lower()
    selected_suburbs_gdf = bounded_burbs_gdf.merge(suburbs_df, left_on='suburbname', right_on='Suburb')
    selected_suburbs_gdf = selected_suburbs_gdf[['Province',	'Region_Name',	'Area_Name',	'Suburb',	'URL_Endpoint', 'geometry']]
    
    # Fetch the target rental properties
    base_url = 'https://www.privateproperty.co.za'
    url_arguments = f"?tp={max_price}&bd={min_bedrooms}&pt={property_type_code}&ba={min_bathrooms}"
    
    # Property data for selected Suburbs  
    def output_builder(gdf_chunk):
        outcome_gdf = gpd.GeoDataFrame(columns=['Province',	'Region',	'Area',	'Suburb',	'geometry', 'Title', 'Prop_Type', 'Price', 'Deposit', 'Bedrooms', 'Bathrooms', 'Address', 'URL'])
        for suburb_index, suburb_row in gdf_chunk.iterrows():
            suburb_endpoint = suburb_row.URL_Endpoint
            
            suburb_url = base_url + suburb_endpoint + url_arguments
            suburb_response = requests.get(suburb_url)
            suburb_soup = BeautifulSoup(suburb_response.text, 'html.parser')
            
            properties = suburb_soup.find_all('a', class_ = "listingResult row")
            for propert in properties:
                property_info = propert.find('div', class_ = "infoHolder")
                bedroom = np.nan
                bathroom = np.nan
                for index, div in enumerate(property_info.find('div', class_ = "features row").find_all('div')):
                    if len(div.attrs['class']) == 2:
                        if div.attrs['class'][1] == 'bedroom':
                            bedroom = property_info.find('div', class_ = "features row").find_all('div')[index - 1].get_text()
                        elif div.attrs['class'][1] == 'bathroom':
                            bathroom = property_info.find('div', class_ = "features row").find_all('div')[index - 1].get_text()
                
                row = pd.DataFrame([
                        {'Province': suburb_row.Province,
                         'Region': suburb_row.Region_Name,
                         'Area': suburb_row.Area_Name,
                         'Suburb': suburb_row.Suburb,
                         'geometry': suburb_row.geometry,
                         'Title': property_info.find('div', class_ = "title").get_text(),
                         'Prop_Type': property_info.find('div', class_ = "propertyType").get_text(),
                         'Price': property_info.find('div', class_ = "priceDescription").get_text(),
                         'Deposit': property_info.find('div', class_ = "priceAdditionalDescriptor").get_text(),
                         'Bedrooms': bedroom,
                         'Bathrooms': bathroom,
                         'Address': property_info.find('div', class_ = "address"),
                         'URL': base_url + propert.get_attribute_list(key='href')[0]}
                         ])
                if row.Address is not None:
                    row['Address'] = row.Address[0]
                outcome_gdf = outcome_gdf.append(row).reset_index(drop=True)

        return outcome_gdf
    
    num_processes = multiprocessing.cpu_count()
    chunk_size = int(selected_suburbs_gdf.shape[0]/num_processes)
    chunks = [selected_suburbs_gdf.iloc[selected_suburbs_gdf.index[i:i + chunk_size]] for i in range(0, selected_suburbs_gdf.shape[0], chunk_size)]

    with ThreadPool(num_processes) as p:
        result = p.map(output_builder, chunks)
    properties_gdf = gpd.GeoDataFrame(pd.concat(result, ignore_index=True))
    
    properties_gdf.sort_index(inplace=True)
    
    # Clean the retrieved property dataframe
    properties_gdf['Price'] = properties_gdf.Price.str.lstrip('R ').str.replace(' ', '').astype(int)
    properties_gdf['Bedrooms'] = properties_gdf.Bedrooms.fillna(-999).astype(float)
    properties_gdf['Bathrooms'] = properties_gdf.Bathrooms.fillna(-999).astype(float)
    properties_gdf['Area'] = properties_gdf.Area.fillna(properties_gdf.Region)
    
    # Filter geodataframe by mandatory dimensions
    filtered_properties_gdf = properties_gdf[properties_gdf.Bedrooms <= max_bedrooms]
    filtered_properties_gdf = filtered_properties_gdf[filtered_properties_gdf.Bathrooms <= max_bathrooms]
    
    filtered_properties_gdf.reset_index(drop=True, inplace=True)
    filtered_properties_gdf.crs = 3857

    return filtered_properties_gdf

def mapAreas(office, travelBounds, fp_gdf, maximum_travel_time_minutes):
    yaxis_r_factor = 2000
    xaxis_r_factor = yaxis_r_factor * 3 * ((travelBounds.bounds.maxy - travelBounds.bounds.miny) / (travelBounds.bounds.maxx - travelBounds.bounds.minx))
    fig, base = plt.subplots(figsize=(10,11))
    travelBounds.plot(ax=base, facecolor='none', edgecolor='purple', label='{}-minutes Travel Time boundary'.format(maximum_travel_time_minutes))
    fp_gdf.plot(ax=base, facecolor='green', edgecolor='black', alpha=0.3, label="Properties")
    office.plot(ax=base, marker='o', facecolor='purple', edgecolor='purple', markersize=40, label='Target Workplace')
    base.set_axis_off()
    base.set_ylim(top=(travelBounds.bounds.maxy + yaxis_r_factor - 100).item(), bottom=(travelBounds.bounds.miny - yaxis_r_factor + 100).item())
    base.set_xlim(left=(travelBounds.bounds.minx - xaxis_r_factor + 100).item(), right=(travelBounds.bounds.maxx + xaxis_r_factor - 100).item())
    base.text(office.geometry.x+200, office.geometry.y+300, "Workplace", fontweight=1000, color='red',fontsize=11)
    base.text(office.geometry.x+200, office.geometry.y+300, "Workplace", fontweight=100, color='black',fontsize=12)
    ctx.add_basemap(base, source=ctx.providers.OpenStreetMap.Mapnik)
    return fig

def targetedAreas(fp_gdf):
    tA_df = fp_gdf.dissolve('Area', as_index=False, aggfunc="count")[["Area","URL"]].rename(columns ={'URL':'Count'}).sort_values("Count",ascending=False,ignore_index=True)
    return tA_df

def meanPrice(fp_gdf):
    mean = np.round_(np.mean(fp_gdf.Price)).astype(int)
    std = np.round_(np.std(fp_gdf.Price)).astype(int)
    return mean, std

def make_clickable(row):
    return f'<a target="_blank" href="{row.URL}">{row.Title}</a>'

filter = None

def drawTable(fp_gdf, f1=None):
    filtered_properties_table = pd.DataFrame(fp_gdf[["Title","Bedrooms","Bathrooms","Price","Suburb","Area","Region","URL"]])
    if f1 is not None:
        table = filtered_properties_table[filtered_properties_table["Area"].isin(f1)]
    else:
        table = filtered_properties_table
    table['Title'] = table.apply(make_clickable, axis=1)
    table.drop(columns=['URL'], inplace=True)
    table.rename(columns={"Title": "Rental", "Price":"Price (R)"}, inplace=True)
    table = table.to_html(escape=False)
    return table

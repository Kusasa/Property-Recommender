# -----------------------------Import Libraries-----------------------------
from propertyRecommender import *
import streamlit as st

# -----------------------------Start App------------------------------------
st.write("""
# Viable Residential Property to Rent
This tool fetches and provides you with a list of residential properties which are viable for you to rent, based on your:

    - Workplace location,
    - Travel time to your workplace,
    - Rental property needs.

""")


# -----------------------------Define functions----------------------------------
def user_inputs():
    global address, mode, maximum_travel_time_minutes, maximum_travel_time_seconds, max_price, min_bedrooms, max_bedrooms, \
        min_bathrooms, max_bathrooms, property_type_code
    # Travel Filters
    #x_coord = st.sidebar.text_input('Longitude (decimal degrees in epsg4326)', 28.06144872313252)
    #y_coord = st.sidebar.text_input('Latitude (decimal degrees in epsg4326)', -26.042370716139192)
    address = st.sidebar.text_input('Address (i.e. 161 Maude Street, Sandown, Sandton, Gauteng)')
    mode = st.sidebar.selectbox('Travel Mode', ["drive", "bicycle", "walk", "approximated_transit"], 0)
    maximum_travel_time_minutes = st.sidebar.number_input('Maximum Average Travel Time (minutes)', 10)
    maximum_travel_time_seconds = 60 *  maximum_travel_time_minutes
    
    # Property Filters
    ###property_type = st.sidebar.selectbox('Property Type', ['Houses','Flats & Apartments','Townhouse & Clusters','Land','Farms & Smallholdings','Garden Cottage'],1)
    property_type = st.sidebar.selectbox('Property Type', ['Houses','Flats & Apartments','Townhouse & Clusters','Garden Cottage'],1)
    property_type_code = str(property_types[property_type])
    max_price = st.sidebar.number_input('Maximum Average Travel Time (minutes)', 6000)
    min_bedrooms = st.sidebar.slider('Minimum number of Bedrooms', 1, 6, 1, 1)
    max_bedrooms = st.sidebar.slider('Maximum number of Bedrooms', 1, 6, 2, 1)
    min_bathrooms = st.sidebar.slider('Minimum number of Bathrooms', 1, 3, 1, 1)
    max_bathrooms = st.sidebar.slider('Maximum number of Bathrooms', 1, 3, 1, 1)

geocoding = st.cache(geocoding)

travelBounds = st.cache(travelBounds, allow_output_mutation=True)

fetch_ppData = st.cache(fetch_ppData)

def listAreas(targetAreas):
    st.write("Target Areas (property count):")
    blocks  = [targetAreas.iloc[targetAreas.index[i:i + 3]] for i in range(0, targetAreas.shape[0], 3)]
    col1, col2, col3 = st.columns(3)
    with col1:
        if len(blocks) >= 1:
            for index,row in blocks[0].iterrows():
                st.write(f"\t{row.Area} ({row.Count})")
        pass
    with col2:
        if len(blocks) >= 2:
            for index,row in blocks[1].iterrows():
                st.write(f"\t{row.Area} ({row.Count})")
        pass
    with col3:
        if len(blocks) == 3:
            for index,row in blocks[2].iterrows():
                st.write(f"\t{row.Area} ({row.Count})")
        pass

def tableFilter(fp_gdf):
    st.subheader("Table of target rental properties")
    with st.expander("Filter table below:"):
        filter1 = st.multiselect('Area', fp_gdf.Area.unique(), fp_gdf.Area.unique())
    return filter1

def outputs():
    st.subheader('Target areas and their rental property density with your required attributes within the target locality')
    figure = mapAreas(office_gdf, travelTimeBounds_gdf, filtered_properties_gdf, maximum_travel_time_minutes)
    st.pyplot(figure)
    st.subheader("List of target areas")
    targetAreas_df = targetedAreas(filtered_properties_gdf)
    st.write("There are {} target areas within a {} minutes {} time from your target workplace.\n".format(len(targetAreas_df), maximum_travel_time_minutes, mode))
    listAreas(targetAreas_df)
    st.subheader("Price of target rentals")
    mean, std = meanPrice(filtered_properties_gdf)
    st.write("The price of my target rental properties: R{} +/-R{}".format(mean,std))
    #filter = tableFilter(filtered_properties_gdf)
    table = drawTable(filtered_properties_gdf)
    st.write(table, unsafe_allow_html=True)

# -----------------------------User Inputs--------------------------------------
st.sidebar.header('User Input Parameters')
user_inputs()

# -----------------------------Run the app----------------------------------------
button_sent = st.button("Run")

if button_sent:
    lat, lon = geocoding(address)
    if (lat is not None) and (lon is not None):
        office_gdf, travelTimeBounds_gdf = travelBounds(lat,lon,mode,maximum_travel_time_minutes)
        filtered_properties_gdf = fetch_ppData(travelTimeBounds_gdf,max_price,min_bedrooms,max_bedrooms,min_bathrooms,max_bathrooms,property_type_code)
        # The outputs
        outputs()

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='zpid',
    merge_update_columns=[
        'address',
        'detailurl',
        'lotid',
        'imgsrc',
        'price',
        'zip',
        'buildingname',
        'propertytype',
        'bathrooms',
        'bedrooms',
        'livingarea',
        'longitude',
        'latitude',
        'unit',
        'units',
        'apartment_added_dt',
        'updated_at'
    ],
    post_hook="TRUNCATE TABLE raw.raw_new_zillow_apartments"
) }}

WITH daily_data AS (
    SELECT
        zpid,
        address,
        detailurl,
        lotid,
        imgsrc,
        price,
        zip,
        buildingname,
        propertytype,
        bathrooms,
        bedrooms,
        livingarea,
        longitude,
        latitude,
        unit,
        units,
        apartment_added_dt,
        CURRENT_TIMESTAMP() AS updated_at -- Use current timestamp for updates
    FROM {{ source('raw', 'raw_new_zillow_apartments') }}
)

SELECT
    zpid,
    address,
    detailurl,
    lotid,
    imgsrc,
    price,
    zip,
    buildingname,
    propertytype,
    bathrooms,
    bedrooms,
    livingarea,
    longitude,
    latitude,
    unit,
    units,
    apartment_added_dt,
    updated_at
FROM daily_data

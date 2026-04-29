import os
from google.cloud import bigquery
import pandas as pd

def fetch_mlab_data_alameda_2020(project_id: str, output_csv: str = 'mlab_raw_alameda_2020.csv'):
    """
    Fetches raw M-Lab NDT data (both downloads and uploads) for Alameda County ZCTAs for the year 2020.
    
    Requirements:
    1. A Google Cloud Project (project_id)
    2. Authentication set up (e.g., via `gcloud auth application-default login`)
    """
    client = bigquery.Client(project=project_id)

    # We fetch data month-by-month to provide progress updates and prevent timeouts
    months = [
        ('2020-01-01', '2020-01-31'),
        ('2020-02-01', '2020-02-29'),
        ('2020-03-01', '2020-03-31'),
        ('2020-04-01', '2020-04-30'),
        ('2020-05-01', '2020-05-31'),
        ('2020-06-01', '2020-06-30'),
        ('2020-07-01', '2020-07-31'),
        ('2020-08-01', '2020-08-31'),
        ('2020-09-01', '2020-09-30'),
        ('2020-10-01', '2020-10-31'),
        ('2020-11-01', '2020-11-30'),
        ('2020-12-01', '2020-12-31')
    ]

    all_data = []

    for start_date, end_date in months:
        print(f"Fetching data for {start_date} to {end_date}...")
        
        # We query BOTH unified_downloads and unified_uploads and combine them with UNION ALL
        query = f"""
        WITH alameda_zctas AS (
            SELECT zip_code, zip_code_geom
            FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
            WHERE state_code = 'CA'
        ),
        raw_tests AS (
            -- DOWNLOADS
            SELECT
                date,
                'download' AS test_type,
                client.Geo.Latitude AS lat,
                client.Geo.Longitude AS lon,
                a.MeanThroughputMbps AS speed_mbps,
                a.MinRTT AS latency_ms
            FROM `measurement-lab.ndt.unified_downloads`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND client.Geo.CountryCode = 'US'
                AND client.Geo.Latitude BETWEEN 37.45 AND 37.90
                AND client.Geo.Longitude BETWEEN -122.35 AND -121.45
            
            UNION ALL
            
            -- UPLOADS
            SELECT
                date,
                'upload' AS test_type,
                client.Geo.Latitude AS lat,
                client.Geo.Longitude AS lon,
                a.MeanThroughputMbps AS speed_mbps,
                a.MinRTT AS latency_ms
            FROM `measurement-lab.ndt.unified_uploads`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND client.Geo.CountryCode = 'US'
                AND client.Geo.Latitude BETWEEN 37.45 AND 37.90
                AND client.Geo.Longitude BETWEEN -122.35 AND -121.45
        )
        SELECT
            z.zip_code AS zcta,
            r.date AS test_date,
            r.test_type,
            r.speed_mbps,
            r.latency_ms
        FROM raw_tests r
        JOIN alameda_zctas z
        ON ST_WITHIN(ST_GEOGPOINT(r.lon, r.lat), z.zip_code_geom)
        """
        
        query_job = client.query(query)
        df = query_job.to_dataframe()
        all_data.append(df)
        print(f"  -> Found {len(df)} tests in Alameda ZCTAs for this month.")

    # Combine all raw tests
    print("Combining all months together...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Save to CSV
    final_df.to_csv(output_csv, index=False)
    print(f"Saved {len(final_df)} raw test results to {output_csv}")
    print("You can now use Pandas to calculate medians, standard deviations, and IQR directly from this file!")

if __name__ == "__main__":
    # Your project ID from earlier
    YOUR_GCP_PROJECT_ID = "directed-asset-494823-t5"
    
    if YOUR_GCP_PROJECT_ID == "":
        print("Please edit the script to include your GCP Project ID.")
    else:
        fetch_mlab_data_alameda_2020(YOUR_GCP_PROJECT_ID)

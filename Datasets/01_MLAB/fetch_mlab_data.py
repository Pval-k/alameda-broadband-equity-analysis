import os
from google.cloud import bigquery
import pandas as pd

def fetch_mlab_data_alameda_2020(project_id: str, output_csv: str = 'mlab_alameda_2020.csv'):
    """
    Fetches M-Lab NDT data for Alameda County ZCTAs for the year 2020.
    
    Requirements:
    1. A Google Cloud Project (project_id)
    2. Authentication set up (e.g., via `gcloud auth application-default login`)
    3. Joined the `mlab-discuss` Google Group to query measurement-lab project for free.
    """
    client = bigquery.Client(project=project_id)

    # Note: We use bigquery-public-data.geo_us_boundaries.zip_codes to map 
    # M-Lab's lat/lon points to ZCTAs, filtering to zip codes in Alameda County.
    # We join this with the measurement-lab.ndt.unified_downloads table.
    # To avoid taking 10+ minutes and timing out, we'll fetch data month-by-month
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
        
        query = f"""
        WITH alameda_zctas AS (
            SELECT zip_code, zip_code_geom
            FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
            WHERE state_code = 'CA'
        ),
        mlab_month AS (
            SELECT
                date,
                client.Geo.Latitude AS lat,
                client.Geo.Longitude AS lon,
                a.MeanThroughputMbps AS download_mbps,
                a.MinRTT AS latency_ms
            FROM `measurement-lab.ndt.unified_downloads`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
                AND client.Geo.CountryCode = 'US'
                AND client.Geo.Latitude BETWEEN 37.45 AND 37.90
                AND client.Geo.Longitude BETWEEN -122.35 AND -121.45
        )
        SELECT
            z.zip_code AS zcta,
            COUNT(*) AS test_count,
            AVG(m.download_mbps) AS avg_download_mbps,
            APPROX_QUANTILES(m.download_mbps, 100)[OFFSET(50)] AS median_download_mbps,
            AVG(m.latency_ms) AS avg_latency_ms
        FROM mlab_month m
        JOIN alameda_zctas z
        ON ST_WITHIN(ST_GEOGPOINT(m.lon, m.lat), z.zip_code_geom)
        GROUP BY z.zip_code
        """
        
        query_job = client.query(query)
        df = query_job.to_dataframe()
        all_data.append(df)
        print(f"  -> Found {len(df)} ZCTAs with data.")

    # Combine all months and aggregate
    print("Combining all months together...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    # We need to do a final aggregation since ZCTAs will appear multiple times (once per month)
    final_aggregated = final_df.groupby('zcta').apply(
        lambda x: pd.Series({
            'total_tests': x['test_count'].sum(),
            'avg_download_mbps': (x['avg_download_mbps'] * x['test_count']).sum() / x['test_count'].sum(),
            'median_download_mbps': x['median_download_mbps'].median(), # Approximation of yearly median
            'avg_latency_ms': (x['avg_latency_ms'] * x['test_count']).sum() / x['test_count'].sum()
        })
    ).reset_index()

    # Save to CSV
    final_aggregated.to_csv(output_csv, index=False)
    print(f"Saved yearly aggregated results to {output_csv}")

if __name__ == "__main__":
    # TODO: Replace with your actual Google Cloud Project ID
    YOUR_GCP_PROJECT_ID = "directed-asset-494823-t5"
    
    if YOUR_GCP_PROJECT_ID == "":
        print("Please edit the script to include your GCP Project ID.")
    else:
        fetch_mlab_data_alameda_2020(YOUR_GCP_PROJECT_ID)

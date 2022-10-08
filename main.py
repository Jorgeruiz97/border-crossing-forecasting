# ETL script for border crossing time snapshots.

import time
import logging
from datetime import datetime

import pandas as pd
import numpy as np

from google.cloud import storage
import xmltodict
import requests

logging.basicConfig(level=logging.INFO)


def main() -> None:
    # Query raw XML file from the CBP website.
    response = requests.get('https://bwt.cbp.gov/xml/bwt.xml')

    # Parse XML file response text to a python dict.
    raw = xmltodict.parse(response.content)['border_wait_time']

    # Freeze the extraction time to use as reference.
    extraction_timestamp = int(time.mktime(datetime.now().timetuple()))

    # Parse the document last update timestamp.
    last_update_timestamp = f"{raw['last_updated_date']} {raw['last_updated_time']}"
    last_update_timestamp = datetime.strptime(
        last_update_timestamp, '%Y-%m-%d %H:%M:%S')
    last_update_timestamp = time.mktime(last_update_timestamp.timetuple())

    crossings = (
        # Commercial
        ('commercial', 'standard_lanes'),
        ('commercial', 'FAST_lanes'),
        # Passenger
        ('passenger', 'standard_lanes'),
        ('passenger', 'ready_lanes'),
        ('passenger', 'NEXUS_SENTRI_lanes'),
        # Pedestrian
        ('pedestrian', 'standard_lanes'),
        ('pedestrian', 'ready_lanes')
    )

    main = []

    for lane_type, lane_name in crossings:
        for port in raw['port']:
            if lane_type == 'pedestrian':
                lane = port[f'{lane_type}_lanes']
                # Source data `automation_type` column has a typo therefore we need to maintain it.
                automation = port['pedestrain_automation_type']
            else:
                lane = port[f'{lane_type}_vehicle_lanes']
                automation = port[f'{lane_type}_automation_type']

            main.append({
                'extraction_timestamp': extraction_timestamp,
                'last_update_timestamp': last_update_timestamp,
                'port_number': port['port_number'],
                'border': port['border'],
                'port_name': port['port_name'],
                'crossing_name': port['crossing_name'],
                'hours': port['hours'],
                'port_status': port['port_status'],
                'lane_type': lane_type,
                'lane_name': lane_name,
                'automation_type': automation,
                'maximum_lanes': lane['maximum_lanes'],
                'update_time': lane[lane_name]['update_time'],
                'operational_status': lane[lane_name]['operational_status'],
                'delay_minutes': lane[lane_name]['delay_minutes'],
                'lanes_open': lane[lane_name]['lanes_open']
            })

    # Build DataFrame from list of records.
    df = pd.DataFrame.from_records(main)

    # Clean up DataFrame of missing data.
    df = df.replace(['N/A', None, '', ' '], np.NaN)

    bucket = storage.Client().bucket('ports_bucket')

    today = datetime.today()

    blob = bucket.blob(
        f'ports/{today.year}/{today.month}/{today.day}/{int(extraction_timestamp)}')

    # Save snapshot of ports lanes as a parquet file to a Google Cloud Storage.
    blob.upload_from_string(df.to_parquet(compression='gzip'))


if __name__ == '__main__':
    # Grab Current Time Before Running the Code
    start = time.time()
    main()
    logging.info(f'total execution time: {time.time() - start}')

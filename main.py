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

def main() -> None:
    # Query raw XML file from the CBP website.
    response = requests.get('https://bwt.cbp.gov/xml/bwt.xml')

    # Open connection to Google cloud storage.
    bucket = storage.Client().bucket('border-wait-times')

    # Parse XML file response text to a python dict.
    raw = xmltodict.parse(response.content)['border_wait_time']

    # Freeze the extraction time in UNIX MICROS to use as reference.
    extraction_timestamp = int(time.mktime(datetime.now().timetuple())) * _ONE_MILLION

    # Parse the document last update timestamp.
    last_update_xml = f"{raw['last_updated_date']} {raw['last_updated_time']}"

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
                'last_update_xml': last_update_xml,
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

    # Build DataFrame from list of records & clean up missing data.
    df = pd.DataFrame.from_records(main).replace(['N/A', None, '', ' '], np.NaN)

    # # Save raw snapshot of ports lanes as a parquet file to a Google Cloud Storage.
    # bucket.blob(f'snapshots/cbp/raw/dt={extraction_timestamp}').upload_from_string(json.dumps(raw))

    # Save snapshot of ports lanes as a parquet file to Google Cloud Storage.
    bucket.blob(f'snapshots/cbp/transformed/dt={extraction_timestamp}').upload_from_string(df.to_parquet(compression='gzip'))


if __name__ == '__main__':
    # Grab Current Time Before Running the Code
    start = time.time()
    main()
    logging.info(f'total execution time: {time.time() - start}')

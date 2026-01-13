"""
Silver Layer Transformation Script
Transforms raw Bronze layer data into cleaned and structured Silver layer data
"""

import os
import json
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_and_transform(raw_data: Dict) -> Dict:
    """
    Extract and transform fields from raw weather data
    
    Args:
        raw_data: Raw weather data dictionary from Bronze layer
        
    Returns:
        Transformed data dictionary with renamed fields
    """
    transformed = {}
    
    # Location data
    transformed['longitude'] = raw_data.get('coord', {}).get('lon')
    transformed['latitude'] = raw_data.get('coord', {}).get('lat')
    transformed['city_name'] = raw_data.get('name')
    transformed['country_code'] = raw_data.get('sys', {}).get('country')
    transformed['timezone'] = raw_data.get('timezone')
    
    # Temperature data
    main = raw_data.get('main', {})
    transformed['current_temp'] = main.get('temp')
    transformed['feels_like'] = main.get('feels_like')
    transformed['min_temp'] = main.get('temp_min')
    transformed['max_temp'] = main.get('temp_max')
    
    # Weather conditions
    weather = raw_data.get('weather', [{}])[0] if raw_data.get('weather') else {}
    transformed['weather_id'] = weather.get('id')
    transformed['weather_group'] = weather.get('main')
    transformed['weather_description'] = weather.get('description')
    transformed['weather_icon_code'] = weather.get('icon')
    
    # Atmospheric data
    transformed['atmospheric_pressure'] = main.get('pressure')
    transformed['humidity_percentage'] = main.get('humidity')
    transformed['sea_level_pressure'] = main.get('sea_level')
    transformed['ground_level'] = main.get('grnd_level')
    
    # Wind data
    wind = raw_data.get('wind', {})
    transformed['wind_speed'] = wind.get('speed')
    transformed['wind_direction'] = wind.get('deg')
    
    # Other measurements
    transformed['visibility'] = raw_data.get('visibility')
    transformed['cloudiness_percentage'] = raw_data.get('clouds', {}).get('all')
    transformed['rain_last_hour'] = raw_data.get('rain', {}).get('1h')
    transformed['snow_last_hour'] = raw_data.get('snow', {}).get('1h')
    
    # Timestamp data - convert Unix timestamps to datetime strings
    dt_timestamp = raw_data.get('dt')
    sunrise_timestamp = raw_data.get('sys', {}).get('sunrise')
    sunset_timestamp = raw_data.get('sys', {}).get('sunset')
    
    transformed['data_time'] = datetime.fromtimestamp(dt_timestamp, UTC).isoformat() if dt_timestamp else None
    transformed['sunrise_time'] = datetime.fromtimestamp(sunrise_timestamp, UTC).isoformat() if sunrise_timestamp else None
    transformed['sunset_time'] = datetime.fromtimestamp(sunset_timestamp, UTC).isoformat() if sunset_timestamp else None
    
    # Add ingestion timestamp
    transformed['ingestion_timestamp'] = datetime.now(UTC).isoformat()
    
    return transformed


def process_bronze_files(bronze_path: str, silver_path: str) -> None:
    """
    Process all JSON files from Bronze layer and save to Silver layer
    
    Args:
        bronze_path: Path to bronze layer directory
        silver_path: Path to silver layer directory
    """
    bronze_dir = Path(bronze_path)
    silver_dir = Path(silver_path)
    silver_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all JSON files from bronze layer
    json_files = list(bronze_dir.glob('*.json'))
    
    if not json_files:
        logger.warning(f"No JSON files found in {bronze_path}")
        return
    
    logger.info(f"Found {len(json_files)} files to process")
    
    # Process each file
    all_records = []
    successful = 0
    failed = 0
    
    for json_file in json_files:
        try:
            logger.info(f"Processing {json_file.name}")
            
            # Read raw data
            with open(json_file, 'r') as f:
                raw_data = json.load(f)
            
            # Transform data
            transformed_data = extract_and_transform(raw_data)
            all_records.append(transformed_data)
            
            successful += 1
            
        except Exception as e:
            logger.error(f"Error processing {json_file.name}: {e}")
            failed += 1
    
    # Save to Silver layer as CSV
    if all_records:
        df = pd.DataFrame(all_records)
        
        # Generate output filename with timestamp
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        output_file = silver_dir / f"weather_silver_{timestamp}.csv"
        
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(all_records)} records to {output_file}")
        
        # Also save as Parquet for better performance (if pyarrow is available)
        try:
            parquet_file = silver_dir / f"weather_silver_{timestamp}.parquet"
            df.to_parquet(parquet_file, index=False)
            logger.info(f"Saved {len(all_records)} records to {parquet_file}")
        except ImportError:
            logger.info("Parquet save skipped (pyarrow not installed)")
    
    logger.info(f"Transformation complete: {successful} successful, {failed} failed")


def main():
    """Main transformation function"""
    # Define paths
    bronze_path = Path(__file__).parent.parent / "bronze" / "raw_weather_data"
    silver_path = Path(__file__).parent / "processed_weather_data"
    
    logger.info("Starting Silver layer transformation")
    process_bronze_files(str(bronze_path), str(silver_path))
    logger.info("Silver layer transformation complete")


if __name__ == "__main__":
    main()

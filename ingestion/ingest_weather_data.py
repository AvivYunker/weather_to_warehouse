"""
Weather Data Ingestion Script
Fetches weather data from OpenWeatherMap API and stores in Bronze layer
"""

import os
import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import logging
from dotenv import load_dotenv

from weather_api_client import WeatherAPIClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "../config/config.yaml") -> Dict:
    """Load configuration from YAML file"""
    config_file = Path(__file__).parent / config_path
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def save_to_bronze(data: Dict, location: str, bronze_path: str) -> None:
    """
    Save raw weather data to Bronze layer
    
    Args:
        data: Weather data dictionary
        location: Location identifier
        bronze_path: Path to bronze storage
    """
    # Create bronze directory if it doesn't exist
    bronze_dir = Path(__file__).parent.parent / bronze_path
    bronze_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{location}_{timestamp}.json"
    filepath = bronze_dir / filename
    
    # Save data
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Saved data to {filepath}")


def ingest_weather_data() -> None:
    """Main ingestion function"""
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config()
    
    # Get API key from environment variable
    api_key = os.getenv('OPENWEATHER_API_KEY', config['api']['api_key'])
    
    if api_key == "YOUR_API_KEY_HERE" or not api_key:
        logger.error("Please set OPENWEATHER_API_KEY in .env file or config.yaml")
        return
    
    # Initialize API client
    client = WeatherAPIClient(
        api_key=api_key,
        base_url=config['api']['base_url'],
        timeout=config['api']['timeout']
    )
    
    # Fetch data for each location
    locations = config['locations']
    successful = 0
    failed = 0
    
    logger.info(f"Starting ingestion for {len(locations)} locations")
    
    for location in locations:
        city = location['city']
        country = location['country']
        
        # Fetch data with retry logic
        data = client.get_weather_with_retry(
            city=city,
            country=country,
            retry_attempts=config['ingestion']['retry_attempts'],
            retry_delay=config['ingestion']['retry_delay']
        )
        
        if data:
            # Save to bronze layer
            location_id = f"{city}_{country}".lower().replace(" ", "_")
            save_to_bronze(
                data=data,
                location=location_id,
                bronze_path=config['storage']['bronze_path']
            )
            successful += 1
        else:
            failed += 1
    
    logger.info(f"Ingestion complete: {successful} successful, {failed} failed")


if __name__ == "__main__":
    ingest_weather_data()

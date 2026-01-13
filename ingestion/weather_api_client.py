"""
OpenWeatherMap API Client
Handles API requests to fetch weather data
"""

import requests
import logging
from typing import Dict, Optional
from time import sleep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherAPIClient:
    """Client for interacting with OpenWeatherMap API"""
    
    def __init__(self, api_key: str, base_url: str, timeout: int = 30):
        """
        Initialize the API client
        
        Args:
            api_key: OpenWeatherMap API key
            base_url: Base URL for the API
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        
    def get_weather_by_city(self, city: str, country: str, units: str = "metric") -> Optional[Dict]:
        """
        Fetch current weather data for a specific city
        
        Args:
            city: City name
            country: Country code (ISO 3166)
            units: Units of measurement (metric, imperial, standard)
            
        Returns:
            Weather data as dictionary or None if request fails
        """
        params = {
            "q": f"{city},{country}",
            "appid": self.api_key,
            "units": units
        }
        
        try:
            logger.info(f"Fetching weather data for {city}, {country}")
            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched data for {city}, {country}")
            return data
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {city}, {country}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {city}, {country}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {city}, {country}: {e}")
            return None
    
    def get_weather_with_retry(
        self, 
        city: str, 
        country: str, 
        retry_attempts: int = 3, 
        retry_delay: int = 5
    ) -> Optional[Dict]:
        """
        Fetch weather data with retry logic
        
        Args:
            city: City name
            country: Country code
            retry_attempts: Number of retry attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            Weather data as dictionary or None if all attempts fail
        """
        for attempt in range(retry_attempts):
            data = self.get_weather_by_city(city, country)
            if data is not None:
                return data
            
            if attempt < retry_attempts - 1:
                logger.warning(f"Retry {attempt + 1}/{retry_attempts} for {city}, {country} in {retry_delay}s")
                sleep(retry_delay)
        
        logger.error(f"Failed to fetch data for {city}, {country} after {retry_attempts} attempts")
        return None

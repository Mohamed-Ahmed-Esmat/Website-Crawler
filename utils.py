import re
from typing import Optional, Tuple, Dict
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import logging

class LocationExtractor:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="webcrawler")
        self._location_cache = {}

    def extract_location(self, text: str) -> Optional[Dict[str, float]]:
        """Extract location information from text and convert to coordinates"""
        try:
            # Common location patterns
            patterns = [
                r'(?:located in|location:|address:|based in)\s+([^\.]+)',
                r'(?:city of|town of)\s+([^\.]+)',
                r'(?:[A-Z][a-z]+,\s+[A-Z]{2},\s+[A-Z]+)',  # City, State, Country format
                r'(?:[A-Z][a-z]+,\s+[A-Z][a-z]+)'  # City, Country format
            ]

            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    location_str = matches[0].strip()
                    
                    # Check cache first
                    if location_str in self._location_cache:
                        return self._location_cache[location_str]
                    
                    # Geocode the location
                    location = self.geolocator.geocode(location_str, timeout=5)
                    if location:
                        result = {
                            "lat": location.latitude,
                            "lon": location.longitude
                        }
                        # Cache the result
                        self._location_cache[location_str] = result
                        return result

        except GeocoderTimedOut:
            logging.warning("Geocoding service timed out")
        except Exception as e:
            logging.error(f"Error extracting location: {e}")
        
        return None

    def extract_from_url(self, url: str) -> Optional[Dict[str, float]]:
        """Try to extract location information from URL structure"""
        try:
            # Extract potential location names from URL segments
            segments = re.split(r'[/\-_]', url.lower())
            for segment in segments:
                if len(segment) > 3:  # Avoid very short segments
                    # Check cache first
                    if segment in self._location_cache:
                        return self._location_cache[segment]
                    
                    # Try geocoding
                    try:
                        location = self.geolocator.geocode(segment, timeout=5)
                        if location:
                            result = {
                                "lat": location.latitude,
                                "lon": location.longitude
                            }
                            # Cache the result
                            self._location_cache[segment] = result
                            return result
                    except GeocoderTimedOut:
                        continue
                    
        except Exception as e:
            logging.error(f"Error extracting location from URL: {e}")
        
        return None

# Create a singleton instance
location_extractor = LocationExtractor()
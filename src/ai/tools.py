from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

class AirQualityTools:
    def __init__(self, spark_session=None):
        self.spark = spark_session or SparkSession.builder.getOrCreate()
        
    def get_current_conditions(
        self,
        location_name: str = None,
        location_id: int = None
    ) -> Dict[str, Any]:
        """
        Get current air quality conditions for a location.
        """
        try:
            if location_id:
                condition = f"location_id = {location_id}"
            elif location_name:
                condition = f"location_name = '{location_name}'"
            else:
                return {"error": "Must provide location_name or location_id"}
            
            query = f"""
            SELECT 
                location_name,
                location_id,
                city,
                country_code,
                parameter,
                value,
                unit,
                datetime_local,
                latitude,
                longitude
            FROM gold.air_quality_latest
            WHERE {condition}
            ORDER BY datetime_local DESC
            LIMIT 1
            """
            
            result = self.spark.sql(query).collect()
            
            if not result:
                return {"error": f"No data found for location: {location_name or location_id}"}
            
            row = result[0]
            aqi_info = self._calculate_aqi_o3(row['value'])
            
            return {
                "location": row['location_name'],
                "city": row['city'],
                "country": row['country_code'],
                "timestamp": str(row['datetime_local']),
                "pollutant": row['parameter'],
                "concentration": float(row['value']),
                "unit": row['unit'],
                "aqi": aqi_info['aqi'],
                "category": aqi_info['category'],
                "health_implications": aqi_info['health_implications'],
                "coordinates": {
                    "lat": float(row['latitude']),
                    "lon": float(row['longitude'])
                }
            }
            
        except Exception as e:
            return {"error": f"Failed to retrieve conditions: {str(e)}"}
    
    def compare_locations(
        self,
        locations: List[str],
        period_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Compare air quality across multiple locations.
        """
        try:
            locations_str = "', '".join(locations)
            
            query = f"""
            SELECT 
                location_name,
                AVG(value) as avg_pollution,
                MAX(value) as peak_pollution,
                MIN(value) as min_pollution,
                STDDEV(value) as variability
            FROM silver.air_quality_measurements
            WHERE location_name IN ('{locations_str}')
                AND datetime_local >= CURRENT_TIMESTAMP() - INTERVAL {period_hours} HOURS
            GROUP BY location_name
            ORDER BY avg_pollution DESC
            """
            
            results = self.spark.sql(query).collect()
            
            if not results:
                return {"error": "No data found for specified locations"}
            
            comparisons = [
                {
                    "location": row['location_name'],
                    "avg_pollution": float(row['avg_pollution']),
                    "peak": float(row['peak_pollution']),
                    "min": float(row['min_pollution']),
                    "variability": float(row['variability']) if row['variability'] else 0,
                    "aqi_category": self._calculate_aqi_o3(row['avg_pollution'])['category']
                }
                for row in results
            ]
            
            for i, comp in enumerate(comparisons, 1):
                comp['rank'] = i
            
            return {
                "period_hours": period_hours,
                "locations_compared": len(comparisons),
                "comparisons": comparisons,
                "cleanest": comparisons[-1]['location'] if comparisons else None,
                "most_polluted": comparisons[0]['location'] if comparisons else None
            }
            
        except Exception as e:
            return {"error": f"Failed to compare locations: {str(e)}"}
    
    def _calculate_aqi_o3(self, o3_ugm3: float) -> Dict[str, Any]:
        """
        Calculate AQI from O3 concentration.
        """
        if o3_ugm3 <= 108:
            aqi = (50 / 108) * o3_ugm3
            category = "Good"
            implications = "Air quality is satisfactory. No health concerns."
        elif o3_ugm3 <= 140:
            aqi = 51 + ((100 - 51) / (140 - 108)) * (o3_ugm3 - 108)
            category = "Moderate"
            implications = "Acceptable for most. Unusually sensitive people should consider limiting prolonged outdoor exertion."
        elif o3_ugm3 <= 170:
            aqi = 101 + ((150 - 101) / (170 - 140)) * (o3_ugm3 - 140)
            category = "Unhealthy for Sensitive Groups"
            implications = "Sensitive groups may experience health effects."
        elif o3_ugm3 <= 210:
            aqi = 151 + ((200 - 151) / (210 - 170)) * (o3_ugm3 - 170)
            category = "Unhealthy"
            implications = "Everyone may begin to experience health effects."
        else:
            aqi = 201 + ((300 - 201) / (400 - 210)) * min(o3_ugm3 - 210, 190)
            category = "Very Unhealthy" if o3_ugm3 <= 400 else "Hazardous"
            implications = "Health alert: everyone may experience serious health effects."
        
        return {
            "aqi": round(aqi, 1),
            "category": category,
            "health_implications": implications
        }

# Instantiate global tools instance
# Note: In a real Databricks environment, 'spark' is available globally, 
# but here we instantiate it safely.
try:
    _tools_instance = AirQualityTools()
except:
    _tools_instance = None

def get_current_air_quality(location_name: str, location_id: int = None) -> Dict[str, Any]:
    global _tools_instance
    if not _tools_instance:
        _tools_instance = AirQualityTools()
    return _tools_instance.get_current_conditions(location_name, location_id)

def compare_city_risk(city1: str, city2: str) -> Dict[str, Any]:
    # Wrapper to match the 'CompareCities' tool signature which might expect specific args
    # or just a string descriptions, but for structured tools:
    global _tools_instance
    if not _tools_instance:
        _tools_instance = AirQualityTools()
    return _tools_instance.compare_locations([city1, city2])
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import json


class AirQualityTools:
    def __init__(self, spark_session=None):
        self.spark = spark_session
        
    def get_current_conditions(
        self,
        location_name: str = None,
        location_id: int = None
    ) -> Dict[str, Any]:
        """
        Get current air quality conditions for a location
        
        Args:
            location_name: Name of the location (e.g., "Trapani")
            location_id: ID of the location
            
        Returns:
            Dictionary with current O3 levels, AQI, and health recommendations
        """
        try:
            # Build query
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
            
            # Calculate AQI from O3 value
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
    
    def get_historical_trend(
        self,
        location_name: str,
        days: int = 7
    ) -> Dict[str, Any]:
        """
        Get historical trend for a location
        
        Args:
            location_name: Name of the location
            days: Number of days to look back
            
        Returns:
            Dictionary with trend analysis
        """
        try:
            query = f"""
            SELECT 
                DATE(datetime_local) as date,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value,
                COUNT(*) as readings
            FROM silver.air_quality_measurements
            WHERE location_name = '{location_name}'
                AND datetime_local >= CURRENT_DATE() - INTERVAL {days} DAYS
            GROUP BY DATE(datetime_local)
            ORDER BY date DESC
            """
            
            results = self.spark.sql(query).collect()
            
            if not results:
                return {"error": f"No historical data for {location_name}"}
            
            trend_data = [
                {
                    "date": str(row['date']),
                    "avg": float(row['avg_value']),
                    "max": float(row['max_value']),
                    "min": float(row['min_value']),
                    "readings": int(row['readings'])
                }
                for row in results
            ]
            
            if len(trend_data) >= 2:
                recent_avg = sum(d['avg'] for d in trend_data[:3]) / min(3, len(trend_data))
                older_avg = sum(d['avg'] for d in trend_data[-3:]) / min(3, len(trend_data))
                
                if recent_avg > older_avg * 1.1:
                    trend = "worsening"
                elif recent_avg < older_avg * 0.9:
                    trend = "improving"
                else:
                    trend = "stable"
            else:
                trend = "insufficient_data"
            
            return {
                "location": location_name,
                "period_days": days,
                "trend": trend,
                "data": trend_data,
                "summary": {
                    "avg_pollution": sum(d['avg'] for d in trend_data) / len(trend_data),
                    "peak_pollution": max(d['max'] for d in trend_data),
                    "total_readings": sum(d['readings'] for d in trend_data)
                }
            }
            
        except Exception as e:
            return {"error": f"Failed to retrieve trend: {str(e)}"}
    
    def compare_locations(
        self,
        locations: List[str],
        period_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Compare air quality across multiple locations
        
        Args:
            locations: List of location names to compare
            period_hours: Time period to analyze
            
        Returns:
            Comparison data with rankings
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
            
            # Add rankings
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
    
    def detect_anomalies(
        self,
        location_name: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Detect unusual pollution spikes or drops
        
        Args:
            location_name: Location to analyze
            hours: Time window to check
            
        Returns:
            Anomaly detection results
        """
        try:
            query = f"""
            WITH stats AS (
                SELECT 
                    AVG(value) as mean_val,
                    STDDEV(value) as std_val
                FROM silver.air_quality_measurements
                WHERE location_name = '{location_name}'
                    AND datetime_local >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
            ),
            recent AS (
                SELECT 
                    datetime_local,
                    value,
                    (value - stats.mean_val) / NULLIF(stats.std_val, 0) as z_score
                FROM silver.air_quality_measurements
                CROSS JOIN stats
                WHERE location_name = '{location_name}'
                    AND datetime_local >= CURRENT_TIMESTAMP() - INTERVAL {hours} HOURS
            )
            SELECT *
            FROM recent
            WHERE ABS(z_score) > 2.5
            ORDER BY ABS(z_score) DESC
            """
            
            results = self.spark.sql(query).collect()
            
            anomalies = [
                {
                    "timestamp": str(row['datetime_local']),
                    "value": float(row['value']),
                    "z_score": float(row['z_score']),
                    "severity": "extreme" if abs(row['z_score']) > 3 else "moderate"
                }
                for row in results
            ]
            
            return {
                "location": location_name,
                "period_hours": hours,
                "anomalies_detected": len(anomalies),
                "anomalies": anomalies,
                "alert": len(anomalies) > 0
            }
            
        except Exception as e:
            return {"error": f"Failed to detect anomalies: {str(e)}"}
    
    def get_prediction(
        self,
        location_name: str,
        hours_ahead: int = 24
    ) -> Dict[str, Any]:
        """
        Get ML model prediction for future pollution levels
        
        Args:
            location_name: Location to predict for
            hours_ahead: How many hours to predict
            
        Returns:
            Prediction with confidence intervals
        """
        try:
            query = f"""
            SELECT 
                location_id,
                location_name
            FROM gold.air_quality_latest
            WHERE location_name = '{location_name}'
            LIMIT 1
            """
            
            result = self.spark.sql(query).collect()
            if not result:
                return {"error": f"Location not found: {location_name}"}
            
            location_id = result[0]['location_id']
            
            # Call ML model endpoint (placeholder)
            # In production, this would be:
            # response = requests.post(f"{model_endpoint}/predict", json={...})
            
            return {
                "location": location_name,
                "location_id": location_id,
                "prediction_timestamp": str(datetime.now() + timedelta(hours=hours_ahead)),
                "hours_ahead": hours_ahead,
                "predicted_value": 0.0,  # Placeholder - replace with actual model call
                "confidence_interval": {"lower": 0.0, "upper": 0.0},
                "note": "Prediction from ML model - implement model serving endpoint"
            }
            
        except Exception as e:
            return {"error": f"Failed to get prediction: {str(e)}"}

    
    def get_health_recommendation(
        self,
        location_name: str,
        activity: str = "general",
        sensitive_group: bool = False
    ) -> Dict[str, Any]:
        """
        Get personalized health recommendations based on current conditions
        
        Args:
            location_name: Location to check
            activity: Type of activity (running, cycling, outdoor_work, general)
            sensitive_group: Whether person is in sensitive group (asthma, elderly, children)
            
        Returns:
            Health recommendations and safety ratings
        """
        try:
            current = self.get_current_conditions(location_name=location_name)
            
            if "error" in current:
                return current
            
            aqi = current['aqi']
            
            # Determine safety based on activity and sensitivity
            if sensitive_group:
                if aqi <= 50:
                    safety = "safe"
                    advice = f"{activity.replace('_', ' ').title()} is safe, but monitor for symptoms."
                elif aqi <= 100:
                    safety = "moderate_risk"
                    advice = f"Consider reducing intensity of {activity.replace('_', ' ')}."
                elif aqi <= 150:
                    safety = "unhealthy"
                    advice = f"Avoid {activity.replace('_', ' ')}. Stay indoors if possible."
                else:
                    safety = "dangerous"
                    advice = f"Do not perform {activity.replace('_', ' ')}. Stay indoors with air purifier."
            else:
                if aqi <= 100:
                    safety = "safe"
                    advice = f"{activity.replace('_', ' ').title()} is safe for healthy individuals."
                elif aqi <= 150:
                    safety = "moderate_risk"
                    advice = f"Healthy individuals can continue {activity.replace('_', ' ')}, but take breaks."
                elif aqi <= 200:
                    safety = "unhealthy"
                    advice = f"Consider postponing {activity.replace('_', ' ')} or reducing duration."
                else:
                    safety = "dangerous"
                    advice = f"Avoid {activity.replace('_', ' ')}. Health risk for everyone."
            
            return {
                "location": location_name,
                "current_aqi": aqi,
                "activity": activity,
                "sensitive_group": sensitive_group,
                "safety_rating": safety,
                "recommendation": advice,
                "additional_precautions": self._get_precautions(aqi, sensitive_group),
                "best_time_window": self._suggest_best_time(location_name, activity)
            }
            
        except Exception as e:
            return {"error": f"Failed to generate recommendation: {str(e)}"}
    
    
    def _calculate_aqi_o3(self, o3_ugm3: float) -> Dict[str, Any]:
        """
        Calculate AQI from O3 concentration (8-hour average approximation)
        
        EPA AQI Breakpoints for Ozone (8-hour average in ppm):
        0-54 ppb (0-108 µg/m³) = 0-50 AQI (Good)
        55-70 ppb (109-140 µg/m³) = 51-100 AQI (Moderate)
        71-85 ppb (141-170 µg/m³) = 101-150 AQI (Unhealthy for Sensitive)
        86-105 ppb (171-210 µg/m³) = 151-200 AQI (Unhealthy)
        """
        
        if o3_ugm3 <= 108:
            aqi = (50 / 108) * o3_ugm3
            category = "Good"
            color = "green"
            implications = "Air quality is satisfactory. No health concerns."
        elif o3_ugm3 <= 140:
            aqi = 51 + ((100 - 51) / (140 - 108)) * (o3_ugm3 - 108)
            category = "Moderate"
            color = "yellow"
            implications = "Acceptable for most. Unusually sensitive people should consider limiting prolonged outdoor exertion."
        elif o3_ugm3 <= 170:
            aqi = 101 + ((150 - 101) / (170 - 140)) * (o3_ugm3 - 140)
            category = "Unhealthy for Sensitive Groups"
            color = "orange"
            implications = "Sensitive groups may experience health effects. General public less likely to be affected."
        elif o3_ugm3 <= 210:
            aqi = 151 + ((200 - 151) / (210 - 170)) * (o3_ugm3 - 170)
            category = "Unhealthy"
            color = "red"
            implications = "Everyone may begin to experience health effects. Sensitive groups at greater risk."
        else:
            aqi = 201 + ((300 - 201) / (400 - 210)) * min(o3_ugm3 - 210, 190)
            category = "Very Unhealthy" if o3_ugm3 <= 400 else "Hazardous"
            color = "purple" if o3_ugm3 <= 400 else "maroon"
            implications = "Health alert: everyone may experience serious health effects."
        
        return {
            "aqi": round(aqi, 1),
            "category": category,
            "color": color,
            "health_implications": implications
        }
    
    def _get_precautions(self, aqi: float, sensitive: bool) -> List[str]:
        """Get list of precautions based on AQI"""
        precautions = []
        
        if aqi > 100:
            precautions.append("Close windows and doors")
            precautions.append("Use air purifier if available")
        
        if aqi > 150:
            precautions.append("Wear N95 mask if going outside")
            precautions.append("Limit outdoor activities")
            
        if aqi > 200:
            precautions.append("Stay indoors as much as possible")
            precautions.append("Seek medical attention if experiencing symptoms")
        
        if sensitive and aqi > 50:
            precautions.append("Keep rescue inhaler nearby (if applicable)")
            precautions.append("Monitor symptoms closely")
        
        return precautions if precautions else ["No special precautions needed - air quality is good"]
    
    def _suggest_best_time(self, location_name: str, activity: str) -> str:
        """Suggest best time of day for outdoor activities"""
        try:
            query = f"""
            SELECT 
                HOUR(datetime_local) as hour,
                AVG(value) as avg_pollution
            FROM silver.air_quality_measurements
            WHERE location_name = '{location_name}'
                AND datetime_local >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
            GROUP BY HOUR(datetime_local)
            ORDER BY avg_pollution ASC
            LIMIT 1
            """
            
            result = self.spark.sql(query).collect()
            
            if result:
                best_hour = result[0]['hour']
                return f"Best time for {activity}: around {best_hour}:00 (historically lowest pollution)"
            
            return "Check again later for timing recommendations"
            
        except:
            return "Unable to determine best time window"


def get_tool_definitions() -> List[Dict[str, Any]]:
    """
    Define tools in format expected by LangChain/Anthropic agent
    
    Returns:
        List of tool definitions
    """
    return [
        {
            "name": "get_current_conditions",
            "description": "Get current air quality conditions for a specific location. Use this when user asks about current pollution levels, AQI, or general air quality status.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location_name": {
                        "type": "string",
                        "description": "Name of the location (e.g., 'Trapani', 'Rome')"
                    },
                    "location_id": {
                        "type": "integer",
                        "description": "Optional: Location ID if known"
                    }
                },
                "required": []
            }
        },
        {
            "name": "get_historical_trend",
            "description": "Analyze historical pollution trends over a period of days. Use this when user asks about trends, patterns, or how pollution has changed over time.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location_name": {
                        "type": "string",
                        "description": "Name of the location"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days to look back (default: 7)",
                        "default": 7
                    }
                },
                "required": ["location_name"]
            }
        },
        {
            "name": "compare_locations",
            "description": "Compare air quality across multiple locations. Use this when user wants to know which location has better/worse air quality.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "locations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of location names to compare"
                    },
                    "period_hours": {
                        "type": "integer",
                        "description": "Time period in hours (default: 24)",
                        "default": 24
                    }
                },
                "required": ["locations"]
            }
        },
        {
            "name": "detect_anomalies",
            "description": "Detect unusual pollution spikes or anomalies. Use this when user asks about unusual readings or sudden changes.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location_name": {
                        "type": "string",
                        "description": "Location to analyze"
                    },
                    "hours": {
                        "type": "integer",
                        "description": "Time window in hours (default: 24)",
                        "default": 24
                    }
                },
                "required": ["location_name"]
            }
        },
        {
            "name": "get_health_recommendation",
            "description": "Get personalized health and activity recommendations based on current air quality. Use this when user asks if it's safe to do outdoor activities or needs health advice.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location_name": {
                        "type": "string",
                        "description": "Location to check"
                    },
                    "activity": {
                        "type": "string",
                        "description": "Type of activity: running, cycling, outdoor_work, or general",
                        "default": "general"
                    },
                    "sensitive_group": {
                        "type": "boolean",
                        "description": "True if person has asthma, respiratory conditions, is elderly, or a child",
                        "default": False
                    }
                },
                "required": ["location_name"]
            }
        },
        {
            "name": "get_prediction",
            "description": "Get ML model prediction for future pollution levels. Use this when user asks about future conditions or forecasts.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "location_name": {
                        "type": "string",
                        "description": "Location to predict for"
                    },
                    "hours_ahead": {
                        "type": "integer",
                        "description": "Hours into the future to predict (default: 24)",
                        "default": 24
                    }
                },
                "required": ["location_name"]
            }
        }
    ]
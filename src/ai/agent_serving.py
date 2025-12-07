"""
Air Quality Agent Deployment and API Serving
Flask API for serving the agent + Databricks Job scheduling
"""

from flask import Flask, request, jsonify
from typing import Dict, Any, List
import logging
import json
from datetime import datetime
import os
from functools import wraps

from src.ai.agent import AirQualityAgent
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Global agent instance (initialized on startup)
agent = None
spark = None


def init_agent():
    """Initialize the agent with Spark session"""
    global agent, spark
    
    try:
        # Initialize Spark (this will work in Databricks)
        spark = SparkSession.builder \
            .appName("AirQualityAgentAPI") \
            .getOrCreate()
        
        # Initialize agent
        agent = AirQualityAgent(spark_session=spark)
        
        logger.info("Agent initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize agent: {str(e)}", exc_info=True)
        raise


def require_agent(f):
    """Decorator to ensure agent is initialized"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if agent is None:
            return jsonify({
                "error": "Agent not initialized",
                "status": "error"
            }), 500
        return f(*args, **kwargs)
    return decorated_function


# ========== API ENDPOINTS ==========

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "agent_initialized": agent is not None,
        "timestamp": datetime.now().isoformat()
    })


@app.route('/api/v1/chat', methods=['POST'])
@require_agent
def chat():
    """
    Chat with the agent
    
    Request body:
    {
        "message": "What's the air quality in Trapani?",
        "conversation_history": []  # Optional
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'message' not in data:
            return jsonify({
                "error": "Missing 'message' in request body",
                "status": "error"
            }), 400
        
        message = data['message']
        conversation_history = data.get('conversation_history', [])
        
        logger.info(f"Chat request: {message}")
        
        # Get agent response
        response = agent.chat(
            user_message=message,
            conversation_history=conversation_history
        )
        
        return jsonify({
            "response": response['response'],
            "tools_used": [t['tool'] for t in response['tool_uses']],
            "iterations": response['iterations'],
            "status": response['status'],
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/current_conditions', methods=['GET'])
@require_agent
def get_current_conditions():
    """
    Get current air quality conditions for a location
    
    Query params:
    - location_name: Name of location (e.g., "Trapani")
    - location_id: Optional location ID
    """
    try:
        location_name = request.args.get('location_name')
        location_id = request.args.get('location_id', type=int)
        
        if not location_name and not location_id:
            return jsonify({
                "error": "Must provide location_name or location_id",
                "status": "error"
            }), 400
        
        result = agent.tools.get_current_conditions(
            location_name=location_name,
            location_id=location_id
        )
        
        if "error" in result:
            return jsonify(result), 404
        
        return jsonify({
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in current_conditions endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/health_recommendation', methods=['POST'])
@require_agent
def health_recommendation():
    """
    Get health recommendation for an activity
    
    Request body:
    {
        "location_name": "Trapani",
        "activity": "running",  # running, cycling, outdoor_work, general
        "sensitive_group": false
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'location_name' not in data:
            return jsonify({
                "error": "Missing 'location_name' in request body",
                "status": "error"
            }), 400
        
        result = agent.tools.get_health_recommendation(
            location_name=data['location_name'],
            activity=data.get('activity', 'general'),
            sensitive_group=data.get('sensitive_group', False)
        )
        
        if "error" in result:
            return jsonify(result), 404
        
        return jsonify({
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in health_recommendation endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/compare_locations', methods=['POST'])
@require_agent
def compare_locations():
    """
    Compare air quality across locations
    
    Request body:
    {
        "locations": ["Trapani", "Rome", "Milan"],
        "period_hours": 24
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'locations' not in data:
            return jsonify({
                "error": "Missing 'locations' in request body",
                "status": "error"
            }), 400
        
        result = agent.tools.compare_locations(
            locations=data['locations'],
            period_hours=data.get('period_hours', 24)
        )
        
        if "error" in result:
            return jsonify(result), 404
        
        return jsonify({
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in compare_locations endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/monitoring_report', methods=['POST'])
@require_agent
def monitoring_report():
    """
    Run autonomous monitoring and generate report
    
    Request body:
    {
        "locations": ["Trapani", "Rome"],
        "alert_threshold_aqi": 150
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'locations' not in data:
            return jsonify({
                "error": "Missing 'locations' in request body",
                "status": "error"
            }), 400
        
        result = agent.autonomous_monitoring(
            locations=data['locations'],
            alert_threshold_aqi=data.get('alert_threshold_aqi', 150)
        )
        
        return jsonify({
            "data": result,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in monitoring_report endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


@app.route('/api/v1/daily_report', methods=['POST'])
@require_agent
def daily_report():
    """
    Generate daily comprehensive report
    
    Request body:
    {
        "locations": ["Trapani", "Rome"]
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'locations' not in data:
            return jsonify({
                "error": "Missing 'locations' in request body",
                "status": "error"
            }), 400
        
        report = agent.daily_report(locations=data['locations'])
        
        return jsonify({
            "report": report,
            "status": "success",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in daily_report endpoint: {str(e)}", exc_info=True)
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500


# ========== DATABRICKS JOB FUNCTIONS ==========

def scheduled_monitoring_job(
    locations: List[str],
    alert_threshold_aqi: int = 150,
    notification_webhook: str = None
):
    """
    Scheduled job for autonomous monitoring
    
    This function is designed to be called by Databricks Jobs
    
    Args:
        locations: List of locations to monitor
        alert_threshold_aqi: AQI threshold for alerts
        notification_webhook: Optional webhook URL for notifications (Slack, Teams, etc.)
    """
    logger.info("Starting scheduled monitoring job")
    
    try:
        # Initialize agent
        init_agent()
        
        # Run monitoring
        report = agent.autonomous_monitoring(
            locations=locations,
            alert_threshold_aqi=alert_threshold_aqi
        )
        
        logger.info(f"Monitoring complete. Found {report['alert_count']} alerts")
        
        # Send notifications if there are alerts
        if report['alert_count'] > 0 and notification_webhook:
            send_notification(report, notification_webhook)
        
        # Save report to Delta table
        save_monitoring_report(report)
        
        return report
        
    except Exception as e:
        logger.error(f"Error in scheduled monitoring job: {str(e)}", exc_info=True)
        raise


def daily_report_job(
    locations: List[str],
    output_path: str = None,
    notification_webhook: str = None
):
    """
    Scheduled job for daily reports
    
    Args:
        locations: Locations to include in report
        output_path: Optional path to save report
        notification_webhook: Optional webhook for notification
    """
    logger.info("Starting daily report job")
    
    try:
        # Initialize agent
        init_agent()
        
        # Generate report
        report = agent.daily_report(locations=locations)
        
        logger.info("Daily report generated successfully")
        
        # Save report
        if output_path:
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = f"{output_path}/daily_report_{timestamp}.txt"
            
            with open(filename, 'w') as f:
                f.write(report)
            
            logger.info(f"Report saved to: {filename}")
        
        # Send notification
        if notification_webhook:
            send_notification({"report": report}, notification_webhook, report_type="daily")
        
        return report
        
    except Exception as e:
        logger.error(f"Error in daily report job: {str(e)}", exc_info=True)
        raise


def save_monitoring_report(report: Dict[str, Any]):
    """Save monitoring report to Delta table"""
    try:
        # Convert to DataFrame
        alerts_data = []
        for alert in report['alerts']:
            alerts_data.append({
                "monitoring_time": report['monitoring_time'],
                "location": alert['location'],
                "aqi": alert['aqi'],
                "category": alert['category'],
                "severity": alert['severity'],
                "alert_message": alert['alert']
            })
        
        if alerts_data:
            df = spark.createDataFrame(alerts_data)
            
            # Save to Delta table
            df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable("gold.air_quality_alerts")
            
            logger.info(f"Saved {len(alerts_data)} alerts to gold.air_quality_alerts")
    
    except Exception as e:
        logger.error(f"Error saving monitoring report: {str(e)}")


def send_notification(
    data: Dict[str, Any],
    webhook_url: str,
    report_type: str = "monitoring"
):
    """
    Send notification via webhook (Slack, Teams, etc.)
    
    Args:
        data: Report data
        webhook_url: Webhook URL
        report_type: Type of report (monitoring or daily)
    """
    try:
        import requests
        
        if report_type == "monitoring":
            # Format monitoring alert
            alert_count = data.get('alert_count', 0)
            
            message = f"🚨 *Air Quality Alert*\n\n"
            message += f"Found {alert_count} location(s) with concerning air quality:\n\n"
            
            for alert in data.get('alerts', []):
                message += f"*{alert['location']}*\n"
                message += f"• AQI: {alert['aqi']} ({alert['category']})\n"
                message += f"• Severity: {alert['severity']}\n"
                message += f"• Recommendation: {alert['alert'][:200]}...\n\n"
        
        else:  # daily report
            message = f"📊 *Daily Air Quality Report*\n\n"
            message += data.get('report', '')[:1000]  # Limit message size
        
        # Send to webhook (format for Slack - adjust for other platforms)
        payload = {
            "text": message,
            "mrkdwn": True
        }
        
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        
        logger.info("Notification sent successfully")
        
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")


# ========== STARTUP ==========

@app.before_first_request
def startup():
    """Initialize agent when app starts"""
    init_agent()


# ========== MAIN ==========

if __name__ == "__main__":
    # For local development
    init_agent()
    app.run(host='0.0.0.0', port=5000, debug=True)

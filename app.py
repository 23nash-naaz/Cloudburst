import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from io import StringIO
import re

# Page configuration
st.set_page_config(
    page_title="Cloudburst Prediction System - India",
    page_icon="🌧️",
    layout="wide"
)

# Initialize SQLite Database
@st.cache_resource
def init_database():
    conn = sqlite3.connect('cloudburst_data.db', check_same_thread=False)
    cursor = conn.cursor()
   
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cloudburst_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL,
            district TEXT NOT NULL,
            date TEXT NOT NULL,
            rainfall_mm REAL,
            duration_hours REAL,
            casualties INTEGER,
            severity TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
   
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL,
            district TEXT NOT NULL,
            date TEXT NOT NULL,
            humidity REAL,
            temperature REAL,
            wind_speed REAL,
            pressure REAL,
            cloud_cover REAL,
            precipitation REAL
        )
    ''')
   
    # Check if data already exists
    cursor.execute("SELECT COUNT(*) FROM cloudburst_history")
    if cursor.fetchone()[0] == 0:
        # Insert cloudburst historical data
        cloudburst_data = [
            # Uttarakhand
            ('Uttarakhand', 'Chamoli', '2023-06-15', 150, 3, 12, 'High', 30.4, 79.4),
            ('Uttarakhand', 'Rudraprayag', '2023-08-12', 100, 2, 5, 'Medium', 30.3, 78.9),
            ('Uttarakhand', 'Pithoragarh', '2023-09-03', 88, 1.5, 2, 'Medium', 29.6, 80.2),
            ('Uttarakhand', 'Dehradun', '2024-06-10', 135, 3, 10, 'High', 30.3, 78.0),
            ('Uttarakhand', 'Chamoli', '2024-08-18', 115, 2, 7, 'High', 30.4, 79.4),
            ('Uttarakhand', 'Uttarkashi', '2024-09-01', 95, 2, 3, 'Medium', 30.7, 78.4),
           
            # Himachal Pradesh
            ('Himachal Pradesh', 'Kullu', '2023-07-08', 120, 2.5, 8, 'High', 31.9, 77.1),
            ('Himachal Pradesh', 'Shimla', '2023-08-25', 110, 2.5, 6, 'High', 31.1, 77.2),
            ('Himachal Pradesh', 'Mandi', '2024-07-05', 128, 2.5, 9, 'High', 31.7, 76.9),
            ('Himachal Pradesh', 'Kinnaur', '2024-08-22', 108, 2.5, 5, 'High', 31.6, 78.2),
            ('Himachal Pradesh', 'Chamba', '2024-08-05', 102, 2, 4, 'Medium', 32.5, 76.1),
           
            # Jammu and Kashmir
            ('Jammu and Kashmir', 'Anantnag', '2023-07-20', 95, 2, 3, 'Medium', 33.7, 75.1),
            ('Jammu and Kashmir', 'Doda', '2024-07-12', 98, 2, 4, 'Medium', 33.1, 75.5),
            ('Jammu and Kashmir', 'Kishtwar', '2024-06-18', 105, 2.5, 6, 'High', 33.3, 75.8),
           
            # Arunachal Pradesh
            ('Arunachal Pradesh', 'West Kameng', '2023-06-28', 105, 2, 4, 'High', 27.3, 92.4),
            ('Arunachal Pradesh', 'Tawang', '2024-06-25', 112, 2, 6, 'High', 27.6, 91.9),
            ('Arunachal Pradesh', 'East Kameng', '2024-07-30', 98, 2, 3, 'Medium', 27.1, 93.0),
           
            # Sikkim
            ('Sikkim', 'North Sikkim', '2023-07-15', 92, 2, 3, 'Medium', 27.8, 88.6),
            ('Sikkim', 'East Sikkim', '2024-07-18', 90, 1.5, 2, 'Medium', 27.3, 88.6),
            ('Sikkim', 'West Sikkim', '2024-08-14', 96, 2, 4, 'Medium', 27.3, 88.3),
           
            # Meghalaya
            ('Meghalaya', 'East Khasi Hills', '2024-06-08', 125, 3, 8, 'High', 25.6, 91.9),
            ('Meghalaya', 'West Khasi Hills', '2023-06-20', 118, 2.5, 6, 'High', 25.5, 91.3),
            ('Meghalaya', 'South Garo Hills', '2024-07-15', 110, 2, 5, 'High', 25.3, 90.6),
           
            # West Bengal (North Bengal)
            ('West Bengal', 'Darjeeling', '2023-07-10', 105, 2.5, 7, 'High', 27.0, 88.3),
            ('West Bengal', 'Kalimpong', '2023-08-05', 98, 2, 5, 'Medium', 27.1, 88.5),
            ('West Bengal', 'Jalpaiguri', '2024-06-22', 115, 3, 9, 'High', 26.5, 88.7),
            ('West Bengal', 'Darjeeling', '2024-07-28', 108, 2.5, 6, 'High', 27.0, 88.3),
            ('West Bengal', 'Alipurduar', '2024-08-10', 92, 2, 4, 'Medium', 26.5, 89.5),
           
            # Maharashtra (Western Ghats)
            ('Maharashtra', 'Raigad', '2023-07-22', 145, 3, 15, 'High', 18.5, 73.3),
            ('Maharashtra', 'Pune', '2024-07-16', 138, 2.5, 12, 'High', 18.5, 73.9),
            ('Maharashtra', 'Satara', '2024-08-08', 125, 3, 8, 'High', 17.7, 74.0),
           
            # Kerala (Western Ghats)
            ('Kerala', 'Idukki', '2023-06-18', 155, 3.5, 18, 'High', 9.9, 77.1),
            ('Kerala', 'Wayanad', '2023-08-02', 142, 3, 14, 'High', 11.6, 76.1),
            ('Kerala', 'Idukki', '2024-06-25', 148, 3, 16, 'High', 9.9, 77.1),
            ('Kerala', 'Kozhikode', '2024-07-20', 132, 2.5, 10, 'High', 11.2, 75.8),
        ]
       
        cursor.executemany('''
            INSERT INTO cloudburst_history
            (state, district, date, rainfall_mm, duration_hours, casualties, severity, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', cloudburst_data)
       
        # Insert weather data
        weather_data = [
            ('Uttarakhand', 'All Districts', '2025-10-28', 85, 28, 15, 980, 90, 45),
            ('Himachal Pradesh', 'All Districts', '2025-10-28', 82, 26, 12, 985, 85, 38),
            ('Jammu and Kashmir', 'All Districts', '2025-10-28', 75, 24, 10, 990, 70, 25),
            ('Arunachal Pradesh', 'All Districts', '2025-10-28', 88, 27, 14, 982, 88, 42),
            ('Sikkim', 'All Districts', '2025-10-28', 80, 25, 11, 987, 75, 32),
            ('Meghalaya', 'All Districts', '2025-10-28', 90, 29, 16, 978, 92, 55),
            ('West Bengal', 'North Bengal', '2025-10-28', 83, 30, 13, 985, 80, 40),
            ('Assam', 'All Districts', '2025-10-28', 78, 31, 9, 992, 65, 28),
            ('Maharashtra', 'Western Ghats', '2025-10-28', 87, 32, 14, 983, 88, 48),
            ('Kerala', 'All Districts', '2025-10-28', 89, 33, 12, 980, 90, 52),
            # Additional states without historical cloudburst data
            ('Andhra Pradesh', 'All Districts', '2025-10-28', 70, 29, 8, 995, 60, 20),
            ('Bihar', 'All Districts', '2025-10-28', 65, 30, 10, 998, 55, 15),
            ('Chhattisgarh', 'All Districts', '2025-10-28', 72, 28, 12, 992, 65, 25),
            ('Goa', 'All Districts', '2025-10-28', 80, 31, 14, 988, 70, 30),
            ('Gujarat', 'All Districts', '2025-10-28', 60, 32, 5, 1000, 50, 10),
            ('Haryana', 'All Districts', '2025-10-28', 55, 25, 6, 1002, 45, 8),
            ('Jharkhand', 'All Districts', '2025-10-28', 68, 29, 11, 994, 62, 22),
            ('Karnataka', 'All Districts', '2025-10-28', 75, 30, 13, 990, 68, 28),
            ('Madhya Pradesh', 'All Districts', '2025-10-28', 62, 27, 9, 997, 58, 18),
            ('Manipur', 'All Districts', '2025-10-28', 82, 26, 15, 986, 72, 35),
            ('Mizoram', 'All Districts', '2025-10-28', 85, 28, 16, 984, 75, 32),
            ('Nagaland', 'All Districts', '2025-10-28', 78, 27, 14, 988, 70, 28),
            ('Odisha', 'All Districts', '2025-10-28', 70, 29, 12, 993, 65, 25),
            ('Punjab', 'All Districts', '2025-10-28', 58, 26, 7, 999, 52, 12),
            ('Rajasthan', 'All Districts', '2025-10-28', 50, 30, 4, 1005, 40, 5),
            ('Tamil Nadu', 'All Districts', '2025-10-28', 68, 31, 11, 991, 62, 22),
            ('Telangana', 'All Districts', '2025-10-28', 72, 30, 10, 994, 66, 24),
            ('Tripura', 'All Districts', '2025-10-28', 80, 28, 13, 987, 73, 30),
            ('Uttar Pradesh', 'All Districts', '2025-10-28', 60, 29, 8, 996, 55, 18),
        ]
       
        cursor.executemany('''
            INSERT INTO weather_data
            (state, district, date, humidity, temperature, wind_speed, pressure, cloud_cover, precipitation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', weather_data)
       
        conn.commit()
   
    return conn

# Initialize database
conn = init_database()

# Initialize chat history
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# List of all Indian states
all_indian_states = [
    "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh",
    "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand",
    "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
    "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab",
    "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura",
    "Uttar Pradesh", "Uttarakhand", "West Bengal", "Jammu and Kashmir"
]

# Helper functions
def execute_query(query, params=()):
    """Execute SQL query and return results as DataFrame"""
    return pd.read_sql_query(query, conn, params=params)

def get_cloudburst_history(state=None):
    """Get cloudburst history for a specific state or all states"""
    if state:
        query = "SELECT * FROM cloudburst_history WHERE state = ? ORDER BY date DESC"
        return execute_query(query, (state,))
    else:
        query = "SELECT * FROM cloudburst_history ORDER BY date DESC"
        return execute_query(query)

def get_weather_data(state=None):
    """Get current weather data"""
    if state:
        query = "SELECT * FROM weather_data WHERE state = ? ORDER BY date DESC LIMIT 1"
        return execute_query(query, (state,))
    else:
        query = "SELECT * FROM weather_data ORDER BY date DESC"
        return execute_query(query)

def predict_cloudburst(state):
    """Predict cloudburst probability based on historical and weather data"""
    # Get historical data
    historical = get_cloudburst_history(state)
    weather = get_weather_data(state)
   
    if historical.empty:
        return {
            'risk': 'Low',
            'probability': 15,
            'alert': False,
            'message': 'No historical cloudburst data available for this state',
            'color': 'green',
            'total_incidents': 0,
            'recent_incidents': 0,
            'avg_rainfall': 0,
            'max_rainfall': 0,
            'avg_casualties': 0,
            'weather': weather.iloc[0] if not weather.empty else None
        }
   
    # Calculate risk factors
    total_incidents = len(historical)
    recent_incidents = len(historical[historical['date'] >= '2024-01-01'])
    avg_rainfall = historical['rainfall_mm'].mean()
    max_rainfall = historical['rainfall_mm'].max()
    avg_casualties = historical['casualties'].mean()
   
    # Risk scoring
    risk_score = 0
   
    # Historical frequency (0-30 points)
    risk_score += min(total_incidents * 3, 30)
   
    # Recent activity (0-25 points)
    risk_score += min(recent_incidents * 5, 25)
   
    # Current weather conditions (0-45 points)
    if not weather.empty:
        w = weather.iloc[0]
        if w['humidity'] > 80:
            risk_score += 15
        elif w['humidity'] > 70:
            risk_score += 8
       
        if w['pressure'] < 985:
            risk_score += 12
        elif w['pressure'] < 990:
            risk_score += 6
       
        if w['cloud_cover'] > 85:
            risk_score += 10
        elif w['cloud_cover'] > 75:
            risk_score += 5
       
        if w['wind_speed'] > 12:
            risk_score += 8
        elif w['wind_speed'] > 8:
            risk_score += 3
   
    # Determine risk level
    if risk_score >= 70:
        risk = 'Critical'
        alert = True
        color = 'red'
        probability = 85 + (risk_score - 70) / 3
        message = '🚨 EXTREME ALERT: High probability of cloudburst in the next 24-48 hours!'
    elif risk_score >= 50:
        risk = 'High'
        alert = True
        color = 'orange'
        probability = 65 + (risk_score - 50) / 2
        message = '⚠️ HIGH ALERT: Significant cloudburst risk detected. Take precautions!'
    elif risk_score >= 30:
        risk = 'Medium'
        alert = True
        color = 'yellow'
        probability = 40 + (risk_score - 30) / 1.5
        message = '⚠️ MODERATE ALERT: Monitor weather conditions closely.'
    else:
        risk = 'Low'
        alert = False
        color = 'green'
        probability = 15 + risk_score / 2
        message = '✅ LOW RISK: Weather conditions are relatively stable.'
   
    return {
        'risk': risk,
        'probability': min(round(probability, 1), 95),
        'alert': alert,
        'message': message,
        'color': color,
        'total_incidents': total_incidents,
        'recent_incidents': recent_incidents,
        'avg_rainfall': round(avg_rainfall, 1),
        'max_rainfall': round(max_rainfall, 1),
        'avg_casualties': round(avg_casualties, 1),
        'weather': weather.iloc[0] if not weather.empty else None
    }

def query_information(query_type, state, district=None):
    """Query specific information about rainfall, humidity, precipitation"""
    if query_type == "Historical Rainfall":
        if district and district != "All Districts":
            query = """
                SELECT date, district, rainfall_mm, duration_hours
                FROM cloudburst_history
                WHERE state = ? AND district = ?
                ORDER BY date DESC
            """
            return execute_query(query, (state, district))
        else:
            query = """
                SELECT date, district, rainfall_mm, duration_hours
                FROM cloudburst_history
                WHERE state = ?
                ORDER BY date DESC
            """
            return execute_query(query, (state,))
   
    elif query_type == "Current Weather":
        query = """
            SELECT state, humidity, temperature, wind_speed, pressure,
                   cloud_cover, precipitation, date
            FROM weather_data
            WHERE state = ?
            ORDER BY date DESC LIMIT 1
        """
        return execute_query(query, (state,))
   
    elif query_type == "Precipitation Trends":
        query = """
            SELECT date, rainfall_mm as precipitation
            FROM cloudburst_history
            WHERE state = ?
            ORDER BY date
        """
        return execute_query(query, (state,))

# Chatbot functions
def process_chatbot_query(user_query):
    """Process natural language queries and return appropriate responses"""
    query_lower = user_query.lower()
    
    # Extract state names from query
    mentioned_states = [state for state in all_indian_states if state.lower() in query_lower]
    
    try:
        # Query: Which state has most/more cloudbursts?
        if any(word in query_lower for word in ['most cloudburst', 'more cloudburst', 'highest cloudburst', 'maximum cloudburst']):
            result = execute_query("""
                SELECT state, COUNT(*) as total_incidents
                FROM cloudburst_history
                GROUP BY state
                ORDER BY total_incidents DESC
                LIMIT 5
            """)
            
            if not result.empty:
                response = "**📊 States with Most Cloudbursts:**\n\n"
                for idx, row in result.iterrows():
                    response += f"{idx+1}. **{row['state']}**: {row['total_incidents']} incidents\n"
                return response, result
        
        # Query: Least cloudbursts / Safest places
        elif any(word in query_lower for word in ['least cloudburst', 'fewest cloudburst', 'lowest cloudburst', 'safest', 'safe place', 'safe state', 'safer']):
            result = execute_query("""
                SELECT state, COUNT(*) as total_incidents
                FROM cloudburst_history
                GROUP BY state
                ORDER BY total_incidents ASC
                LIMIT 5
            """)
            
            if not result.empty:
                response = "**✅ Safest States (Least Cloudbursts):**\n\n"
                for idx, row in result.iterrows():
                    response += f"{idx+1}. **{row['state']}**: {row['total_incidents']} incidents\n"
                response += "\n💡 These states have experienced the fewest cloudburst incidents historically."
                return response, result
        
        # Query: States with no cloudbursts
        elif 'no cloudburst' in query_lower or 'zero cloudburst' in query_lower or 'never had' in query_lower:
            all_states_df = pd.DataFrame({'state': all_indian_states})
            states_with_cloudbursts = execute_query("SELECT DISTINCT state FROM cloudburst_history")
            safe_states = all_states_df[~all_states_df['state'].isin(states_with_cloudbursts['state'])]
            
            if not safe_states.empty:
                response = "**🛡️ States with No Recorded Cloudbursts:**\n\n"
                for idx, state in enumerate(safe_states['state'].values, 1):
                    response += f"{idx}. **{state}**\n"
                response += f"\n✅ Total: **{len(safe_states)}** states have no historical cloudburst data."
                return response, safe_states
            else:
                return "All states with available data have experienced cloudbursts.", None
        
        # Query: Most dangerous/deadliest
        elif any(word in query_lower for word in ['dangerous', 'deadliest', 'most fatal', 'most casualties', 'worst']):
            result = execute_query("""
                SELECT state, SUM(casualties) as total_casualties, COUNT(*) as incidents
                FROM cloudburst_history
                GROUP BY state
                ORDER BY total_casualties DESC
                LIMIT 5
            """)
            
            if not result.empty:
                response = "**💔 Most Dangerous States (By Casualties):**\n\n"
                for idx, row in result.iterrows():
                    response += f"{idx+1}. **{row['state']}**: {int(row['total_casualties'])} casualties ({int(row['incidents'])} incidents)\n"
                return response, result
        
        # Query: When do cloudbursts occur most
        elif any(word in query_lower for word in ['when', 'which month', 'what month', 'season', 'time of year']):
            result = execute_query("""
                SELECT 
                    SUBSTR(date, 6, 2) as month,
                    COUNT(*) as incidents
                FROM cloudburst_history
                GROUP BY month
                ORDER BY incidents DESC
            """)
            
            if not result.empty:
                month_names = {
                    '01': 'January', '02': 'February', '03': 'March', '04': 'April',
                    '05': 'May', '06': 'June', '07': 'July', '08': 'August',
                    '09': 'September', '10': 'October', '11': 'November', '12': 'December'
                }
                result['month_name'] = result['month'].map(month_names)
                
                response = "**📅 Cloudburst Frequency by Month:**\n\n"
                for idx, row in result.iterrows():
                    response += f"**{row['month_name']}**: {row['incidents']} incidents\n"
                response += "\n💡 Cloudbursts are most common during monsoon months (June-August)."
                return response, result
        
        # Query: Districts with most cloudbursts
        elif 'district' in query_lower and any(word in query_lower for word in ['most', 'highest', 'top']):
            if mentioned_states:
                state = mentioned_states[0]
                result = execute_query("""
                    SELECT district, COUNT(*) as incidents, SUM(casualties) as casualties
                    FROM cloudburst_history
                    WHERE state = ?
                    GROUP BY district
                    ORDER BY incidents DESC
                """, (state,))
                
                if not result.empty:
                    response = f"**🏘️ Most Affected Districts in {state}:**\n\n"
                    for idx, row in result.iterrows():
                        response += f"{idx+1}. **{row['district']}**: {row['incidents']} incidents, {int(row['casualties'])} casualties\n"
                    return response, result
            else:
                result = execute_query("""
                    SELECT state, district, COUNT(*) as incidents
                    FROM cloudburst_history
                    GROUP BY state, district
                    ORDER BY incidents DESC
                    LIMIT 10
                """)
                
                if not result.empty:
                    response = "**🏘️ Top 10 Most Affected Districts:**\n\n"
                    for idx, row in result.iterrows():
                        response += f"{idx+1}. **{row['district']}, {row['state']}**: {row['incidents']} incidents\n"
                    return response, result
        
        # Query: Duration/intensity
        elif any(word in query_lower for word in ['duration', 'how long', 'longest', 'shortest']):
            if 'longest' in query_lower:
                result = execute_query("""
                    SELECT state, district, date, duration_hours, rainfall_mm
                    FROM cloudburst_history
                    ORDER BY duration_hours DESC
                    LIMIT 5
                """)
                
                if not result.empty:
                    response = "**⏱️ Longest Duration Cloudbursts:**\n\n"
                    for idx, row in result.iterrows():
                        response += f"{idx+1}. **{row['state']}, {row['district']}** ({row['date']}): {row['duration_hours']} hours, {row['rainfall_mm']} mm\n"
                    return response, result
            else:
                result = execute_query("""
                    SELECT ROUND(AVG(duration_hours), 2) as avg_duration,
                           ROUND(MIN(duration_hours), 2) as min_duration,
                           ROUND(MAX(duration_hours), 2) as max_duration
                    FROM cloudburst_history
                """)
                
                if not result.empty:
                    row = result.iloc[0]
                    response = "**⏱️ Cloudburst Duration Statistics:**\n\n"
                    response += f"Average duration: **{row['avg_duration']} hours**\n"
                    response += f"Shortest duration: **{row['min_duration']} hours**\n"
                    response += f"Longest duration: **{row['max_duration']} hours**"
                    return response, result
        
        # Query: Year comparison
        elif any(word in query_lower for word in ['2023 vs 2024', 'compare 2023', 'compare 2024', 'year comparison']):
            result = execute_query("""
                SELECT 
                    SUBSTR(date, 1, 4) as year,
                    COUNT(*) as incidents,
                    SUM(casualties) as casualties,
                    ROUND(AVG(rainfall_mm), 2) as avg_rainfall
                FROM cloudburst_history
                GROUP BY year
                ORDER BY year
            """)
            
            if not result.empty:
                response = "**📊 Year-wise Comparison:**\n\n"
                for idx, row in result.iterrows():
                    response += f"**{row['year']}**: {row['incidents']} incidents, {int(row['casualties'])} casualties, {row['avg_rainfall']} mm avg rainfall\n"
                return response, result
        
        # Query: High severity incidents
        elif 'high severity' in query_lower or 'severe' in query_lower:
            if mentioned_states:
                state = mentioned_states[0]
                result = execute_query("""
                    SELECT district, date, rainfall_mm, casualties
                    FROM cloudburst_history
                    WHERE state = ? AND severity = 'High'
                    ORDER BY rainfall_mm DESC
                """, (state,))
                
                if not result.empty:
                    response = f"**⚠️ High Severity Cloudbursts in {state}:**\n\n"
                    response += f"Total high severity incidents: **{len(result)}**\n\n"
                    return response, result
            else:
                result = execute_query("""
                    SELECT state, COUNT(*) as high_severity_count
                    FROM cloudburst_history
                    WHERE severity = 'High'
                    GROUP BY state
                    ORDER BY high_severity_count DESC
                """)
                
                if not result.empty:
                    response = "**⚠️ High Severity Cloudbursts by State:**\n\n"
                    for idx, row in result.iterrows():
                        response += f"{idx+1}. **{row['state']}**: {row['high_severity_count']} high severity incidents\n"
                    return response, result
        
        # Query: Trend analysis
        elif any(word in query_lower for word in ['trend', 'increasing', 'decreasing', 'getting worse', 'getting better']):
            result = execute_query("""
                SELECT 
                    SUBSTR(date, 1, 4) as year,
                    COUNT(*) as incidents
                FROM cloudburst_history
                GROUP BY year
                ORDER BY year
            """)
            
            if not result.empty and len(result) > 1:
                trend = "increasing" if result['incidents'].iloc[-1] > result['incidents'].iloc[0] else "decreasing"
                response = f"**📈 Cloudburst Trend Analysis:**\n\n"
                for idx, row in result.iterrows():
                    response += f"**{row['year']}**: {row['incidents']} incidents\n"
                response += f"\n💡 The trend shows {trend} frequency from 2023 to 2024."
                return response, result
        
        # Query: Risk level / prediction
        elif any(word in query_lower for word in ['risk', 'prediction', 'forecast', 'likely']) and mentioned_states:
            state = mentioned_states[0]
            prediction = predict_cloudburst(state)
            
            response = f"**🔮 Risk Assessment for {state}:**\n\n"
            response += f"Risk Level: **{prediction['risk']}**\n"
            response += f"Probability: **{prediction['probability']}%**\n"
            response += f"Total Incidents: **{prediction['total_incidents']}**\n"
            response += f"Recent Incidents (2024): **{prediction['recent_incidents']}**\n\n"
            response += f"💡 {prediction['message']}"
            
            return response, None
            
        # Query: Total casualties
        elif 'total casualties' in query_lower or 'how many deaths' in query_lower or 'total deaths' in query_lower:
            if mentioned_states:
                state = mentioned_states[0]
                result = execute_query("""
                    SELECT state, SUM(casualties) as total_casualties, COUNT(*) as incidents
                    FROM cloudburst_history
                    WHERE state = ?
                    GROUP BY state
                """, (state,))
                
                if not result.empty:
                    response = f"**💔 Casualties in {state}:**\n\n"
                    response += f"Total casualties: **{int(result['total_casualties'].iloc[0])}**\n"
                    response += f"Total incidents: **{int(result['incidents'].iloc[0])}**"
                    return response, result
            else:
                result = execute_query("""
                    SELECT SUM(casualties) as total_casualties, COUNT(*) as total_incidents
                    FROM cloudburst_history
                """)
                
                response = f"**💔 Overall Casualties:**\n\n"
                response += f"Total casualties: **{int(result['total_casualties'].iloc[0])}**\n"
                response += f"Total incidents: **{int(result['total_incidents'].iloc[0])}**"
                return response, result
        
        # Query: Highest/maximum rainfall
        elif 'highest rainfall' in query_lower or 'maximum rainfall' in query_lower or 'most rainfall' in query_lower:
            if mentioned_states:
                state = mentioned_states[0]
                result = execute_query("""
                    SELECT state, district, date, rainfall_mm, severity
                    FROM cloudburst_history
                    WHERE state = ?
                    ORDER BY rainfall_mm DESC
                    LIMIT 1
                """, (state,))
            else:
                result = execute_query("""
                    SELECT state, district, date, rainfall_mm, severity
                    FROM cloudburst_history
                    ORDER BY rainfall_mm DESC
                    LIMIT 1
                """)
            
            if not result.empty:
                row = result.iloc[0]
                response = f"**🌧️ Highest Rainfall Record:**\n\n"
                response += f"State: **{row['state']}**\n"
                response += f"District: **{row['district']}**\n"
                response += f"Date: **{row['date']}**\n"
                response += f"Rainfall: **{row['rainfall_mm']} mm**\n"
                response += f"Severity: **{row['severity']}**"
                return response, result
        
        # Query: Information about specific state
        elif mentioned_states:
            state = mentioned_states[0]
            
            # Get state statistics
            stats = execute_query("""
                SELECT 
                    COUNT(*) as total_incidents,
                    ROUND(AVG(rainfall_mm), 2) as avg_rainfall,
                    ROUND(MAX(rainfall_mm), 2) as max_rainfall,
                    SUM(casualties) as total_casualties
                FROM cloudburst_history
                WHERE state = ?
            """, (state,))
            
            recent = execute_query("""
                SELECT COUNT(*) as recent_incidents
                FROM cloudburst_history
                WHERE state = ? AND date >= '2024-01-01'
            """, (state,))
            
            if not stats.empty and stats['total_incidents'].iloc[0] > 0:
                response = f"**📊 Cloudburst Information for {state}:**\n\n"
                response += f"Total incidents: **{int(stats['total_incidents'].iloc[0])}**\n"
                response += f"Recent incidents (2024): **{int(recent['recent_incidents'].iloc[0])}**\n"
                response += f"Average rainfall: **{stats['avg_rainfall'].iloc[0]} mm**\n"
                response += f"Maximum rainfall: **{stats['max_rainfall'].iloc[0]} mm**\n"
                response += f"Total casualties: **{int(stats['total_casualties'].iloc[0])}**"
                return response, stats
            else:
                return f"No historical cloudburst data found for **{state}**.", None
        
        # Query: Recent cloudbursts or 2024 data
        elif '2024' in query_lower or 'recent' in query_lower or 'latest' in query_lower:
            result = execute_query("""
                SELECT state, district, date, rainfall_mm, casualties, severity
                FROM cloudburst_history
                WHERE date >= '2024-01-01'
                ORDER BY date DESC
                LIMIT 10
            """)
            
            if not result.empty:
                response = "**📅 Recent Cloudbursts (2024):**\n\n"
                response += f"Total incidents in 2024: **{len(result)}**\n\n"
                return response, result
        
        # Query: Severity levels
        elif 'severity' in query_lower or 'high severity' in query_lower:
            result = execute_query("""
                SELECT severity, COUNT(*) as count
                FROM cloudburst_history
                GROUP BY severity
                ORDER BY count DESC
            """)
            
            if not result.empty:
                response = "**⚠️ Cloudbursts by Severity:**\n\n"
                for idx, row in result.iterrows():
                    response += f"**{row['severity']}**: {row['count']} incidents\n"
                return response, result
        
        # Query: Compare states
        elif 'compare' in query_lower and len(mentioned_states) >= 2:
            state1, state2 = mentioned_states[0], mentioned_states[1]
            result = execute_query("""
                SELECT 
                    state,
                    COUNT(*) as incidents,
                    ROUND(AVG(rainfall_mm), 2) as avg_rainfall,
                    SUM(casualties) as casualties
                FROM cloudburst_history
                WHERE state IN (?, ?)
                GROUP BY state
            """, (state1, state2))
            
            if not result.empty:
                response = f"**⚖️ Comparison: {state1} vs {state2}**\n\n"
                return response, result
        
        # Query: Average rainfall
        elif 'average rainfall' in query_lower or 'avg rainfall' in query_lower:
            if mentioned_states:
                state = mentioned_states[0]
                result = execute_query("""
                    SELECT ROUND(AVG(rainfall_mm), 2) as avg_rainfall
                    FROM cloudburst_history
                    WHERE state = ?
                """, (state,))
                
                if not result.empty:
                    response = f"**🌧️ Average Rainfall in {state}:**\n\n"
                    response += f"**{result['avg_rainfall'].iloc[0]} mm**"
                    return response, result
            else:
                result = execute_query("""
                    SELECT state, ROUND(AVG(rainfall_mm), 2) as avg_rainfall
                    FROM cloudburst_history
                    GROUP BY state
                    ORDER BY avg_rainfall DESC
                    LIMIT 5
                """)
                
                response = "**🌧️ Top States by Average Rainfall:**\n\n"
                for idx, row in result.iterrows():
                    response += f"{idx+1}. **{row['state']}**: {row['avg_rainfall']} mm\n"
                return response, result
        
        # Default: List all states with data
        else:
            result = execute_query("""
                SELECT DISTINCT state
                FROM cloudburst_history
                ORDER BY state
            """)
            
            response = "**💬 I can help you with cloudburst information!**\n\n"
            response += "Try asking me:\n"
            response += "- Which state has the most cloudbursts?\n"
            response += "- What are the total casualties?\n"
            response += "- Tell me about cloudbursts in [state name]\n"
            response += "- What was the highest rainfall recorded?\n"
            response += "- Show me recent cloudbursts\n"
            response += "- Compare [state1] and [state2]\n\n"
            response += f"I have data for {len(result)} states."
            return response, result
            
    except Exception as e:
        return f"❌ Sorry, I encountered an error: {str(e)}", None

# Main UI
st.title("🌧️ Cloudburst Prediction System - India")
st.markdown("### Real-time Weather Analysis & Historical Data (2023-2024)")

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.radio(
    "Select Page",
    ["🏠 Home & Prediction", "💬 Chatbot Assistant", "📊 Database Explorer", "🔍 Query Information"]
)

if page == "🏠 Home & Prediction":
    st.header("Cloudburst Risk Assessment")
   
    # Use all Indian states for dropdown
    states_list = sorted(all_indian_states)
   
    col1, col2 = st.columns([2, 1])
   
    with col1:
        selected_state = st.selectbox(
            "Select State",
            [""] + states_list,
            help="Choose a state to check cloudburst risk"
        )
   
    with col2:
        predict_btn = st.button("🔮 Predict Risk", type="primary", use_container_width=True)
   
    if predict_btn and selected_state:
        st.divider()
       
        # Get prediction
        prediction = predict_cloudburst(selected_state)
       
        # Display alert box
        if prediction['color'] == 'red':
            st.error(f"### {prediction['message']}")
        elif prediction['color'] == 'orange':
            st.warning(f"### {prediction['message']}")
        elif prediction['color'] == 'yellow':
            st.info(f"### {prediction['message']}")
        else:
            st.success(f"### {prediction['message']}")
       
        # Risk metrics
        col1, col2, col3, col4 = st.columns(4)
       
        with col1:
            st.metric("Risk Level", prediction['risk'])
        with col2:
            st.metric("Probability", f"{prediction['probability']}%")
        with col3:
            st.metric("Total Incidents", prediction['total_incidents'])
        with col4:
            st.metric("Recent (2024)", prediction['recent_incidents'])
       
        st.divider()
       
        # Weather conditions
        if prediction['weather'] is not None:
            st.subheader("🌤️ Current Weather Conditions")
           
            wcol1, wcol2, wcol3, wcol4, wcol5 = st.columns(5)
           
            with wcol1:
                st.metric("💧 Humidity", f"{prediction['weather']['humidity']}%")
            with wcol2:
                st.metric("🌡️ Temperature", f"{prediction['weather']['temperature']}°C")
            with wcol3:
                st.metric("💨 Wind Speed", f"{prediction['weather']['wind_speed']} km/h")
            with wcol4:
                st.metric("🔽 Pressure", f"{prediction['weather']['pressure']} mb")
            with wcol5:
                st.metric("☁️ Cloud Cover", f"{prediction['weather']['cloud_cover']}%")
           
            st.metric("🌧️ Precipitation", f"{prediction['weather']['precipitation']} mm")
       
        st.divider()
       
        # Historical data
        st.subheader(f"📜 Historical Cloudburst Records - {selected_state}")
        historical_df = get_cloudburst_history(selected_state)
       
        if not historical_df.empty:
            # Display statistics
            stats_col1, stats_col2, stats_col3 = st.columns(3)
           
            with stats_col1:
                st.metric("Average Rainfall", f"{prediction['avg_rainfall']} mm")
            with stats_col2:
                st.metric("Maximum Rainfall", f"{prediction['max_rainfall']} mm")
            with stats_col3:
                st.metric("Avg Casualties", f"{prediction['avg_casualties']}")
           
            # Display table
            display_df = historical_df[['date', 'district', 'rainfall_mm', 'duration_hours', 'casualties', 'severity']]
            st.dataframe(display_df, use_container_width=True, hide_index=True)
           
            # Visualization
            st.subheader("📈 Rainfall Trend Analysis")
            fig = px.bar(
                historical_df,
                x='date',
                y='rainfall_mm',
                color='severity',
                title=f'Cloudburst Rainfall History - {selected_state}',
                labels={'rainfall_mm': 'Rainfall (mm)', 'date': 'Date'},
                color_discrete_map={'High': '#ff4444', 'Medium': '#ffaa00'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No historical data available for this state.")
   
    elif predict_btn:
        st.warning("⚠️ Please select a state first!")

elif page == "💬 Chatbot Assistant":
    st.header("🤖 Cloudburst Information Chatbot")
    st.markdown("Ask me anything about cloudbursts in India! I can answer questions about rainfall, casualties, state comparisons, and more.")
    
    # Display chat history
    chat_container = st.container()
    with chat_container:
        for chat in st.session_state.chat_history:
            with st.chat_message("user"):
                st.write(chat['user'])
            with st.chat_message("assistant"):
                st.markdown(chat['bot'])
                if chat.get('data') is not None and not chat['data'].empty:
                    st.dataframe(chat['data'], use_container_width=True, hide_index=True)
    
    # Chat input
    user_input = st.chat_input("Ask me about cloudbursts... (e.g., 'Which state has the most cloudbursts?')")
    
    if user_input:
        # Process the query
        response, data = process_chatbot_query(user_input)
        
        # Add to chat history
        st.session_state.chat_history.append({
            'user': user_input,
            'bot': response,
            'data': data
        })
        
        # Rerun to display new message
        st.rerun()
    
    # Quick query buttons
    st.markdown("---")
    st.markdown("**💡 Quick Queries:**")
    qcol1, qcol2, qcol3, qcol4 = st.columns(4)
    
    with qcol1:
        if st.button("🏆 Most Cloudbursts", use_container_width=True):
            response, data = process_chatbot_query("Which state has the most cloudbursts?")
            st.session_state.chat_history.append({
                'user': "Which state has the most cloudbursts?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol2:
        if st.button("✅ Safest States", use_container_width=True):
            response, data = process_chatbot_query("Which are the safest states?")
            st.session_state.chat_history.append({
                'user': "Which are the safest states?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol3:
        if st.button("💔 Most Dangerous", use_container_width=True):
            response, data = process_chatbot_query("Which is the most dangerous state?")
            st.session_state.chat_history.append({
                'user': "Which is the most dangerous state?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol4:
        if st.button("📅 Monthly Trends", use_container_width=True):
            response, data = process_chatbot_query("When do cloudbursts occur most?")
            st.session_state.chat_history.append({
                'user': "When do cloudbursts occur most?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    qcol5, qcol6, qcol7, qcol8 = st.columns(4)
    
    with qcol5:
        if st.button("🛡️ No Cloudbursts", use_container_width=True):
            response, data = process_chatbot_query("Which states have no cloudbursts?")
            st.session_state.chat_history.append({
                'user': "Which states have no cloudbursts?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol6:
        if st.button("🏘️ Top Districts", use_container_width=True):
            response, data = process_chatbot_query("Which districts have the most cloudbursts?")
            st.session_state.chat_history.append({
                'user': "Which districts have the most cloudbursts?",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol7:
        if st.button("📊 Year Comparison", use_container_width=True):
            response, data = process_chatbot_query("Compare 2023 and 2024")
            st.session_state.chat_history.append({
                'user': "Compare 2023 and 2024",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    with qcol8:
        if st.button("📈 Trends", use_container_width=True):
            response, data = process_chatbot_query("Show me cloudburst trends")
            st.session_state.chat_history.append({
                'user': "Show me cloudburst trends",
                'bot': response,
                'data': data
            })
            st.rerun()
    
    # Clear chat button
    if st.button("🗑️ Clear Chat History"):
        st.session_state.chat_history = []
        st.rerun()

elif page == "📊 Database Explorer":
    st.header("Complete Cloudburst Database (2023-2024)")
   
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📋 All Records", "🗺️ State-wise Analysis", "📊 Statistics"])
   
    with tab1:
        st.subheader("Complete Cloudburst History")
        all_data = get_cloudburst_history()
       
        # Filters
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            filter_state = st.multiselect("Filter by State", all_data['state'].unique())
        with fcol2:
            filter_severity = st.multiselect("Filter by Severity", all_data['severity'].unique())
        with fcol3:
            filter_year = st.multiselect("Filter by Year", ['2023', '2024'])
       
        # Apply filters
        filtered_data = all_data.copy()
        if filter_state:
            filtered_data = filtered_data[filtered_data['state'].isin(filter_state)]
        if filter_severity:
            filtered_data = filtered_data[filtered_data['severity'].isin(filter_severity)]
        if filter_year:
            filtered_data = filtered_data[filtered_data['date'].str.startswith(tuple(filter_year))]
       
        st.dataframe(filtered_data, use_container_width=True, hide_index=True)
       
        # Download button
        csv = filtered_data.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name="cloudburst_data.csv",
            mime="text/csv"
        )
   
    with tab2:
        st.subheader("State-wise Cloudburst Analysis")
       
        state_stats = execute_query("""
            SELECT
                state,
                COUNT(*) as total_incidents,
                ROUND(AVG(rainfall_mm), 2) as avg_rainfall,
                ROUND(MAX(rainfall_mm), 2) as max_rainfall,
                SUM(casualties) as total_casualties
            FROM cloudburst_history
            GROUP BY state
            ORDER BY total_incidents DESC
        """)
       
        st.dataframe(state_stats, use_container_width=True, hide_index=True)
       
        # Visualization
        fig1 = px.bar(
            state_stats,
            x='state',
            y='total_incidents',
            title='Total Cloudburst Incidents by State',
            labels={'total_incidents': 'Number of Incidents', 'state': 'State'}
        )
        st.plotly_chart(fig1, use_container_width=True)
       
        fig2 = px.bar(
            state_stats,
            x='state',
            y='avg_rainfall',
            title='Average Rainfall by State (mm)',
            labels={'avg_rainfall': 'Average Rainfall (mm)', 'state': 'State'}
        )
        st.plotly_chart(fig2, use_container_width=True)
   
    with tab3:
        st.subheader("Overall Statistics")
       
        total_incidents = execute_query("SELECT COUNT(*) as count FROM cloudburst_history")
        total_casualties = execute_query("SELECT SUM(casualties) as total FROM cloudburst_history")
        avg_rainfall = execute_query("SELECT AVG(rainfall_mm) as avg FROM cloudburst_history")
       
        mcol1, mcol2, mcol3 = st.columns(3)
        with mcol1:
            st.metric("Total Incidents", total_incidents['count'].iloc[0])
        with mcol2:
            st.metric("Total Casualties", int(total_casualties['total'].iloc[0]))
        with mcol3:
            st.metric("Avg Rainfall", f"{avg_rainfall['avg'].iloc[0]:.2f} mm")
       
        # Severity distribution
        severity_dist = execute_query("""
            SELECT severity, COUNT(*) as count
            FROM cloudburst_history
            GROUP BY severity
        """)
       
        fig = px.pie(
            severity_dist,
            values='count',
            names='severity',
            title='Cloudburst Severity Distribution',
            color='severity',
            color_discrete_map={'High': '#ff4444', 'Medium': '#ffaa00'}
        )
        st.plotly_chart(fig, use_container_width=True)

elif page == "🔍 Query Information":
    st.header("Query Weather & Rainfall Information")
    st.markdown("Get specific information about rainfall, humidity, precipitation for any state")
   
    # Use all Indian states for dropdown
    states_list_query = sorted(all_indian_states)
   
    # Query interface
    qcol1, qcol2 = st.columns(2)
   
    with qcol1:
        query_type = st.selectbox(
            "Select Query Type",
            ["Historical Rainfall", "Current Weather", "Precipitation Trends"]
        )
   
    with qcol2:
        query_state = st.selectbox("Select State", states_list_query)
   
    # Optional district filter for historical data
    query_district = None
    if query_type == "Historical Rainfall":
        districts = execute_query(
            "SELECT DISTINCT district FROM cloudburst_history WHERE state = ? ORDER BY district",
            (query_state,)
        )
        if districts.empty:
            st.info("No districts available for historical rainfall in this state.")
        else:
            query_district = st.selectbox(
                "Select District (Optional)",
                ["All Districts"] + districts['district'].tolist()
            )
   
    if st.button("🔍 Execute Query", type="primary"):
        result = query_information(query_type, query_state, query_district)
       
        if not result.empty:
            st.success(f"✅ Query executed successfully! Found {len(result)} records.")
           
            if query_type == "Historical Rainfall":
                st.subheader(f"📊 Historical Rainfall Data - {query_state}")
                st.dataframe(result, use_container_width=True, hide_index=True)
               
                # Summary statistics
                st.markdown("### Summary Statistics")
                scol1, scol2, scol3 = st.columns(3)
                with scol1:
                    st.metric("Total Records", len(result))
                with scol2:
                    st.metric("Avg Rainfall", f"{result['rainfall_mm'].mean():.2f} mm")
                with scol3:
                    st.metric("Max Rainfall", f"{result['rainfall_mm'].max():.2f} mm")
               
                # Chart
                fig = px.line(
                    result,
                    x='date',
                    y='rainfall_mm',
                    title='Rainfall Trend Over Time',
                    markers=True
                )
                st.plotly_chart(fig, use_container_width=True)
           
            elif query_type == "Current Weather":
                st.subheader(f"🌤️ Current Weather Conditions - {query_state}")
                weather_data = result.iloc[0]
               
                wcol1, wcol2, wcol3 = st.columns(3)
                with wcol1:
                    st.metric("💧 Humidity", f"{weather_data['humidity']}%")
                    st.metric("🌡️ Temperature", f"{weather_data['temperature']}°C")
                with wcol2:
                    st.metric("💨 Wind Speed", f"{weather_data['wind_speed']} km/h")
                    st.metric("🔽 Pressure", f"{weather_data['pressure']} mb")
                with wcol3:
                    st.metric("☁️ Cloud Cover", f"{weather_data['cloud_cover']}%")
                    st.metric("🌧️ Precipitation", f"{weather_data['precipitation']} mm")
               
                st.info(f"📅 Data as of: {weather_data['date']}")
           
            elif query_type == "Precipitation Trends":
                st.subheader(f"🌧️ Precipitation Trends - {query_state}")
                st.dataframe(result, use_container_width=True, hide_index=True)
               
                fig = px.area(
                    result,
                    x='date',
                    y='precipitation',
                    title='Precipitation Trend Analysis',
                    labels={'precipitation': 'Precipitation (mm)', 'date': 'Date'}
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No data found for the selected query.")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p>🌧️ Cloudburst Prediction System | Data Source: Historical Records 2023-2024</p>
    <p>⚠️ This is a predictive system. Always follow official weather advisories.</p>
</div>
""", unsafe_allow_html=True)

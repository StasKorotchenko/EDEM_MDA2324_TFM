import streamlit as st
import requests
import pandas as pd
import plotly.graph_objs as go
from datetime import datetime

st.set_page_config(page_title="Customer Prediction App", page_icon=":chart_with_upwards_trend:")

# Custom CSS styling
st.markdown(
    """
    <style>
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        border: none; /* Remove border */
        transition: color 0.3s;
        font-size: 16px;
        padding: 10px 20px;
    }
    .stButton>button:hover, .stButton>button:active {
        background-color: #4CAF50; /* Maintain green background */
        color: white; /* Maintain white text */
        border: none; /* Remove border */
    }
    .low-demand {
        background-color: #E0FFFF;
    }
    .high-demand {
        background-color: #B0E0E6;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Title of the application
st.title("Customer Prediction App")

# Page selection
page = st.sidebar.selectbox("Select the prediction model", ["Select a model", "Cluster Prediction", "Demand Prediction"])

if page == "Select a model":
    st.write("Please select a prediction model from the sidebar.")
elif page == "Cluster Prediction":
    st.write("Enter the data below to get a cluster prediction.")
    
    # UI for data input
    total_spent = st.number_input('Total spent', min_value=0.0, format="%f")
    purchase_frequency = st.number_input('Total purchases', min_value=0.0, format="%f")
    average_order_value = st.number_input('Average order value', min_value=0.0, format="%f")
    num_reviews = st.number_input('Number of reviews', min_value=0, format="%d")
    avg_review_score = st.number_input('Average review score', min_value=0.0, max_value=5.0, format="%f")
    days_since_last_purchase = st.number_input('Days since last purchase', min_value=0.0, format="%f")

    # If the Predict button is pressed
    if st.button("Predict Cluster"):
        with st.spinner("Processing data... Please wait."):
            # Prepare the data for the POST request
            data = {
                "total_spent": total_spent,
                "purchase_frequency": purchase_frequency,
                "average_order_value": average_order_value,
                "num_reviews": num_reviews,
                "avg_review_score": avg_review_score,
                "days_since_last_purchase": days_since_last_purchase
            }

            try:
                # Send the POST request
                response = requests.post("https://fastapi-app-v47hoksqvq-no.a.run.app/predict", json=data)
                
                if response.status_code == 200:
                    prediction = response.json().get("prediction", "Unknown")
                    st.success(f"Client cluster prediction: {prediction}")
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")
elif page == "Demand Prediction":
    st.write("Enter the number of days and optionally the start date to get a demand prediction.")
    
    # UI for days and start date input
    days = st.number_input('Number of Days for Prediction', min_value=1, format="%d")
    start_date = st.date_input("Select Start Date", value=datetime.today())
    
    # If the Predict button is pressed
    if st.button("Predict Demand"):
        with st.spinner("Processing data... Please wait."):
            # Prepare the data for the POST request
            data = {
                "days": int(days),
                "start_date": start_date.strftime("%Y-%m-%d")  # Convert date to string
            }

            try:
                # Send the POST request
                response = requests.post("https://fastapi-app-v47hoksqvq-no.a.run.app/demand_predict", json=data)
                
                if response.status_code == 200:
                    forecast = response.json().get("forecast", [])
                    
                    # Convert data to DataFrame for display
                    if forecast:
                        df = pd.DataFrame(forecast)
                        df['ds'] = pd.to_datetime(df['ds'])
                        df['Date'] = df['ds'].dt.strftime('%Y-%m-%d (%A)')  # Date with weekday
                        df = df[['Date', 'yhat', 'yhat_lower', 'yhat_upper']]
                        
                        # Round values to two decimal places
                        df['yhat'] = df['yhat'].round(2)
                        df['yhat_lower'] = df['yhat_lower'].round(2)
                        df['yhat_upper'] = df['yhat_upper'].round(2)

                        df.rename(columns={
                            'yhat': 'Predicted Demand',
                            'yhat_lower': 'Lower Bound',
                            'yhat_upper': 'Upper Bound'
                        }, inplace=True)

                        # Formatting values in the table
                        def format_values(x):
                            return f"{x:.2f}"

                        # Create the Plotly graph
                        fig = go.Figure()

                        # Forecast line with markers
                        fig.add_trace(go.Scatter(
                            x=df['Date'], 
                            y=df['Predicted Demand'], 
                            mode='lines+markers', 
                            name='Demand',
                            line=dict(color='blue'),
                            marker=dict(size=8)
                        ))

                        # Fill area between yhat_lower and yhat_upper
                        fig.add_trace(go.Scatter(
                            x=df['Date'].tolist() + df['Date'].tolist()[::-1],
                            y=df['Upper Bound'].tolist() + df['Lower Bound'].tolist()[::-1],
                            fill='toself',
                            fillcolor='rgba(173, 216, 230, 0.2)',  # Light blue color
                            line=dict(color='rgba(255,255,255,0)'),
                            hoverinfo="skip",
                            showlegend=False
                        ))

                        # Layout configuration
                        fig.update_layout(
                            title='Demand Forecast',
                            yaxis_title='Demand',
                            xaxis_title='',  # Remove X-axis label
                            hovermode='x',
                            template='plotly_white'
                        )

                        # Display the graph
                        st.plotly_chart(fig)

                        # Highlight extremes in the table
                        def highlight_extremes(row):
                            styles = []
                            for value in row:
                                if value == df['Predicted Demand'].max():
                                    styles.append('background-color: #CCFFCC')  # Light green
                                elif value == df['Predicted Demand'].min():
                                    styles.append('background-color: #FFCCCB')  # Light red
                                else:
                                    styles.append('')
                            return styles

                        # Display the DataFrame with formatting
                        st.write("Demand prediction:")
                        st.dataframe(df.style.apply(highlight_extremes, axis=1)
                                          .format(formatter={'Predicted Demand': format_values,
                                                              'Lower Bound': format_values,
                                                              'Upper Bound': format_values}))
                    else:
                        st.write("No data available for the given forecast.")
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")

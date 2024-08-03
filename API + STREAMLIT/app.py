import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="Customer Prediction App", page_icon=":chart_with_upwards_trend:")

# Estilo CSS personalizado
st.markdown(
    """
    <style>
    .stButton>button {
        background-color: #4CAF50;
        color: white;
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
    total_spent = st.number_input('Total Spent', min_value=0.0, format="%f")
    purchase_frequency = st.number_input('Purchase Frequency', min_value=0.0, format="%f")
    average_order_value = st.number_input('Average Order Value', min_value=0.0, format="%f")
    num_reviews = st.number_input('Number of Reviews', min_value=0, format="%d")
    avg_review_score = st.number_input('Average Review Score', min_value=0.0, max_value=5.0, format="%f")
    days_since_last_purchase = st.number_input('Days Since Last Purchase', min_value=0.0, format="%f")

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
                response = requests.post("http://localhost:8000/predict", json=data)
                
                if response.status_code == 200:
                    prediction = response.json().get("prediction", "Unknown")
                    st.success(f"Client cluster prediction: {prediction}")
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")
elif page == "Demand Prediction":
    st.write("Enter the number of days to get a demand prediction.")
    
    # UI for days input
    days = st.number_input('Number of Days for Prediction', min_value=1, format="%d")

    # If the Predict button is pressed
    if st.button("Predict Demand"):
        with st.spinner("Processing data... Please wait."):
            # Prepare the data for the POST request
            data = {
                "days": int(days)  # Преобразование дней в целое число
            }

            try:
                # Send the POST request
                response = requests.post("http://localhost:8000/demand_predict", json=data)
                
                if response.status_code == 200:
                    forecast = response.json().get("forecast", [])
                    
                    # Преобразуем данные в DataFrame для отображения
                    if forecast:
                        df = pd.DataFrame(forecast)
                        df['ds'] = pd.to_datetime(df['ds'])
                        df = df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
                        df['yhat'] = df['yhat'].round(2)
                        df['yhat_lower'] = df['yhat_lower'].round(2)
                        df['yhat_upper'] = df['yhat_upper'].round(2)

                        st.write("Demand prediction:")
                        st.dataframe(df)
                    else:
                        st.write("No data available for the given forecast.")
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")

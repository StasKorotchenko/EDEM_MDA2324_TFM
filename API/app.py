import streamlit as st
import requests

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

# Título de la aplicación
st.title("Customer Prediction App")
st.write("Enter the data below to get a prediction about the customer. All fields are required.")

# Interfaz de usuario para la entrada de datos
total_spent = st.number_input('Total Spent', min_value=0.0, format="%f", value=float('nan'))
purchase_frequency = st.number_input('Purchase Frequency', min_value=0.0, format="%f", value=float('nan'))
average_order_value = st.number_input('Average Order Value', min_value=0.0, format="%f", value=float('nan'))
num_reviews = st.number_input('Number of Reviews', min_value=0, format="%d", value=0)
avg_review_score = st.number_input('Average Review Score', min_value=1.0, max_value=5.0, format="%f", value=float('nan'))

if st.button("Predict"):
    with st.spinner("Processing data... Please wait."):
        response = requests.post("http://localhost:8000/predict", json={
            "total_spent": total_spent,
            "purchase_frequency": purchase_frequency,
            "average_order_value": average_order_value,
            "num_reviews": num_reviews,
            "avg_review_score": avg_review_score
        })
        
        if response.status_code == 200:
            prediction = response.json().get("prediction", ["Unknown"])[0]
            st.success(f"Client cluster prediction: {prediction}")
        else:
            st.error(f"Error: {response.json().get('detail', 'Unknown error')}")


import streamlit as st
import requests
import pandas as pd
import plotly.graph_objs as go

st.set_page_config(page_title="Customer Prediction App", page_icon=":chart_with_upwards_trend:")

# Estilo CSS personalizado
st.markdown(
    """
    <style>
    .stButton>button {
        background-color: #4CAF50;
        color: white;
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
    purchase_frequency = st.number_input('Purchase frequency', min_value=0.0, format="%f")
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
                response = requests.post("https://fastapi-app-v47hoksqvq-no.a.run.app//predict", json=data)
                
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
                response = requests.post("https://fastapi-app-v47hoksqvq-no.a.run.app/demand_predict", json=data)
                
                if response.status_code == 200:
                    forecast = response.json().get("forecast", [])
                    
                    # Преобразуем данные в DataFrame для отображения
                    if forecast:
                        df = pd.DataFrame(forecast)
                        df['ds'] = pd.to_datetime(df['ds']).dt.date  # Только дата без времени
                        df = df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
                        
                        # Округляем значения до двух знаков после запятой
                        df['yhat'] = df['yhat'].round(2)
                        df['yhat_lower'] = df['yhat_lower'].round(2)
                        df['yhat_upper'] = df['yhat_upper'].round(2)

                        df.rename(columns={
                            'ds': 'Date',
                            'yhat': 'Predicted Demand',
                            'yhat_lower': 'Lower Bound',
                            'yhat_upper': 'Upper Bound'
                        }, inplace=True)

                        # Форматирование значений в таблице
                        def format_values(x):
                            return f"{x:.2f}"

                        # Построение графика с использованием Plotly
                        fig = go.Figure()

                        # Линия прогноза
                        fig.add_trace(go.Scatter(
                            x=df['Date'], 
                            y=df['Predicted Demand'], 
                            mode='lines', 
                            name='Forecast',
                            line=dict(color='blue')
                        ))

                        # Заполнение области между yhat_lower и yhat_upper
                        fig.add_trace(go.Scatter(
                            x=df['Date'].tolist() + df['Date'].tolist()[::-1],
                            y=df['Upper Bound'].tolist() + df['Lower Bound'].tolist()[::-1],
                            fill='toself',
                            fillcolor='rgba(173, 216, 230, 0.2)',  # Светло-голубой цвет
                            line=dict(color='rgba(255,255,255,0)'),
                            hoverinfo="skip",
                            showlegend=False
                        ))

                        # Настройка осей и оформления
                        fig.update_layout(
                            title='Demand Forecast',
                            yaxis_title='Demand',
                            xaxis_title='',  # Убираем подпись оси X
                            hovermode='x',
                            template='plotly_white'
                        )

                        # Отображение графика
                        st.plotly_chart(fig)

                        # Подсветка экстремальных значений в таблице
                        def highlight_extremes(row):
                            styles = []
                            for value in row:
                                if value == df['Predicted Demand'].max():
                                    styles.append('background-color: #FFCCCB')  # Светло-красный
                                elif value == df['Predicted Demand'].min():
                                    styles.append('background-color: #E0FFFF')  # Светло-синий
                                else:
                                    styles.append('')
                            return styles

                        # Отображение таблицы с форматированием значений
                        st.write("Demand prediction:")
                        st.dataframe(df.style.apply(highlight_extremes, axis=1, subset=['Predicted Demand', 'Lower Bound', 'Upper Bound'])
                                          .format(formatter={'Predicted Demand': format_values,
                                                              'Lower Bound': format_values,
                                                              'Upper Bound': format_values}))
                    else:
                        st.write("No data available for the given forecast.")
                else:
                    st.error(f"Error: {response.json().get('detail', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                st.error(f"Request failed: {str(e)}")

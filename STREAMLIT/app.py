import streamlit as st
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import joblib
import os

# Ruta a los archivos CSV
DATA_PATH = '/Users/pablomartinomdedeu/Desktop/EDEM_MDA2324_TFM/DATA/clean/'

# Cargar los datos desde archivos CSV
@st.cache_data
def load_data():
    customers = pd.read_csv(os.path.join(DATA_PATH, 'customers.csv'))
    geolocalization = pd.read_csv(os.path.join(DATA_PATH, 'geolocalizaciones_cleaned.csv'))
    order_items = pd.read_csv(os.path.join(DATA_PATH, 'order_items_cleaned.csv'))
    orders = pd.read_csv(os.path.join(DATA_PATH, 'orders_cleaned.csv'))
    order_payments = pd.read_csv(os.path.join(DATA_PATH, 'order_payments.csv'))
    order_reviews = pd.read_csv(os.path.join(DATA_PATH, 'reviews_cleaned.csv'))
    products = pd.read_csv(os.path.join(DATA_PATH, 'products_cleaned.csv'))
    sellers = pd.read_csv(os.path.join(DATA_PATH, 'sellers.csv'))
    return customers, geolocalization, order_items, orders, order_payments, order_reviews, products, sellers

customers, geolocalization, order_items, orders, order_payments, order_reviews, products, sellers = load_data()

# Definir la interfaz de usuario de Streamlit
st.title('Predicción de la Demanda')
st.write('Seleccione un proveedor para predecir la demanda.')

# Lista de vendedores para seleccionar
seller_list = sellers['seller_id'].unique()
selected_seller = st.selectbox('Seleccione un vendedor', seller_list)

# Filtrar datos por vendedor seleccionado
filtered_data = order_items[order_items['seller_id'] == selected_seller]

# Juntar datos necesarios para la predicción
merged_data_products = filtered_data.merge(products, on='product_id')
st.write("Columnas después de fusionar con productos:")
st.write(merged_data_products.columns.tolist())

merged_data = merged_data_products.merge(orders, on='order_id')
st.write("Columnas después de fusionar con orders:")
st.write(merged_data.columns.tolist())

# Verificar si las columnas existen
required_columns = ['price', 'freight_value', 'weight_g', 'length_cm', 'height_cm', 'width_cm']
missing_columns = [col for col in required_columns if col not in merged_data.columns]

if missing_columns:
    st.error(f"Las siguientes columnas están faltando en el DataFrame: {', '.join(missing_columns)}")
else:
    # Renombrar columnas para que coincidan con los nombres esperados
    merged_data.rename(columns={
        'weight_g': 'product_weight_g',
        'length_cm': 'product_length_cm',
        'height_cm': 'product_height_cm',
        'width_cm': 'product_width_cm'
    }, inplace=True)
    
    X = merged_data[['price', 'freight_value', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']]
    y = merged_data['price']  # Ajusta si deseas predecir una columna diferente

    # Dividir datos en entrenamiento y prueba
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Entrenar el modelo
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Hacer predicciones
    y_pred = model.predict(X_test)

    # Calcular el error de predicción
    mse = mean_squared_error(y_test, y_pred)

    # Mostrar el error de predicción
    st.write(f'Error Cuadrático Medio de Predicción: {mse}')

    # Guardar el modelo entrenado
    joblib.dump(model, 'demand_prediction_model.pkl')

    # Función para predecir demanda
    def predict_demand(model, input_data):
        prediction = model.predict([input_data])
        return prediction

    # Entrada de datos para la predicción de la demanda
    st.write('Ingrese las características del producto para predecir la demanda:')
    price = st.number_input('Precio')
    freight_value = st.number_input('Valor del Flete')
    product_weight_g = st.number_input('Peso del Producto (g)')
    product_length_cm = st.number_input('Longitud del Producto (cm)')
    product_height_cm = st.number_input('Altura del Producto (cm)')
    product_width_cm = st.number_input('Ancho del Producto (cm)')

    input_data = [price, freight_value, product_weight_g, product_length_cm, product_height_cm, product_width_cm]

    # Predecir la demanda
    if st.button('Predecir Demanda'):
        model = joblib.load('demand_prediction_model.pkl')
        prediction = predict_demand(model, input_data)
        st.write(f'La demanda predicha es: {prediction[0]}')








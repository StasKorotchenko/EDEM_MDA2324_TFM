# EDEM_MDA2324_TFM

### **ECENARIO & OBJETIVO**

El conjunto de datos de e-ComSys ofrece una visión general del mundo e-commerce. Abarca 100.000 pedidos en varios mercados, desde los años 2021 a 2023. Incluye tablas con información sobre pedidos, clientes, productos, pagos, reseñas... Para cada pedido se tiene el recorrido desde la selección del producto hasta la entrega.

Los objetivos del proyecto giran en torno a la evaluación del desempeño, el análisis de los clientes y el modelado predictivo para la predicción de la demanda. Además, un objetivo clave del proyecto es diseñar y desarrollar una arquitectura end to end. Esta arquitectura abarca componentes de ingesta, procesamiento, almacenamiento, análisis y visualización de datos, lo que proporciona un marco escalable y eficiente para extraer información del conjunto de datos.

### **Tecnología Cloud**

Diseño de la Arquitectura End to End. Proporcionar un diagrama de arquitectura detallado y una explicación paso a paso de la solución propuesta. Especificar las tecnologías, herramientas y bibliotecas a utilizar

**Ingesta de Datos**

Describir cómo importarán el conjunto de datos de e-ComSys, considerando métodos de ingesta tanto en tiempo real como en batch (por ejemplo, Azure Data Factory, scripts en Python,     etc.).


**Almacenamiento de Datos**

Identificar dónde y cómo se almacenarán los datos, considerando la integración de flujos de datos en tiempo real (por ejemplo, Azure Blob Storage, bases de datos SQL, datalakes).

**Procesamiento de Datos**

Explicar el proceso de limpieza, transformación y preparación de los datos para el análisis tanto en contextos de procesamiento en tiempo real como en batch (por ejemplo, Apache Spark, pandas, SQL).

**Análisis de Datos**

Describir cómo se realizará el análisis de datos y qué tipos de análisis se planea realizar (por ejemplo, clustering o modelado predictivo) (por ejemplo, scikit-learn, Power BI).

**Visualización y Generación de Informes**

Detallar cómo se visualizarán los resultados del análisis y generar informes, considerando actualizaciones de datos en tiempo real (por ejemplo, Power BI, Tableau, matplotlib).

**Análisis de Ventas y Productos**

Ingresos Totales y por País
Evolución de Ventas: Análisis horario, diario, semanal, mensual para identificar períodos pico.
Productos Más Vendidos
Categorías de Productos: Evaluar las calificaciones más altas y más bajas.
Predicción de Ventas Futuras: Basado en datos históricos.

**Análisis Geográfico**

Variación de Ventas y Comportamiento del Cliente: Por regiones geográficas.
Volumen de Ventas: Regiones con mayores y menores ventas.

**Análisis de Usuarios**

Número de Usuarios: Activos, inactivos, recurrentes, recuperados.
Clientes Principales: Basado en el número de pedidos y el total gastado.
Frecuencia de Compra: Por cliente.
Métricas por Usuario: ATV (Average Transaction Value), UPT (Units per Transaction), AVG (Importe Medio), AVG (Unidades).

**Segmentación RFM**

Asignar puntuaciones de "Recency, Frequency and Monetary value" y segmentar a los clientes en función de estas puntuaciones para identificar segmentos de clientes.

**Predicción de Abandono ("Churn Analysis")**

Identificar patrones que predicen si un cliente abandonará el servicio.

**Automatización y Programación**

Discutir cómo automatizar las tareas de ingesta, procesamiento y generación de informes de datos, incluyendo el manejo de datos en tiempo real. Considerar usar servicios como Azure Data Factory u otras herramientas de programación para procesos en batch, y Azure Stream Analytics para procesos en tiempo real.

**Escalabilidad y Rendimiento**

Abordar cómo garantizar que la arquitectura pueda escalar con el aumento del volumen y la complejidad de los datos. Considerar estrategias de optimización del rendimiento, especialmente para el manejo de datos en tiempo real.

**Seguridad y Gobernanza de Datos**

Describir cómo garantizarán la seguridad y privacidad de los datos. Mencionar cualquier práctica de gobernanza de datos a implementar.

**Análisis de Rendimiento de Entregas**

Tiempo Promedio de Entrega: Evaluar si varía entre las diferentes regiones.
Factores que Afectan los Tiempos de Entrega: Identificar estos factores.
Impacto del Tiempo de Entrega en las Calificaciones de las Reseñas: Evaluar cómo afecta.

**Análisis de Pagos**

Métodos de Pago Comunes: Identificar los más utilizados.
Valor de los Pedidos: Comparar entre diferentes métodos de pago.
Impacto de las Cuotas de Pago en el Total de la Compra: Analizar cómo afectan.

**Análisis de Reseñas de Clientes**

Relación entre Comentarios y Satisfacción del Cliente: Identificar patrones para incentivar reseñas.
Satisfacción Media: Comparar entre categorías y países.
Satisfacción según la Categorización RFM: Evaluar satisfacción de diferentes tipos de clientes.
Características Clave de los Clientes con Mayor Tiempo de Vida: Identificar estas características.
Tendencias Estacionales en las Ventas de Productos: Analizar tendencias.
Identificación de Regiones con Mayor Potencial de Crecimiento: Evaluar regiones.



Este README proporciona una visión general de los objetivos, la arquitectura y los métodos que utilizaremos para llevar a cabo nuestro proyecto de análisis de datos de e-commerce.

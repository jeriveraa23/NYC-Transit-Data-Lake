## Capa Plata (Silver Layer) - Especificaciones de Transformación

La capa Plata realiza la limpieza técnica y estandarización de los datos crudos provenientes de la capa Bronce. El objetivo es proporcionar un conjunto de datos atómico, limpio y tipado para análisis avanzado y entrenamiento de modelos.

### Diccionario de Datos y Reglas de Transformación

| Campo | Tipo Destino | Descripción | Regla de Validación / Análisis |
| :--- | :--- | :--- | :--- |
| **VendorID** | `Integer` | ID del proveedor de tecnología. | Validar valores conocidos (1 o 2). |
| **tpep_pickup_datetime** | `Timestamp` | Fecha y hora de inicio del viaje. | Convertir de Unix Milliseconds a Timestamp. |
| **tpep_dropoff_datetime** | `Timestamp` | Fecha y hora de fin del viaje. | Convertir y validar que sea mayor al Pickup. |
| **passenger_count** | `Integer` | Número de pasajeros. | Filtrar o imputar si el valor es 0. |
| **trip_distance** | `Double` | Distancia del viaje en millas. | Eliminar registros con distancia <= 0. |
| **RatecodeID** | `Integer` | Código de tarifa final. | Validar IDs entre 1 y 6. |
| **store_and_fwd_flag** | `Boolean` | Indica si el registro se guardó en memoria. | Convertir 'Y'/'N' a True/False. |
| **PULocationID** | `Integer` | ID de zona de recogida (TLC). | Validar contra catálogo oficial de zonas. |
| **DOLocationID** | `Integer` | ID de zona de destino (TLC). | Validar contra catálogo oficial de zonas. |
| **payment_type** | `Integer` | Método de pago (1=Tarjeta, 2=Efectivo). | Crucial para análisis de propinas (Tip). |
| **fare_amount** | `Decimal(10,2)` | Tarifa calculada por el taxímetro. | Debe ser un valor positivo (> 0). |
| **extra** | `Decimal(10,2)` | Recargos (pico y noche). | Manejar como 0.0 si viene nulo. |
| **mta_tax** | `Decimal(10,2)` | Impuesto de la MTA ($0.50). | Validar consistencia con el estándar. |
| **tip_amount** | `Decimal(10,2)` | Monto de la propina. | Solo garantizado en pagos con tarjeta. |
| **tolls_amount** | `Decimal(10,2)` | Peajes pagados. | Sumar al costo total del viaje. |
| **improvement_surcharge**| `Decimal(10,2)` | Recargo por mejora de infraestructura. | Valor fijo estandarizado. |
| **total_amount** | `Decimal(10,2)` | Valor total cobrado al pasajero. | **Auditoría:** Suma de todos los cargos. |
| **congestion_surcharge**| `Decimal(10,2)` | Recargo por zona de congestión. | Solo aplica en zonas específicas de Manhattan. |
| **airport_fee** | `Decimal(10,2)` | Tarifa de acceso a aeropuertos. | Solo aplica en JFK o LaGuardia. |

### Lógica de Limpieza (Data Quality)
Para asegurar la integridad en la Capa Plata, se aplican los siguientes filtros en PySpark:
1. **Filtro de Tiempo:** Se eliminan viajes con duración negativa o igual a cero segundos.
2. **Filtro de Negocio:** Se descartan registros con `total_amount` menor o igual a cero.
3. **Conversión de Unidades:** Los Timestamps se normalizan dividiendo por 1000 los valores de la fuente cruda.


## Capa Oro (Gold Layer) - Métricas de Negocio

La capa Oro transforma los datos limpios de la capa Plata en información estratégica. En esta etapa, los datos se agrupan por día y zona para generar indicadores clave de rendimiento (KPIs) que faciliten la toma de decisiones.

### Tabla de Agregación: `taxi_performance_daily`

**Granularidad:** Diaria por Zona de Recogida (`PULocationID`).

| Campo | Tipo | Origen / Lógica | Descripción de Negocio |
| :--- | :--- | :--- | :--- |
| **pickup_date** | `Date` | `to_date(tpep_pickup_datetime)` | Fecha del servicio (YYYY-MM-DD). |
| **PULocationID** | `Integer` | `PULocationID` | ID de la zona donde inició el viaje. |
| **total_trips** | `Long` | `count(VendorID)` | Volumen total de servicios prestados. |
| **total_revenue** | `Decimal` | `sum(total_amount)` | Ingresos brutos totales generados. |
| **avg_revenue** | `Decimal` | `avg(total_amount)` | Ticket promedio por viaje en esa zona. |
| **total_tips_profit** | `Decimal` | `sum(tip_amount)` | Total de propinas capturadas por sistema. |
| **tip_efficiency_pct**| `Decimal` | `(sum(tip_amount) / sum(fare_amount)) * 100` | % de propina respecto a la tarifa base. |
| **avg_distance** | `Decimal` | `avg(trip_distance)` | Distancia promedio recorrida (Millas). |
| **total_passengers** | `Long` | `sum(passenger_count)` | Total de personas transportadas. |
| **avg_duration_min** | `Decimal` | `avg(trip_duration_minutes)` | Tiempo promedio de viaje en minutos. |

### Reglas de Negocio (Gold Logic)
1. **Filtro de Calidad:** Solo se procesan registros donde `is_total_correct = True` para garantizar que las métricas financieras sean exactas.
2. **Normalización Temporal:** Se eliminan las horas/minutos para agrupar toda la actividad en bloques de 24 horas.
3. **Optimización de Almacenamiento:** Los resultados se redondean a 2 decimales para mejorar la legibilidad en tableros de visualización.
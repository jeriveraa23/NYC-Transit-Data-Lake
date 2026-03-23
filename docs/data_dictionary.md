VendorID: Código que identifica al proveedor tecnológico que instaló el taxímetro (1 = Creative Mobile Technologies; 2 = VeriFone).

tpep_pickup_datetime: Fecha y hora exacta en la que se subió el pasajero y se activó el taxímetro.

tpep_dropoff_datetime: Fecha y hora exacta en la que el pasajero bajó del taxi y se detuvo el cobro.

passenger_count: Número de pasajeros en el vehículo (este es un dato que el conductor ingresa manualmente).

trip_distance: La distancia del viaje en millas reportada por el taxímetro.

RatecodeID: El código de la tarifa final aplicada (1 = Estándar, 2 = JFK, 3 = Newark, 4 = Nassau/Westchester, 5 = Tarifa negociada, 6 = Viaje grupal).

store_and_fwd_flag: Indica si el registro se guardó en la memoria del taxi antes de enviarse al servidor porque no había conexión (Y = sí, N = no).

PULocationID: ID de la zona de TLC (Taxi and Limousine Commission) donde se recogió al pasajero.

DOLocationID: ID de la zona de TLC donde terminó el viaje.

payment_type: Método de pago del cliente (1 = Tarjeta de crédito, 2 = Efectivo, 3 = Sin cargo, 4 = Disputa, 5 = Desconocido, 6 = Viaje cancelado).

fare_amount: El costo del viaje basado puramente en el tiempo y la distancia calculados por el taxímetro.

extra: Recargos misceláneos (como los de $0.50 y $1.00 por hora pico o tarifas nocturnas).

mta_tax: Impuesto de $0.50 de la MTA que se activa automáticamente según la tarifa en uso.

tip_amount: Monto de la propina (este campo se llena automáticamente para pagos con tarjeta; para efectivo siempre aparecerá en 0).

tolls_amount: Suma total de los peajes pagados durante el trayecto.

improvement_surcharge: Recargo de $0.30 por "mejora de infraestructura" que se empezó a cobrar en 2015.

total_amount: La suma total cobrada al pasajero (incluye todos los impuestos, recargos y la propina).

congestion_surcharge: Recargo por congestión (actualmente $2.50) para viajes que circulan por el sur de Manhattan.

airport_fee: Recargo fijo por recoger pasajeros en los aeropuertos LaGuardia o JFK ($1.25).
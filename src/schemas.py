import pyspark.sql.types as t

gold_clients_address_schema = t.StructType([
    t.StructField('client_id', t.StringType(), True), 
    t.StructField('first_name', t.StringType(), True), 
    t.StructField('last_name', t.StringType(), True), 
    t.StructField('email', t.StringType(), True), 
    t.StructField('phone_number', t.StringType(), True), 
    t.StructField('date_of_birth', t.DateType(), True), 
    t.StructField('gender', t.StringType(), True), 
    t.StructField('occupation', t.StringType(), True), 
    t.StructField('created_at', t.DateType(), True), 
    t.StructField('updated_at', t.DateType(), True), 
    t.StructField('status', t.StringType(), True), 
    t.StructField('address_id', t.StringType(), True), 
    t.StructField('neighborhood', t.StringType(), True), 
    t.StructField('coordinates', t.ArrayType(t.DoubleType(), True), True), 
    t.StructField('road', t.StringType(), True), 
    t.StructField('house_number', t.StringType(), True), 
    t.StructField('suburb', t.StringType(), True), 
    t.StructField('city_district', t.StringType(), True), 
    t.StructField('state', t.StringType(), True), 
    t.StructField('postcode', t.StringType(), True), 
    t.StructField('country', t.StringType(), True), 
    t.StructField('lat', t.FloatType(), True), 
    t.StructField('lon', t.FloatType(), True)
])

gold_products_schema = t.StructType([
    t.StructField('product_id', t.StringType(), True), 
    t.StructField('name', t.StringType(), True), 
    t.StructField('category', t.StringType(), True), 
    t.StructField('url', t.StringType(), True), 
    t.StructField('price', t.FloatType(), True), 
    t.StructField('currency', t.StringType(), True), 
    t.StructField(
        'product_components', t.ArrayType(
            t.StructType([
                t.StructField('package_id', t.StringType(), True),
                t.StructField('subpackage_id', t.IntegerType(), True),
                t.StructField('package_quantity', t.IntegerType(), True)
            ]),
        False), False)
])

gold_packages_schema =t.StructType([
    t.StructField('package_id', t.StringType(), True),
    t.StructField('subpackage_id', t.IntegerType(), True),
    t.StructField('name', t.StringType(), True),
    t.StructField('width', t.IntegerType(), True),
    t.StructField('height', t.IntegerType(), True),
    t.StructField('length', t.IntegerType(), True),
    t.StructField('weight', t.FloatType(), True), 
    t.StructField('volume', t.IntegerType(), True),
    t.StructField('stock_quantity', t.IntegerType(), False)
])

orders_delta_schema = schema = t.StructType([
    t.StructField("event_id", t.StringType(), True),
    t.StructField("event_type", t.StringType(), True),
    t.StructField("event_timestamp", t.StringType(), True),
    t.StructField("order_id", t.StringType(), True),
    t.StructField("order_details", t.StructType([
        t.StructField("customer_id", t.StringType(), True),
        t.StructField("order_timestamp", t.StringType(), True),
        t.StructField("order_date", t.StringType(), True),
        t.StructField("items", t.ArrayType(
            t.StructType([
                t.StructField("product_id", t.StringType(), True),
                t.StructField("product_name", t.StringType(), True),
                t.StructField("price", t.DoubleType(), True),
                t.StructField("quantity", t.IntegerType(), True),
                t.StructField("packages", t.ArrayType(
                    t.StructType([
                        t.StructField("package_id", t.StringType(), True),
                        t.StructField("subpackage_id", t.IntegerType(), True),
                        t.StructField("quantity", t.IntegerType(), True),
                        t.StructField("weight", t.DoubleType(), True),
                        t.StructField("volume", t.DoubleType(), True)
                    ])
                ), True)
            ])
        ), True),
        t.StructField("total_amount", t.DoubleType(), True),
        t.StructField("total_volume", t.DoubleType(), True),
        t.StructField("total_weight", t.DoubleType(), True),
        t.StructField("status", t.StringType(), True),
        t.StructField("destination_address", t.StructType([
            t.StructField("address_id", t.StringType(), True),
            t.StructField("neighborhood", t.StringType(), True),
            t.StructField("coordinates", t.ArrayType(t.DoubleType()), True),
            t.StructField("road", t.StringType(), True),
            t.StructField("house_number", t.StringType(), True),
            t.StructField("suburb", t.StringType(), True),
            t.StructField("city_district", t.StringType(), True),
            t.StructField("state", t.StringType(), True),
            t.StructField("postcode", t.StringType(), True),
            t.StructField("country", t.StringType(), True),
            t.StructField("lat", t.DoubleType(), True),
            t.StructField("lon", t.DoubleType(), True)
        ]), True),
        t.StructField("payment_details", t.StructType([
            t.StructField("payment_method", t.StringType(), True),
            t.StructField("payment_status", t.StringType(), True),
            t.StructField("transaction_id", t.StringType(), True)
        ]), True)
    ]), True)
])
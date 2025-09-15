# Databricks notebook source
# MAGIC %md
# MAGIC Bronze Layer

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("e-commerce_app").getOrCreate()

customer_df = spark.read.csv("/Volumes/workspace/ecommerce_sales/bronze_layer/olist_customers_dataset.csv", header="true", inferSchema="true")
order_df = spark.read.csv("/Volumes/workspace/ecommerce_sales/bronze_layer/olist_orders_dataset.csv", header="true", inferSchema="true")
order_items_df = spark.read.csv("/Volumes/workspace/ecommerce_sales/bronze_layer/olist_order_items_dataset.csv", header="true", inferSchema="true")
products_df = spark.read.csv("/Volumes/workspace/ecommerce_sales/bronze_layer/olist_products_dataset.csv", header="true", inferSchema="true")
products_name_df = spark.read.csv('/Volumes/workspace/ecommerce_sales/bronze_layer/product_category_name_translation.csv', header='true', inferSchema='true')
cities_df = spark.read.csv('/Volumes/workspace/ecommerce_sales/bronze_layer/BRAZIL_CITIES.csv', header='true', inferSchema='true')

customer_df.show(5)
order_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Silver layer

# COMMAND ----------

from pyspark.sql.functions import date_format, col

cities_df = cities_df.withColumnRenamed('STATE', 'customer_state')

customer_df = (
    customer_df
    .na.drop()
    .dropDuplicates(['customer_id'])
    .join(cities_df, 'customer_state', 'left')
)

orders_df = (
    order_df
    .filter(order_df.order_status == 'delivered')
    .withColumn('purchased_date', date_format(col('order_purchase_timestamp'), 'yyyy-MM-dd'))
    .withColumn('delivery_date', date_format(col('order_delivered_customer_date'), 'yyyy-MM-dd'))
    .drop(
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    )
)

order_items_df = (
    order_items_df
    .drop('seller_id', 'shipping_limit_date')
    .filter(order_items_df.price > 0)
)

products_df = (
    products_df
    .na.drop()
    .drop(
        'product_name_lenght',
        'product_description_lenght',
        'product_photos_qty',
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
    .join(products_name_df, 'product_category_name', 'left')
    .drop('product_category_name')
    .withColumnRenamed('product_category_name_english', 'product_category_name')
)

customer_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce_sales/silver_layer/customer_clean')
orders_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce_sales/silver_layer/orders_clean')
order_items_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce_sales/silver_layer/order_items_clean')
products_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce_sales/silver_layer/products_clean')

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Layer

# COMMAND ----------

customer_clean_df = spark.read.format('delta').load('/Volumes/workspace/ecommerce_sales/silver_layer/customer_clean')
orders_clean_df = spark.read.format('delta').load('/Volumes/workspace/ecommerce_sales/silver_layer/orders_clean')
order_items_clean_df = spark.read.format('delta').load('/Volumes/workspace/ecommerce_sales/silver_layer/order_items_clean')
products_clean_df = spark.read.format('delta').load('/Volumes/workspace/ecommerce_sales/silver_layer/products_clean')
# display(customer_clean_df)
# display(orders_clean_df)
# display(order_items_clean_df)
# display(products_clean_df)

# COMMAND ----------

merged_df = order_items_clean_df.join(orders_clean_df, "order_id", "inner") \
    .join(products_clean_df, "product_id", "inner") \
    .join(customer_clean_df, "customer_id", "inner")

merged_df.write.format('delta').mode('overwrite').save('/Volumes/workspace/ecommerce_sales/gold_layer/sales_fact')

merged_df.write.mode('overwrite').saveAsTable('workspace.ecommerce_sales.sales_fact')

display(merged_df)

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, countDistinct

#Revenue by product category
revenue_by_category = (
    merged_df.groupBy('product_category_name') \
    .agg(_sum('price').alias('total_revenue')) \
    .orderBy('total_revenue', ascending=False)
)

revenue_by_category.write.mode('overwrite').saveAsTable('workspace.ecommerce_sales.revenue_by_category')

# Orders by city
orders_by_city = merged_df.groupBy('customer_state') \
        .agg(countDistinct('order_id').alias('num_orders')) \
        .orderBy('num_orders', ascending = False)

display(revenue_by_category)
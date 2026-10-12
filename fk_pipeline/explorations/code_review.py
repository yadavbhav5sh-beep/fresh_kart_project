# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select * from fk_1374.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.bronze.stores; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.bronze.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.silver.stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.silver.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fk_1374.bronze.orders;

# COMMAND ----------


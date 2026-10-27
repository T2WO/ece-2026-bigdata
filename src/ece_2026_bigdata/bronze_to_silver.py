from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config("spark.sql.catalog.local.type", "hadoop")
    .config(
        "spark.sql.catalog.local.warehouse",
        "s3a://user-s-chupin-ece/warehouse/"
    )
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.endpoint",
    "https://s3.seaweedfs.adm.adaltas.cloud"
)
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider"
)
hadoop_conf.set("fs.s3a.path.style.access", "true")

BUCKET = "user-s-chupin-ece"

users = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .csv(f"s3a://{BUCKET}/bronze/users.csv")
)

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"s3a://{BUCKET}/bronze/orders.csv")
)
# Nettoyage des utilisateurs
users_silver = (
    users
    .withColumn("birthdate", to_date("birthdate", "yyyy-MM-dd"))
    .dropDuplicates(["uuid"])
    .dropna(subset=["uuid", "username", "mail", "birthdate"])
)

# Nettoyage des commandes
orders_silver = (
    orders
    .dropDuplicates(["uuid"])
    .dropna(subset=["uuid", "user_uuid", "date", "quantity", "product"])
    .filter("quantity > 0")
)
# Enregistrement des utilisateurs en Parquet
users_silver.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/users/"
)

# Enregistrement des commandes en Parquet
orders_silver.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/orders/"
)
# Création de la base Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")

# Création des tables Iceberg
users_silver.writeTo("local.silver.users").using("iceberg").createOrReplace()

orders_silver.writeTo("local.silver.orders").using("iceberg").createOrReplace()

# Vérification des tables
spark.sql("SHOW TABLES IN local.silver").show()

# Arrêt de Spark
spark.stop()
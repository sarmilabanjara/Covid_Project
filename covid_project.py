from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, expr, year, from_unixtime, dayofweek, when


def countryCodeQs(spark,file_path):
    countryCodeQs = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finalcc =countryCodeQs.dropDuplicates()
    finalcc.createOrReplaceTempView("countryCodeQs")
    df= spark.sql("select country, `Alpha-2 code` as alpha2code, `Alpha-3 code` as alpha3code,`Numeric code` as numericcode ,Latitude,Longitude from countryCodeQs")
    df.show(truncate=False)
    df.write.mode("overwrite").saveAsTable("covid_project.countryCodeQs")

def us_daily(spark,file_path):
    us_daily= spark.read.options(header='True', inferSchema=True,delimiter=',').csv(file_path)
    finalud =us_daily.dropDuplicates()
    finalud.createOrReplaceTempView("us_daily")
    finalud.show(truncate=False)
    finalud.write.mode("overwrite").saveAsTable("covid_project.us_daily")

def states_abv(spark,file_path):
    states_abv= spark.read.options(header='True', inferSchema=True,delimiter=',').csv(file_path)
    final_sabv =states_abv.dropDuplicates()
    final_sabv.createOrReplaceTempView("states_abv")
    final_sabv.show(truncate=False)
    final_sabv.write.mode("overwrite").saveAsTable("covid_project.states_abv")

def us_states(spark,file_path):
    us_states = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finalus_states =us_states.dropDuplicates()
    finalus_states.createOrReplaceTempView("us_states")
    finalus_states.show(truncate=False)
    finalus_states.write.mode("overwrite").saveAsTable("covid_project.us_states")

def usa_hospitalbeds(spark,file_path):
    usa_hospitalbeds = spark.read.format("json").options(header='True', inferSchema='True', delimiter=',').json(file_path)
    finalusa_hospitalbeds = usa_hospitalbeds.withColumn("fips",col("fips").cast("int"))
    df =finalusa_hospitalbeds.dropDuplicates()
    df.createOrReplaceTempView("usa_hospitalbeds")
    df.show(truncate=False)
    df.write.mode("overwrite").saveAsTable("covid_project.usa_hospitalbeds")

def county_population(spark,file_path):
    cp = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    cp = cp.withColumnRenamed("population estimate 2018", "populationEstimate2018")
    finalcp =cp.dropDuplicates()
    finalcp.createOrReplaceTempView("county_population")
    finalcp.show(truncate=False)
    finalcp.write.mode("overwrite").saveAsTable("covid_project.county_population")

def data(spark,file_path):
    data = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finaldata =data.dropDuplicates()
    finaldata.createOrReplaceTempView("data")
    finaldata.show(truncate=False)
    finaldata.write.mode("overwrite").saveAsTable("covid_project.data")

'''' #part is double
#part and countrycode is same
def part(spark,file_path):
    part = spark.read.format("json").options(header='True', inferSchema='True', delimiter=',').json(file_path)
    finalpart=part.dropDuplicates()
    finalpart.createOrReplaceTempView("part")
    finalpart.show(truncate=False)
    finalpart.write.mode("overwrite").saveAsTable("covid_project.part")
 '''

def us(spark,file_path):
    us = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finalus = us.dropDuplicates()
    finalus.createOrReplaceTempView("us")
    finalus.show(truncate=False)
    finalus.write.mode("overwrite").saveAsTable("covid_project.us")

def states_daily(spark,file_path):
    sd = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finalsd = sd.dropDuplicates()
    finalsd.createOrReplaceTempView("states_daily")
    finalsd.show(truncate=False)
    finalsd.write.mode("overwrite").saveAsTable("covid_project.states_daily")

def us_county(spark,file_path):
    uc = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(file_path)
    finaluc = uc.dropDuplicates()
    finaluc.createOrReplaceTempView("us_county")
    finaluc.show(truncate=False)
    finaluc.write.mode("overwrite").saveAsTable("covid_project.us_county")

def create_dimensional_model(spark):

    data_df = spark.table("covid_project.data")
    hospital_df = spark.table("covid_project.usa_hospitalbeds")
    states_abv_df= spark.table("covid_project.states_abv")
    us_county_df = spark.table("covid_project.us_county")
    us_daily_df = spark.table("covid_project.us_daily")
    states_daily_df = spark.table("covid_project.states_daily")

    joined_df =  data_df.join(hospital_df, "fips", "inner")
    dimhospital_df = joined_df.select(
        data_df["fips"],
        hospital_df["state_name"].alias("state"),
        hospital_df["latitude"].alias("hos_lat"),
        hospital_df["longtitude"].alias("hos_long"),
        hospital_df["hq_address"],
        hospital_df["hospital_type"],
        hospital_df["hospital_name"],
        hospital_df["hq_city"],
        hospital_df["hq_state"]
    )

    joined_df = data_df.join(us_county_df, "fips", "inner").join(states_abv_df, us_county_df["state"] == states_abv_df["state"], "inner")
    dim_region_df = joined_df.select(
    data_df["fips"],
    states_abv_df["state"],
    data_df["country_region"].alias("region"),
    data_df["latitude"].alias("lat"),
    data_df["longitude"].alias("lang"),
    us_county_df["county"],
    states_abv_df["abbreviation"].alias("states_abb")
)
    joined_df = us_daily_df.join(states_daily_df, "date", "inner")
    joined_df1 = joined_df.join(data_df, "fips", "inner")
    fact_covid_df = joined_df1.select(
    data_df["fips"],
    states_daily_df["state"].alias("states"),
    data_df["country_region"].alias("region"),
    data_df["confirmed"],
    us_daily_df["death"],
    states_daily_df["recovered"],
    data_df["active"],
    us_daily_df["positive"],
    us_daily_df["negative"],
    us_daily_df["hospitalizedCurrently"],
    us_daily_df["hospitalized"],
    states_daily_df["hospitalizedDischarged"]
)
    states_daily_df = states_daily_df.withColumn("date_timestamp", from_unixtime(states_daily_df["date"]))
    dim_date_df = states_daily_df.select(
    "fips",
    "date_timestamp",
    month("date_timestamp").alias("month"),
    year("date_timestamp").alias("year"),
    #expr("CASE WHEN dayofweek(date_timestamp) IN (1,7) THEN 1 ELSE 0 END").alias("is_weekend")
    when(dayofweek("date_timestamp").isin([1, 7]), "weekend").otherwise("weekday").alias("is_weekend")
)

    dimhospital_df.write.mode("overwrite").saveAsTable("dimensional_model.dimhospital")
    dimhospital_df.show(truncate=False)
    dim_region_df.write.mode("overwrite").saveAsTable("dimensional_model.dim_region")
    dim_region_df.show(truncate=False)
    fact_covid_df.write.mode("overwrite").saveAsTable("dimensional_model.fact_covid")
    fact_covid_df.show(truncate=False)
    dim_date_df.write.mode("overwrite").saveAsTable("dimensional_model.dim_date")
    dim_date_df.show(truncate=False)

if __name__ == '__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").enableHiveSupport().getOrCreate()
    countryCodeQs(spark,"file:///home/takeo/pycharmprojects/covid_project/CountryCodeQS.csv")
    us_daily(spark,"file:///home/takeo/pycharmprojects/covid_project/us_daily.csv")
    states_abv(spark,"file:///home/takeo/pycharmprojects/covid_project/states_abv.csv")
    us_states(spark,"file:///home/takeo/pycharmprojects/covid_project/us_states.csv")
    usa_hospitalbeds(spark,"file:///home/takeo/pycharmprojects/covid_project/usa_hospitalbeds.geojson")
    county_population(spark,"file:///home/takeo/pycharmprojects/covid_project/County_Population.csv")
    data(spark,"file:///home/takeo/pycharmprojects/covid_project/data.csv")
    us(spark,"file:///home/takeo/pycharmprojects/covid_project/us.csv")
    states_daily(spark,"file:///home/takeo/pycharmprojects/covid_project/states_daily.csv")
    us_county(spark,"file:///home/takeo/pycharmprojects/covid_project/us_county.csv")

    create_dimensional_model(spark)








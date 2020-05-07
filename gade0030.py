# Databricks notebook source
# MAGIC %md
# MAGIC #Coronavirus genome strains analysis in Spark

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import pandas as pd
from pyspark.ml.stat import ChiSquareTest


# COMMAND ----------

# MAGIC %md
# MAGIC ##Strains dataset - loading, cleaning and processing

# COMMAND ----------

# DBTITLE 1,Loading
# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/shravya-95/gade0030_spark_project/master/nextstrain_metadata.tsv
# MAGIC rm -r corona
# MAGIC mkdir corona
# MAGIC mv *.tsv corona/strains.tsv

# COMMAND ----------

dbutils.fs.mkdirs("corona")
dbutils.fs.mv("file:/databricks/driver/corona/strains.tsv","dbfs:/corona")
strain_str="strain	virus	gisaid_epi_isl	genbank_accession	date	region	country	division	location	region_exposure	country_exposure	division_exposure	segment	length	host	age	sex	originating_lab	submitting_lab	authors	url	title	date_submitted"
new_strain_schema=strain_str.split("\t")
data=pd.read_csv(r"/dbfs/corona/strains.tsv", sep='\t',encoding='utf-8')

raw_df = spark.createDataFrame(data,schema=new_strain_schema)

# COMMAND ----------

raw_df.count()

# COMMAND ----------

# DBTITLE 1,Data validation + cleaning
# MAGIC %md
# MAGIC Steps for data cleaning strains data: </br>
# MAGIC 1) Check for null values </br>
# MAGIC 2) Check if all strains are unique </br>
# MAGIC 3) Check total unique countries </br>
# MAGIC 4) Check total unique districts/divisions </br>
# MAGIC 5) Check number of records where division exposure is same is division tested </br>
# MAGIC 7) Check all strains are for ncov virus </br>
# MAGIC 6) Keep only strain, country, division </br>
# MAGIC 7) Remove non human hosts </br>

# COMMAND ----------

# DBTITLE 0,Check for null values
missing=raw_df.select([count(when((col(c).isNull() | col(c).isin("?")), c)).alias(c) for c in raw_df.columns])
for c in missing:
  missing.select(c).show()

#Too many null / missing values in location, division exposure, age, sex and title. Cannot use these feilds for analysis!


if raw_df.select("strain").distinct().count()==raw_df.select("*").count():
  print ("All strains are unique")
  
countries1=raw_df.select("country").distinct()
divisions=raw_df.select("division").distinct()

a= raw_df.where((col('division')==col('division_exposure')) | (col('division_exposure') == 'null')).count()
percent=(a/raw_df.count())*100
print (str(percent)+" percent of division exposure data is either same as division or null => No valuable information in division exposure")

raw_df=raw_df.filter(col("host")=="Human")

# COMMAND ----------

# raw_df.filter(col("country")=="USA").count()
# raw_df.select(col("strain")).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Counts dataset - loading, cleaning and processing

# COMMAND ----------

# DBTITLE 1,Loading
# MAGIC %sh
# MAGIC rm *csv
# MAGIC wget https://raw.githubusercontent.com/shravya-95/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/05-02-2020.csv
# MAGIC mv *.csv corona/deaths.csv

# COMMAND ----------

# MAGIC %sh ls corona/

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/corona/deaths.csv","dbfs:/corona")
c_str="Admin2,Province_State,Country_Region,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key"
req_columns=c_str.split(",")
deaths_schema_str="division,state,country,lat,long_,confirmed,deaths,recovered,active,place_key"
deaths_schema=deaths_schema_str.split(",")
deaths_data=pd.read_csv(r"/dbfs/corona/deaths.csv", sep=',',encoding='utf-8',header=0)
raw_deaths = spark.createDataFrame(deaths_data[req_columns],schema=deaths_schema)

# COMMAND ----------

# DBTITLE 1,Data validation + cleaning - Deaths 
# MAGIC %md
# MAGIC 0) Check for null / missing values in columns </br>
# MAGIC 1) Check every entry has country </br>
# MAGIC 2) Check province + country is unique and create a new column </br>
# MAGIC 3) Compare strain divisions and death province for every country - province data missing in second dataset </br>
# MAGIC 4) Check consistency of data </br>

# COMMAND ----------

missing=raw_deaths.select([count(when((col(c).isNull() | col(c).isin("?")), c)).alias(c) for c in raw_deaths.columns])
for c in missing:
  missing.select(c).show()
#null values in states - find more

raw_deaths.select(col("country")).where(col("state").isNotNull()).distinct().show()

#ALl countries did nto have province level data - skip province level analysis for this data

raw_deaths.where((col("country").isNull()) | (isnan("country"))).count()

#no empty countries

countries2=raw_deaths.select(col("country")).distinct()


# COMMAND ----------

countries1.subtract(countries2).show(20,False)

#Below countries are miss spelled / missing in counts dataset

# COMMAND ----------

raw_deaths.where((col("country")=="Hong Kong") | (col("state")=="Hong Kong")).show()

#replace china with Hong Kong where state is Hong Kong

raw_deaths.where(raw_deaths['country'].rlike(".*Korea")).show()
#Replace Korea, South with South Korea

raw_deaths.where(raw_deaths['country'].rlike("[tT]aiwan")).show()
#Replace Taiwan* with Taiwan

raw_deaths.where(raw_deaths['country'].rlike(".*[cC]ongo.*")).show()
#Replace Congo (Kinshasa) with Democratic Republic of the Congo|

raw_deaths.where(raw_deaths['country'].rlike(".*[cC]zech.*")).show()
#replace Czechia with Czech Republic

#Replace US with USA

# COMMAND ----------

# DBTITLE 1,Correcting country names to match strains data
#Correcting country names in raw_deaths

#1) My dic with correct names mapping
true_names_dic={"US":"USA","Czechia":"Czech Republic","Congo (Kinshasa)":"Democratic Republic of the Congo","Taiwan*":"Taiwan","Korea, South": "South Korea"}

#2) Convert dic to broadcast variable and define my UDF

broadcastCountries = sc.broadcast(true_names_dic)
def update_names(x):
  if broadcastCountries.value.get(x) is None:
    return x
  else:
    return broadcastCountries.value.get(x)

update_names_udf=udf(update_names)

#3) Apply the UDF and update all country names
counts=raw_deaths.withColumn("country",update_names_udf(col("country"))).select(*(c for c in raw_deaths.columns))

#Replacing China with Honk Kong when state is Hong Kong
counts=counts.withColumn("country",when(col("state")=="Hong Kong","Hong Kong").otherwise(col("country")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analysis of world strains data and world counts 

# COMMAND ----------

#Which labs are contributing most / researching most for vaccines
labs=raw_df.groupBy("country").count().select('country',col('count').alias("strains_contributed").cast(DoubleType()))
labs.orderBy(desc("strains_contributed")).show()

# COMMAND ----------

display(labs.orderBy(desc("strains_contributed")).limit(20))

# COMMAND ----------

#Aggregating and combining counts with strains data
new_group=counts\
.groupby(col("country")) \
.sum("deaths","confirmed","recovered","active") \
.withColumnRenamed("sum(deaths)",'deaths') \
.withColumnRenamed("sum(confirmed)",'confirmed') \
.withColumnRenamed("sum(recovered)",'recovered') \
.withColumnRenamed("sum(active)",'active')

# COMMAND ----------

strains_counts=new_group.alias('a').join(labs.alias('b'),col('b.country') == col('a.country')).select([col('a.'+xx) for xx in new_group.columns] + [col('b.strains_contributed')])

# COMMAND ----------

display(strains_counts.orderBy(asc("strains_contributed")))

# COMMAND ----------

strains_counts.stat.corr("strains_contributed","confirmed")

# medium strong correlation between strains contributed by countries and confirmed cases => more mutations occur when virus spreads

# COMMAND ----------

# MAGIC %md
# MAGIC ##USA strains and USA counts analysis at state level

# COMMAND ----------

#joining strains in US with counts in US
strains_US=raw_df.where(col("country")=="USA")
cases_US=counts.where(col("country")=="USA")


# COMMAND ----------

# DBTITLE 1,Validating data at USA level
strains_US.where(col("division_exposure").isNull()).select(col("division")).distinct().show()
strains_US.where(col("division")=="USA").select(col("originating_lab")).distinct().show()
print (strains_US.where(col("originating_lab")=="UW Virology Lab").count())
strains_US=strains_US.filter(col("division")!="USA")
#UW Virology lab does not have correct division - remove these rows as the division cannot be determined

# COMMAND ----------

strains_US.where(col("location").isNull()).select(col("division")).distinct().show()
#many entires do not have county details - so keep analysis at state level


# COMMAND ----------

#Cleaning date feild and converting to date type
#correcting incorrect date strings
def date_format(x):
  if x=="2020-03":
    return "2020-03-01"
  elif x=="2020-04":
    return "2020-04-01"
  else:
    return x
date_format_udf=udf(date_format)
starins_US=strains_US=strains_US \
 .withColumn("new_date",date_format_udf(col("date"))) \
 .drop("date") \
 .withColumnRenamed("new_date","date")

#converting to date type
strains_US=strains_US \
.withColumn("new_date",to_date("date","yyyy-mm-dd")) \
.drop("date") \
.withColumnRenamed("new_date","date")


# COMMAND ----------

import datetime
from pyspark.sql.functions import col, max as max_, min as min_
strains_US.agg(max_("date")).show()
strains_US.agg(min_("date")).show()

#All strains in USA were collected between 01-01-2020 to 01-31-2020

# COMMAND ----------

#1) UDF to categorize strains based on date collected

def categorize(x):
  if x<=datetime.datetime.strptime("2020-01-10", "%Y-%m-%d").date():
    return 1
  elif x<=datetime.datetime.strptime("2020-01-20", "%Y-%m-%d").date():
    return 2
  else:
    return 3

categorize_udf=udf(categorize)

#3) Apply the UDF and create a column to determine how old the strain is relatively

strains_US=strains_US.withColumn("category",categorize_udf(col("date")).cast(IntegerType()))


# COMMAND ----------

#join strains and cases in US based on state
US_counts_strains=strains_US.alias('a').join(cases_US.alias('b'),col('b.state') == col('a.division')).select([col('b.'+xx) for xx in cases_US.columns] + [col('a.strain'),col('a.category')])


# COMMAND ----------

US_counts_strains.where(col("confirmed")>100000).select([col("confirmed"),col("state")]).show()

# COMMAND ----------

#create a vector for ML model
from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(inputCols=["confirmed", "deaths","recovered","active","category"], outputCol="features")
new_df = vecAssembler.transform(US_counts_strains)


# COMMAND ----------

# DBTITLE 1,Attempted : k-means - insignificant results
# from pyspark.ml.clustering import KMeans
# kmeans = KMeans(k=2, seed=2)  # 3 clusters here
# model = kmeans.fit(new_df.select('features'))
# transformed = model.transform(new_df)
# transformed.groupby((col"strain"),col("prediction")).agg(count(lit(1)).alias("cnt")).show()
# pred=transformed.stat.crosstab("strain","prediction")
#pred.where(col("1")>10).show()

# COMMAND ----------

import numpy as np
X=np.array(new_df.select('features').collect())
X=X.reshape(X.shape[0],X.shape[1]*X.shape[2])

# COMMAND ----------

from sklearn.mixture import GaussianMixture
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import numpy as np
X=np.array(new_df.select('features').collect())
X=X.reshape(X.shape[0],X.shape[1]*X.shape[2])
gmm = GaussianMixture(n_components=3).fit(X)
labels = gmm.predict(X)

# COMMAND ----------

fig, foo = plt.subplots()
foo.scatter(X[:, 0], X[:, 1], c=labels, s=40, cmap='viridis')
plt.xlabel('total confirmed')
plt.ylabel('total deaths')
display()

# COMMAND ----------

(unique, counts) = np.unique(labels, return_counts=True)
np.asarray((unique, counts)).T


# COMMAND ----------

#trade off between memory and epsilon value
import numpy as np 
from sklearn.cluster import DBSCAN 
db = DBSCAN(eps=1, min_samples=20)
db.fit(X)
y_pred = db.fit_predict(X)


# core_samples_mask = np.zeros_like(db.labels_, dtype=bool) 
# core_samples_mask[db.core_sample_indices_] = True
labels = db.labels_ 
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0) 
print(labels) 
plt.scatter(X[:,0], X[:,1],c=y_pred, cmap='Paired')
plt.title('number of clusters: %d' %n_clusters_)
plt.xlabel('total confirmed')
plt.ylabel('total deaths')

# COMMAND ----------

display()

# COMMAND ----------

n_clusters_

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Number of originating labs per country </br>
# MAGIC 2) For world - number of mutations vs total cases per country </br>
# MAGIC 3) For USA - number of deaths per infected person for every division with 5 days margin of finding the strain - get time series data</br>

###
from pyspark.conf import SparkConf
import pandas as pd
import numpy as np
import matplotlib as plt
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import plotly
import plotly.plotly as py
from plotly.graph_objs import *
import requests
requests.packages.urllib3.disable_warnings()

######## agg.profile test-------------------------------------
#df = sqlContext.read.parquet('part-r-00000-1a1f1763-f48e-4612-9669-6752f5a3a3ee.snappy.parquet')
#df2 = sqlContext.read.parquet('part-r-00199-1a1f1763-f48e-4612-9669-6752f5a3a3ee.snappy.parquet')

aggprofile = sqlContext.read.parquet('/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/profileAgg/date=20161124')
aggprofile11 = sqlContext.read.parquet('/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/profileAgg/date=20161124/part-r-00001-1a1f1763-f48e-4612-9669-6752f5a3a3ee.snappy.parquet')

for i in 1:75:
	aggprofile.agg(F.distinct(aggprofile.i)).collect()

#df.agg({"subscriberid": "max"}).collect()
#df.agg({"subscriberid": "min"}).collect()
#df.agg(F.min(df.subscriberid)).collect()

#df.agg(F.countDistinct(df.subscriberid)).collect() #5022
#df.agg(F.countDistinct(df.privacy)).collect()  # 3
#df.agg(F.countDistinct(df.osSet)).collect() # 1
#df.agg(F.countDistinct(df.vzCategMap)).collect()
#df.agg(F.countDistinct(df.appCategMap)).collect()
#df.agg(F.countDistinct(df.dmaMap)).collect()
#df.agg(F.countDistinct(df.stateMap)).collect()

#df.describe(['vzCategMap']).show()

#df.count()
#df.first()
#df.head(3)

######## agg.hist -------------------------------------
agghist11 = sqlContext.read.parquet('/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/hist/date=20170202/part-r-00100-e5046983-1de1-4068-a5f3-50e9f899753b.snappy.parquet')

agghist1 = sqlContext.read.parquet('/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/hist/date=20170202')

df.agg({"subscriberid": "max"}).collect()
df.agg({"subscriberid": "min"}).collect()
from pyspark.sql import functions as F
df.agg(F.min(df.subscriberid)).collect()

df.agg(F.countDistinct(df.subscriberid)).collect() #5022
df.agg(F.countDistinct(df.privacy)).collect()  # 3
df.agg(F.countDistinct(df.osSet)).collect() # 1
df.agg(F.countDistinct(df.vzCategMap)).collect()
df.agg(F.countDistinct(df.appCategMap)).collect()
df.agg(F.countDistinct(df.dmaMap)).collect()
df.agg(F.countDistinct(df.stateMap)).collect()

df.describe(['vzCategMap']).show()


len(agghist1.columns)
agghist1.count()
agghist1.show(2, truncate=True)
agghist1.columns
agghist1.select('subscriberid','tld').show(5)
agghist1.select('tld').distinct().count()

agghist1.describe('tldAggScores').show()
agghist1.groupBy("tld").count().toPandas()[0:20]
sorted(agghist1.groupBy("tld").count().show())[0:20]
tldaggkeys = (agghist1.select(F.explode("tldAggScores")).select("key").distinct().rdd.flatMap(lambda x:x).collect())
tldaggexprs = [F.col("tldAggScores").getItem(k).alias(k) for k in tldaggkeys]
=agghist1.select(*tldaggexprs)

agghist1.select(explode(agghist1.tldAggScores).alias("key", "value")).show()
agghist1.select("tldAggScores").show(2)

agghist1.select("tldAggScores").alias("key","value").show(2)


df.count()
df.first()
df.head(3)

test = df.toPandas()
test["subscriberid"][0]
test.dtypes()

testhist = agghist11.toPandas()
testhist[1:10]

testhist.head()
testhist.describe()
testhist['subscriberid'].value_counts()
testhist['osSet'].value_counts(ascending = True)

# testhist['subscriberid'].hist(bins=50)
# df.boxplot(column='ApplicantIncome')
#df.boxplot(column='ApplicantIncome', by = 'Education')

# testhist.to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data")

####### agg-profile ---------------------------------------

agg_profile_all = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/profileAgg")

dates = agg_profile_all.select(agg_profile_all.date).distinct().toPandas()
#for date_ii in list(dates['date']):
date_list = list(dates['date']);
date_list.sort()
for date_ii in date_list[47:]: #: # 
	print date_ii
	agg_profile_all.where(agg_profile_all.date == date_ii).toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/agg_profile_" + str(date_ii), index=False)
	
	
for date_ii in date_list[46:]: #: # 
	print date_ii
	#agg_profile_all.where(agg_profile_all.date == date_ii).toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/agg_profile_" + str(date_ii), index=False)
	for name_ii in range(200):
		print name_ii
		name_st = '%03d'%name_ii
		path2 = "/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/profileAgg/date="+ str(date_ii) + "/part-r-00" +name_st +"*"
		agg_profile = sqlContext.read.parquet(path2)
		agg_profile.toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/agg_profile/" + "date="+ str(date_ii) + "/" + "part-r-00"+name_st, index=False)

	
	
######## agg-hist -------------------------------------------

agg_hist_all = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/hist")
for name_ii in range(200): #: # 
	name_st = '%03d'%name_ii
	print name_st
	path ="/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/agg/hist/date=20170202/part-r-00"+ name_st +"-e5046983-1de1-4068-a5f3-50e9f899753b.snappy.parquet"
	agg_hist = sqlContext.read.parquet(path)
	agg_hist.toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/agg_hist" + "part-r-00"+ name_st, index=False)	

####### oneday-feature ---------------------------------------

oneday_feature_all = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature")
oneday_feature_11 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature/day\=2016-11-24/part-r-00200-14c90367-a1f3-4cc5-b409-18b65f735fb3.snappy.parquet")
oneday_feature_22 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature/day\=2017-01-27/part-r-00000-24e432b4-3c95-41c3-8d46-dc69b8de19fd.snappy.parquet")

oneday_feature_33 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature/day\=2017-01-27/part-r-00386-24e432b4-3c95-41c3-8d46-dc69b8de19fd.snappy.parquet")

oneday_feature_1 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature/day\=2016-11-24")
oneday_feature_2 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/feature/day\=2017-02-02")

dates = oneday_feature_all.select(oneday_feature_all.day).distinct().toPandas()
#for date_ii in list(dates['date']):
date_list = list(dates['day']);
date_list.sort()
for date_ii in date_list: #: # 
	print date_ii
	oneday_feature_all.where(oneday_feature_all.day == date_ii).toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/oneday_feature/date=" + str(date_ii), index=False)
	
oneday_feature_all.select('feature').distinct().count()


####### oneday-profile ---------------------------------------

oneday_profile_all = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/profile")
oneday_profile_1 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/profile/date=20161124")
oneday_profile_11 = sqlContext.read.parquet("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/Data/agg/oneday/profile/date\=20161124/part-r-00000-597babaa-7df1-49db-8ef2-de01b07e103e.snappy.parquet")

dates = oneday_profile_all.select(oneday_profile_all.date).distinct().toPandas()
#for date_ii in list(dates['date']):
date_list = list(dates['date']);
date_list.sort()
for date_ii in date_list[49:]: #: # 
	print date_ii
	oneday_profile_all.where(oneday_profile_all.date == date_ii).toPandas().to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/oneday_profile/date=" + str(date_ii), index=False)


###### 	
	
agg_profile_20161124 = agg_profile_all.where("date = 20161124")

agg_profile_df_20161124= agg_profile_20161124.toPandas()

agg_profile_df_20161124.to_csv("/Users/fengmei/SJSU_ClASSES/Math203/Verizon/R-data/agg_profile_20161124",index=False)


##### plot --------

#sqlContext.registerDataFrameAsTable(agghist11, "agghist11")
df_hist = agghist11.toPandas()
df_hist = agghist11.groupBy("tld").count().toPandas()
data =Data(df_hist)
data =Data([Scatter(x=df_hist['tld'],y=df_hist['count'])])
py.plot(data, filename = "hist/tld")

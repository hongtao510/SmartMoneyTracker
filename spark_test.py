import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
# from pyspark.sql.functions import col 
from pyspark.sql.functions import broadcast



conf = SparkConf().setAppName("sparktest")
sc = SparkContext(conf = conf)
sql_sc = SQLContext(sc)



lines = sql_sc.read.csv("s3n://taohonginsight18b/eod_summary_withoutzeros.csv", 
                        header=True, mode="DROPMALFORMED")

# lines.show(20)
# lines = sql_sc.read.csv("s3n://taohonginsight18b/UnderlyingOptionsTradesCalcs_2018-02.csv", 
#                         header=True, mode="DROPMALFORMED")


filtered = lines.filter((lines.Symbol =="SPX") & 
	                    (lines.PutCall == 'call') & 
	                    (lines.OptionsBins == '20-40') & 
	                    (lines.DayDiffBins == 'short')
	                    )

if filtered.count()==1:
	print float(filtered.collect()[0].asDict()['mean_vol'])

# rdd.map(lambda t: t if t[0] in b.value)


def sparkrowtodict(row):
	'''
	function convert pyspark row to python dict
	'''
	dict_temp = row.asDict()
	dict_reorged_temp = {(dict_temp['Symbol'], dict_temp['PutCall'], dict_temp['DayDiffBins'], dict_temp['OptionsBins']):[dict_temp['mean_vol'], dict_temp['median_vol'], dict_temp['sd_vol']]}
	return dict_reorged_temp

# <class 'pyspark.rdd.PipelinedRDD'>
list_2 = lines.rdd.map(sparkrowtodict)
# <class 'pyspark.broadcast.Broadcast'>
list_3 = sc.broadcast(list_2.collect())  

list_4 = {}
for d in list_3.value:
	list_4.update(d)



# streaming_dict = {"trade_iv": "0.0000", "best_bid": "0.0000", "buyorsell": "SELL", "totalprem": 28500, "underlying_symbol": "BKX", "quote_datetime": "2018-01-08 11:11:05.375", "best_ask": "5.0000", "option_type": "call", "expiration": "2018-01-19", "error": "", "strike": "110.000", "canceled_trade_condition_id": "0", "trade_size": "570", "trade_delta": "0.5000", "trade_condition_id": "0", "trade_price": "0.5000"}

def sparkfilter(x):
	key_tuple = ('A', 'call', 'short', '0-20')
	value = "8000"
	for k in list_3.value.keys():
		if key_tuple in list_3.value:
			return True


def filter1(x):
   return sparkfilter(x, list_3)

rdd.filter(filter1)

# print filtered.mean_vol


#{"trade_iv": "0.0000", "days_to_exp": 1, "best_bid": "0.0000", "exp_bin": "short", "buy_sell": "BUY", "underlying_symbol": "BKX", "quote_datetime": "2018-01-18 09:49:36.125", "total_prem": 136800, "best_ask": "4.8000", "option_type": "C", "delta_bin": "40-60", "expiration": "2018-01-19", "error": "", "strike": "110.000", "canceled_trade_condition_id": "0", "trade_size": "456", "trade_delta": "0.5000", "trade_condition_id": "0", "trade_price": "3.0000"}

# >>> filtered.collect()[0].asDict()
# {'DayDiffBins': u'short', 'OptionsBins': u'20-40', 'sd_vol': u'3078.63718032326', 'Symbol': u'SPX', 'PutCall': u'call', 'mean_vol': u'1250.18975332068', 'median_vol': u'220'}
# >>> filtered.collect()[0].asDict()['mean_vol']
# u'1250.18975332068' 


# lines_filter = lines.where(col('canceled_trade_condition_id')!=0 &
#                            col('underlying_symbol')=="^SPX"
# 	                       )


# lines.registerTempTable("test2")
# sqlContext.sql("""SELECT * FROM test2
#     WHERE canceled_trade_condition_id != 'null' OR underlying_symbol != 'null' OR high != 'null'"""
# ).show()




# print type(lines), dir(lines)
# lines.show(20)



# from pyspark.sql.functions import udf
# squared_udf = udf(squared, LongType())
# df = sqlContext.table("test")
# display(df.select("id", squared_udf("id").alias("id_squared")))


# ratings = lines.map(lambda x: x.split(",")[0])
# ratings_broadcast = sc.broadcast(ratings.collect())


# print ratings, ratings_broadcast.value

# result = ratings.countByValue()
# print (result)




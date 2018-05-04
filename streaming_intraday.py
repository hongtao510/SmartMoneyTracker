import boto3
import config
import io
import datetime



class S3ObjectInterator(io.RawIOBase):
    '''
        http://anthonyfox.io/2017/07/how-i-used-python-and-boto3-to-modify-csvs-in-aws-s3/
    '''
    def __init__(self, s3_key, s3_secret, bucket, key):
        '''Initialize with S3 bucket and key names'''
        self.s3c = boto3.client('s3', aws_access_key_id = s3_key, aws_secret_access_key = s3_secret)
        self.obj_stream = self.s3c.get_object(Bucket=bucket, Key=key)['Body']

    def read(self, n=-1):
        """Read from the stream"""
        return self.obj_stream.read() if n == -1 else self.obj_stream.read(n)


def buyorsell(dict_temp):
    '''
    function to determine buy or sell of a transaction
    '''
    try:
        trade_price = float(dict_temp['trade_price'])
        best_bid = float(dict_temp['best_bid'])
        best_ask = float(dict_temp['best_ask'])

        if trade_price <= best_bid:
            note_str = "SELL"
        elif trade_price >= best_ask:
            note_str = "BUY"
        elif (best_ask-trade_price)>(trade_price-best_bid):
            note_str = "SELL"
        elif (best_ask-trade_price)<(trade_price-best_bid):
            note_str = "BUY"
        else:
            note_str = "MIDDLE"
    except:
        note_str="error"
    return note_str


def calprem(dict_temp):
    '''
    function to calculate premium
    '''
    try:
        qty = float(dict_temp['trade_size'])
        trade_price = float(dict_temp['trade_price'])

        if "," in str(qty):
            qty2=qty.replace(",", "")
        else:
            qty2=qty
        return int(float(qty2)*trade_price*100)
    except:
        return "error"


def calterm(dict_temp):
    '''
    function to determine long or mid or short term trades
    sample data looks like below
    "expiration": "2018-01-19"
    "quote_datetime": "2018-01-08 11:11:05.375"
    '''
    try:
        quote_datetime_str = dict_temp['quote_datetime']
        quote_datetime = datetime.datetime.strptime(quote_datetime_str, "%Y-%m-%d %I:%M:%S.%f")
        expiration_datetime_str = dict_temp['expiration']
        expiration_datetime = datetime.datetime.strptime(expiration_datetime_str, "%Y-%m-%d")
        days_to_exp = abs((quote_datetime-expiration_datetime).days)
        if days_to_exp<=30:
            trade_term = "short"
        elif days_to_exp>30 and days_to_exp<=60:
            trade_term = "medium"
        else:
            trade_term = "long"
    except:
        trade_term = "error"
        days_to_exp = "error"
    return {"trade_term": trade_term, "days_to_exp":days_to_exp}


def caldelta(dict_temp):
    '''
    function to determine long or mid or short term trades
    '''
    try:
        trade_delta_str = dict_temp['trade_delta']
        trade_delta = float(trade_delta_str)
        if trade_delta<=0.2:
            trade_delta_type = "0-20"
        elif trade_delta>0.2 and trade_delta<=0.4:
            trade_delta_type = "20-40"
        elif trade_delta>0.4 and trade_delta<=0.6:
            trade_delta_type = "40-60"
        elif trade_delta>0.6 and trade_delta<=0.8:
            trade_delta_type = "60-80"
        else:
            trade_delta_type = "80-100"
    except:
        trade_delta_type = "error"
    return trade_delta_type


def streaminglinestodict(raw_data):
    '''Function used to con
    '''
    dict_temp = {}
    try:
        trade_condition_id = int(raw_data[10].replace('"',""))
        if trade_condition_id not in [40,41,42,43,44]:
            dict_temp['underlying_symbol'] = raw_data[0].replace("^", "").replace('"',"")
            dict_temp['quote_datetime'] = raw_data[1]
            dict_temp['expiration'] = raw_data[4].replace('"',"")
            dict_temp['strike'] = raw_data[5].replace('"',"")
            option_type_temp = raw_data[6].replace('"',"")
            if option_type_temp == "C":
                option_type_str = "call"
            if option_type_temp == "P":
                option_type_str = "put"
            dict_temp['option_type'] = option_type_str
            dict_temp['trade_size'] = raw_data[8].replace('"',"")
            dict_temp['trade_price'] = raw_data[9].replace('"',"")
            dict_temp['trade_condition_id'] = raw_data[10].replace('"',"")
            dict_temp['canceled_trade_condition_id'] = raw_data[11].replace('"',"")
            dict_temp['best_bid'] = raw_data[12].replace('"',"")
            dict_temp['best_ask'] = raw_data[13].replace('"',"")
            dict_temp['trade_iv'] = raw_data[14].replace('"',"")
            dict_temp['trade_delta'] = raw_data[15].replace('"',"")
            dict_temp['error'] = ""
            buy_sell_temp = buyorsell(dict_temp)
            dict_temp['buy_sell'] = buy_sell_temp
            cal_prem_temp = calprem(dict_temp)
            dict_temp['total_prem'] = cal_prem_temp
            calterm_temp = calterm(dict_temp)
            dict_temp['exp_bin'] = calterm_temp['trade_term']
            dict_temp['days_to_exp'] = calterm_temp['days_to_exp']
            caldelta_temp = caldelta(dict_temp)
            dict_temp['delta_bin'] = caldelta_temp
            dict_temp['unusual'] = "no"
            dict_temp['z_score'] = "na"
    except:
        dict_temp['error'] = "yes"
        dict_temp['unusual'] = "no"
        dict_temp['z_score'] = "na"
    return dict_temp


# obj_stream = S3ObjectInterator(S3_KEY, S3_SECRET, S3_BUCKET, "intraday_subset.csv")

# k=0
# for line in obj_stream:
#     list_temp = line.split(",")
#     # print list_temp, type(list_temp)
#     print streaminglinestodict(list_temp)
#     print "\n"
#     k+=1
#     if k==7:
#         break


#0 underlying_symbol
#1 quote_datetime
#2 sequence_number
#3 root
#4 expiration
#5 strike
#6 option_type
#7 exchange_id
#8 trade_size
#9 trade_price
#10 trade_condition_id
#11 canceled_trade_condition_id
#12 best_bid
#13 best_ask
#14 trade_iv
#15 trade_delta
#16 underlying_bid
#17 underlying_ask
#18 number_of_exchanges {exchange   bid_size    bid ask_size    ask}[number_of_exchanges]



#!/usr/bin/env Rscript
# R script used to preprocess intraday transcation data 

library(dplyr)
library(lubridate)


wd <- "D:/Dropbox/_Insight/data/endofday/2017/"
setwd(wd)


test1 <- read.csv("D:/Dropbox/_Insight/data/intraday/UnderlyingOptionsTradesCalcs_2018-01.csv", header = FALSE)  

test2 <- read.csv("D:/Dropbox/_Insight/data/intraday/UnderlyingOptionsTradesCalcs_2018-02.csv", header = FALSE)  

# remove unused columns and update colume names
test1_sub <- test1[2:29333456,1:19]
test2_sub <- test1[2:1448682,1:19]

test_all <- rbind(test1_sub, test2_sub)

options(digits.secs=3)
test_all[,2] <- ymd_hms(test_all[,2])


col_header <- c("underlying_symbol", "quote_datetime", "sequence_number", "root", "expiration", 
	                 "strike", "option_type", "exchange_id", "trade_size", "trade_price", "trade_condition_id", 
	                 "canceled_trade_condition_id", "best_bid", "best_ask", "trade_iv", "trade_delta", 
	                 "underlying_bid", "underlying_ask", "number_of_exchanges")

names(test_all) <- col_header

test_all <- arrange(test_all, underlying_symbol, quote_datetime)


write.csv(test_all, "intraday_subset.csv", row.names = FALSE)

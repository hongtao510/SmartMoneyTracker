#!/usr/bin/env Rscript
# R script used to preprocess intraday transcation data 

library(dplyr)
library(lubridate)

options(digits.secs=3)

wd <- "D:/Dropbox/_Insight/data/"
setwd(wd)


intraday_subset <- read.csv("intraday_subset.csv", header = TRUE)  
intraday_subset[,2] <- ymd_hms(intraday_subset[,2])


intraday_ticker_pool <- unique(intraday_subset$underlying_symbol)


intraday_subset_sort_quote <- arrange(intraday_subset, quote_datetime)


write.csv(intraday_subset_sort_quote, "intraday_subset_sort_quote.csv", row.names = FALSE)

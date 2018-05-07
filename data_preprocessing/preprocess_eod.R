#!/usr/bin/env Rscript
# R script used to preprocess end of day options transcation data 
# 1. groups options flow to five delta groups (0, 0.2, 0.4, 0.6, 0.8, 1)
# 2. groups flow to three exp groups based on # of days to exp (0, 30, 60, above)
# 3. then calculate mean, sd, and median for volume

library(dplyr)
library(lubridate)
library(gtools)
library(foreach)
library(doParallel)

wd <- "D:/Dropbox/_Insight/data/endofday/2017/"
setwd(wd)

#all_files <- list.files("/Users/taohong/Dropbox/_Insight/data/endofday/2017/", pattern="^2017")
all_files <- list.files(pattern="^2017")
all_files <- mixedsort(all_files)


###################################
## combine End of Day Files
###################################

# register number of cores
cl <- makeCluster(detectCores() - 2)
registerDoParallel(cl, cores = detectCores() - 2)

all_data <- foreach(i = 1:length(all_files), .combine="rbind", .packages=c("dplyr", "lubridate")) %dopar% {
	# "/Users/taohong/Dropbox/_Insight/data/endofday/2017/20170103_OData.csv"
	test1 <- read.csv(paste0("D:/Dropbox/_Insight/data/endofday/2017/", all_files[i]), header = TRUE) %>% 
	         select(c(Symbol, ExpirationDate, DataDate, Delta, Volume, OpenInterest, PutCall)) %>% 
	         filter(Delta!=0)

	test1$OptionsBins <- cut(abs(test1$Delta), breaks=c(0,0.2,0.4,0.6,0.8,1.0), labels=c("0-20","20-40","40-60","60-80","80-100"))
	test1$ExpirationDate <- ymd(test1$ExpirationDate)
	test1$DataDate <- ymd(test1$DataDate)
	test1$DayDiff <- test1$ExpirationDate- test1$DataDate
	test1$DayDiff <- as.numeric(test1$DayDiff) 
	test1$DayDiffBins <- cut(abs(test1$DayDiff), breaks=c(-1,30,60,9999), labels=c("short","medium","long"))
	return (test1)
}

gc()

all_data_nozeros <- filter(all_data, Volume!=0)

# save to hard drive
saveRDS(all_data, "all_data_eod_withzeros.rds")
saveRDS(all_data_nozeros, "all_data_eod_withoutzeros.rds")

symbol_pool <- unique(test1$Symbol)

#test2 <- all_data_nozeros %>% filter(Symbol %in% c("SPX", "SPY", "QQQ", "AAPL")) %>% group_by(Symbol, PutCall, OptionsBins, DayDiffBins)
test2 <- all_data_nozeros %>% group_by(Symbol, PutCall, OptionsBins, DayDiffBins)


test2_summary <- test2 %>% summarise(median_vol = median(Volume), mean_vol = mean(Volume), sd_vol=sd(Volume))

write.csv(test2_summary, "eod_summary_withoutzeros.csv", row.names = FALSE)









# for (each_file in all_files[1:10]){
# 	cat(each_file, "\n")
# 	# "/Users/taohong/Dropbox/_Insight/data/endofday/2017/20170103_OData.csv"
# 	test1 <- read.csv(paste0("/Users/taohong/Dropbox/_Insight/data/endofday/2017/", each_file), header = TRUE) %>% 
# 	         select(c(Symbol, ExpirationDate, DataDate, Delta, OpenInterest, PutCall))

# 	test1$OptionsBins <- cut(abs(test1$Delta), breaks=c(0,0.2,0.4,0.6,0.8,1.1), labels=c("0-20","20-40","40-60","60-80","80-100"))
# 	test1$ExpirationDate <- ymd(test1$ExpirationDate)
# 	test1$DataDate <- ymd(test1$DataDate)
# 	test1$DayDiff <- test1$ExpirationDate- test1$DataDate
# 	test1$DayDiff <- as.numeric(test1$DayDiff) 

# 	test1$DayDiffBins <- cut(abs(test1$DayDiff), breaks=c(0,30,60,999), labels=c("short","medium","long"))
# 	all_data <- rbind(all_data, test1)
# 	}





#  [1] "Symbol"            "ExpirationDate"    "AskPrice"         
#  [4] "AskSize"           "BidPrice"          "BidSize"          
#  [7] "LastPrice"         "PutCall"           "StrikePrice"      
# [10] "Volume"            "ImpliedVolatility" "Delta"            
# [13] "Gamma"             "Vega"              "Rho"              
# [16] "OpenInterest"      "UnderlyingPrice"   "DataDate"         
# [19] "OptionsBins"       "DayDiff"           "DayDiffBins"   
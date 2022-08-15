### Find consecutive no rain periods for each station

dat<-read.csv("daily_merge7_long.csv")

# format date column
dat$date<-as.Date(dat$date, format = "%m/%d/%Y")
head(dat)

# make list of station names
list<-unique(dat$station_name)

library(data.table)

# make empty data frame
dat3<-data.frame()

for (i in list){
  # subset for station
  dat2<-dat[which(dat$station_name == i),]
  
  # remove "NA" value rows
  dat2<-na.omit(dat2)
  
  # convert values to numeric
  dat2$precip_mm<-as.numeric(dat2$precip_mm)
  
  head(dat2)
  
  # count consecutive same value days
  dat2$consec<-with(dat2, (precip_mm == 0)*
                      ave(precip_mm, rleid(precip_mm == 0),
                          FUN=seq_along))
  
  # combine all into one dataframe
  dat3<-rbind(dat3, dat2)
}

head(dat3, 50)
summary(dat3$consec)

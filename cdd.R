# Created by: Billy Henshaw
# Edited 8/23/2022
# CDD
### Find annual maximum consecutive dry days at each pixel from 1990-2014
# To be run AFTER convert_to_int.R

library(devtools)
library(doParallel)
library(ncdf4)
library(raster)
library(dplyr)
library(ClusterR)
library(RColorBrewer)

packages_vector <- c('ncdf4', 'raster', 'dplyr')

# identify number of cores for parallel processing
UseCores <- detectCores() - 1

# range of years in dataset
years <- seq(1990, 2014)

# register chunk of cores for parallel processing
cl <- makeCluster(UseCores)
registerDoParallel(cl)

# unregister parallel processing in case of errors
unregister_dopar <- function() {
  env <- foreach:::.foreachGlobals
  rm(list=ls(name=env), pos=env)
}

# set working directory here
#wd <- "F:/DATA/HawaiiEP/All_Islands_Daily_RF_asc" # Billy's directory
wd <- "D:/FromSeagate/WORKING/DailyMaps/All_Islands_Daily_RF_asc" # Abby's directory

setwd(wd)

# create year folders in workspace if they don't exist
for (year in years) {
  dir.create(file.path(wd, year), showWarnings = FALSE)  
}

# create a list of lists, with each list being one year of daily files.
#  25 lists of 365 or 366 files
data <- lapply(years, function(x) {
  list.files(file.path(wd, x, 'int'), 
             full.names = TRUE, 
             all.files = TRUE,
             pattern = '.tif$',
             no.. = TRUE)
}
)

# grab basenames of all files
#  part of the file path that just includes the file name, including extension
#  function grabs filename without extension
basenames <- lapply(
  lapply(
    years, 
    function(x) {
      list.files(file.path(wd, x, 'int'), 
                 all.files = TRUE,
                 pattern = '.tif$',
                 no.. = TRUE)
    }
  ), 
  function(y) {
    tools::file_path_sans_ext(y)
  }
)


# use first twenty files of 1990 as a sample for testing. Can comment out in prod
#sample <- data[[1]][1:20]

beginCluster(UseCores)
unregister_dopar()

# uncomment for testing
# rainyday_dir <- file.path(wd, '1990', 'rainyday_rc')
# rc_dir <- file.path(wd, '1990', 'sdii_rc')
# int_dir <- file.path(wd, '1990', 'int')
# sdii_dir <-  file.path(wd, '1990', 'sdii')
# r5d_dir <-  file.path(wd, '1990', 'r5d')
# cdd_rc_dir <- file.path(wd, '1990', 'cdd_rc')
# output_dir <- file.path(wd, '1990', 'cdd')

# reclassification matrix for CDD (<1 mm = 1, everything else = 0)
# since we are using integer rasters (rainfall * 100), use 100 as threshold
# less than 1mm is 1, greater or equal to 1mm is 0
rcl <- rbind(c(0, 100, 1),
             c(100, 9999999, 0)
)

# function for finding the max consec dry days per year
cdd <- function(y, val = 1) {
  maxval <- with(rle(y), max(lengths[values == val]))
  return(maxval)
}

beginCluster(UseCores)
unregister_dopar()

# run a for loop of each year's data with parallel processing
foreach (i = 1:length(years), .packages = packages_vector) %dopar% {
  
  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'cdd'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'cdd_rc'), showWarnings = FALSE)
  
  cdd_rc_dir <- file.path(wd, years[i], 'cdd_rc')
  int_dir <- file.path(wd, years[i], 'int')
  output_dir <-  file.path(wd, years[i], 'cdd')
  
  # create empty stack
  x <- stack()
  
  # iterate through each file in the year
  # for (day in sample) {
  for (day in data[[i]]) {
    
    # prepare new file name
    rc_file_name <- sub(pattern = int_dir, 
                        replacement = cdd_rc_dir,
                        paste0(tools::file_path_sans_ext(day),
                               "_cdd_rc.tif"))
    
    # if the file exists already, go to next day. Allows quick
    # resume after error. Be sure to delete the error output file
    # before resuming!
    if (file.exists(rc_file_name)) next
    
    # perform reclassification using reclass matrix
    cat("reclassing", tools::file_path_sans_ext(day), '\n')
    img_rc <- reclassify(
      raster(day),
      rcl,
      include.lowest = TRUE)
    
    # write reclassed raster to output dir as Boolean datatype
    writeRaster(img_rc,
                filename = rc_file_name,
                overwrite=T,
                format="GTiff",
                datatype="LOG1S")
    
    # add reclassed raster to stack
    x <- stack(x, img_rc)
    
  }
  
  output_file_name <- file.path(output_dir,
                                paste0(years[i],
                                       "_cdd.tif"))
  
  cat("calculating cdd for", years[i], '\n')
  
  # perform sum aggregation on raster stack
  img_cdd <- calc(x,
                  cdd)
  
  # write cdd output to output dir as integer type
  writeRaster(img_cdd,
              filename =
                output_file_name,
              overwrite=T,
              format="GTiff",
              datatype="INT2S")
  
}

# free up the cores used for parallel processing
stopCluster(cl)


# move cdd annual layers to a single folder
wd2 <- "D:/FromSeagate/WORKING/DailyMaps" # Abby's directory

# create cdd_annual directory if it doesn't exist
dir.create(file.path(wd2, 'cdd_ann'), showWarnings = FALSE)

for (year in years) {
  # set wd for each year's cdd folder
  wd.yr<-file.path(wd, year, 'cdd')
  setwd(wd.yr)
  # open raster, then re-write in new folder. 
  cdd.yr <- raster(paste0(year,"_cdd.tif"))
  setwd(file.path(wd2,'cdd_ann'))
  writeRaster(cdd.yr,
              filename =
                paste0(year,"_cdd.tif"),
              overwrite=T,
              format="GTiff",
              datatype="INT2S")
  
}

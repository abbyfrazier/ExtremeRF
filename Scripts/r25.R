# Created by: Billy Henshaw
# Edited 8/10/2022
# Calculate R25 metric from daily rainfall data (integer rasters)
# definition from Chu et al. 2010, Journal of Climate
# Annual Count of days when precip >= 25.4 mm
# outputs: integer rasters, geotiffs
# To be run AFTER convert_to_int.R

library(devtools)
library(doParallel)
library(ncdf4)
library(raster)
library(dplyr)
library(ClusterR)

rm(list=ls())

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

# copying R25 annual layers to a single folder
wd2 <- "D:/FromSeagate/WORKING/DailyMaps" # Abby's directory

# create r25_annual directory if it doesn't exist
dir.create(file.path(wd2, 'r25ann'), showWarnings = FALSE)

setwd(wd)

# create year folders in workspace if they don't exist
for (year in years) {
  dir.create(file.path(wd, year), showWarnings = FALSE)  
}

# create a list of lists, with each list being one year of daily files.
# 25 lists of 365 or 366 files
data <- lapply(years, function(x) {
  list.files(file.path(wd, x, 'int'), 
             full.names = TRUE, 
             all.files = TRUE,
             pattern = '.tif$',
             no.. = TRUE)
}
)

# reclassification matrix for r25 (>25.4 mm = 1, everything else = 0)
# since we are using integer rasters (rainfall * 100), use 2540 as threshold
rcl <- rbind(c(0, 2540, 0),
             c(2540, 9999999, 1)
)

beginCluster(UseCores)
unregister_dopar()

# run a for loop of each year's data with parallel processing
foreach (i = 1:length(years), .packages = packages_vector) %dopar% {
  
  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'r25'), showWarnings = FALSE)
  
  rc_dir <- file.path(wd,years[i], 'rc')
  int_dir <- file.path(wd, years[i], 'int')
  output_dir <-  file.path(wd, years[i], "r25")
  
  # create empty stack
  x <- stack()
  
  # iterate through each file in the year
  for (day in data[[i]]) {
    
    # prepare new file name
    rc_file_name <- sub(pattern = int_dir, 
                        replacement = rc_dir,
                        paste0(tools::file_path_sans_ext(day),
                               "_rc.tif"))
    
    # if the file exists already, add reclass to stack and go to next day. Allows quick
    # resume after error. Be sure to delete the error output file
    # before resuming!
#    if (file.exists(rc_file_name)) {
#      img_rc <- raster(rc_file_name)
#      # add reclassed raster to stack
#      x <- stack(x, img_rc)
#      next
#    }
    
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
  
  cat("Number of files in raster stack:", length(as.list(x)))
  output_file_name <- file.path(output_dir,
                                paste0(years[i],
                                       "_r25.tif"))
  
  cat("calculating r25 for", years[i], '\n')
  
  # perform sum aggregation on raster stack
  img_r25 <- calc(x,
                  sum)
  
  # write r25 output to output dir as integer type
  writeRaster(img_r25,
              filename =
                output_file_name,
              overwrite=T,
              format="GTiff",
              datatype="INT2S")
  
  # Copy r25 raster to wd2
  
  output_file_name2 <- file.path(wd2, 
                                 'r25ann',
                                 paste0(years[i],
                                        "_r25.tif"))
  
  file.copy(from = output_file_name, 
            to = output_file_name2)
}

# free up the cores used for parallel processing
stopCluster(cl)

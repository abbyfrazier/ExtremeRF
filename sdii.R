# Created by: Billy Henshaw
# Edited 8/10/2022
# To be run AFTER convert_to_int.R

library(devtools)
library(doParallel)
library(ncdf4)
library(raster)
library(dplyr)
library(ClusterR)

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
wd <- "F:/DATA/HawaiiEP/All_Islands_Daily_RF_asc"

# copying SDII annual layers to a single folder
wd2 <- "D:/FromSeagate/WORKING/DailyMaps" # Abby's directory

# create SDII_annual directory if it doesn't exist
dir.create(file.path(wd2, 'sdiiann'), showWarnings = FALSE)

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

# reclassification matrix for sdii (>1.0 mm = no reclass, everything else = 0)
rcl <- rbind(c(0, 100, 0))

#  reclassification to assess which pixels were rainy
rcl2 <- rbind(c(0, 100, 0),
              c(100, 999999, 1)
)
# use first twenty files of 1990 as a sample for testing. Can comment out in prod
sample <- data[[1]][1:20]

beginCluster(UseCores)
unregister_dopar()

x <- stack()
y <- stack()

# uncomment for testing
# rainyday_dir <- file.path(wd, '1990', 'rainyday_rc')
# rc_dir <- file.path(wd, '1990', 'sdii_rc')
# int_dir <- file.path(wd, '1990', 'int')
# output_dir <-  file.path(wd, '1990', "sdii")

foreach (i = 1:length(years), .packages = packages_vector) %dopar% {

  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'rainyday_rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii_rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii'), showWarnings = FALSE)

  rainyday_dir <- file.path(wd, years[i], 'rainyday_rc')
  rc_dir <- file.path(wd, years[i], 'sdii_rc')
  int_dir <- file.path(wd, years[i], 'int')
  output_dir <-  file.path(wd, years[i], "sdii")

  # create empty stacks
  x <- stack()
  y <- stack()

  # iterate through each file in the year
  for (day in data[[i]]) {
  # for (day in sample) {
    #  prepare new file name
    rd_file_name <- file.path(rainyday_dir, 
                              paste0(basename(tools::file_path_sans_ext(day)),
                               "_sdii_rainyday.tif"))
    
    rc_file_name <- file.path(rc_dir,
                              paste0(basename(tools::file_path_sans_ext(day)),
                                     "_sdii_rc.tif"))
    
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
                datatype="INT2S")
    
    # add reclassed raster to stack
    x <- stack(x, img_rc)
    
    # perform reclassification using reclass matrix
    cat("reclassing", tools::file_path_sans_ext(day), '(rainy day)\n\n')
    img_rd <- reclassify(
      raster(day),
      rcl2,
      include.lowest = TRUE)
    
    # write reclassed raster to output dir as Boolean datatype
    writeRaster(img_rd,
                filename = rd_file_name,
                overwrite=T,
                format="GTiff",
                datatype="LOG1S")
    
    y <- stack(y, img_rd)
  }
  
  output_file_name <- file.path(output_dir,
                                paste0(tools::file_path_sans_ext(years[i]),
                                       "_sdii.tif"))

  # output_file_name <- file.path(output_dir,
  #                               'sample.tif')

  cat("calculating sdii for", years[i], '\n')
  # cat("calculating sdii for sample...\n")
  
  # perform sum aggregation on raster stack
  img_sum <- calc(x,
                  sum)
  img_sdii_rd <- calc(y,
                      sum)
  img_sdii <- img_sum / (img_sdii_rd * 100)
  
  # write sdii output to output dir as integer type
  writeRaster(img_sdii,
              filename =
                output_file_name,
              overwrite=T,
              format="GTiff",
              datatype="FLT4S")
  
  # Copy sdii raster to wd2
  
  output_file_name2 <- file.path(wd2, 
                                 'sdiiann',
                                 paste0(years[i],
                                        "_sdii.tif"))
  
  file.copy(from = output_file_name, 
            to = output_file_name2)
}

# free up the cores used for parallel processing
stopCluster(cl)

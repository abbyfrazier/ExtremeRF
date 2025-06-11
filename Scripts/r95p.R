# Created by: Billy Henshaw
# Edited 8/10/2022
# To be run AFTER convert_to_int.R

rm(list=ls())

library(devtools)
library(doParallel)
library(ncdf4)
library(raster)
library(dplyr)
library(ClusterR)
library(RColorBrewer)

rasterOptions(progress = 'text', timer = TRUE)

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
# wd <- "D:/FromSeagate/WORKING/DailyMaps/All_Islands_Daily_RF_asc" # Abby's directory

setwd(wd)

# copying R95p annual layers to a single folder
# wd2 <- "D:/FromSeagate/WORKING/DailyMaps" # Abby's directory
wd2 <- "F:/HawaiiEP/ExtremeRF/temp"

# create r95p_annual directory if it doesn't exist
dir.create(file.path(wd2, 'r95pann'), showWarnings = FALSE)

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

# reclassification matrix for r95p (0.0 = NA)
rcl <- rbind(c(0, 100, NA))

# use first twenty files of 1990 as a sample for testing. Can comment out in prod
#sample <- data[[1]][1:20]

beginCluster(UseCores)
unregister_dopar()

# uncomment for testing
#rainyday_dir <- file.path(wd, '1990', 'rainyday_rc')
#rc_dir <- file.path(wd, '1990', 'sdii_rc')
#int_dir <- file.path(wd, '1990', 'int')
#sdii_dir <-  file.path(wd, '1990', 'sdii')
#r5d_dir <-  file.path(wd, '1990', 'r5d')
#output_dir <- file.path(wd, '1990', 'r95p')

# ann_prcp <- c()

# quantile function for calculating 95th %ile
q95 <- function(x){
  quantile(x, 0.95, na.rm = TRUE)
}

foreach (i = 1:length(years), .packages = packages_vector) %dopar% {
  # create directory inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'r95p'), showWarnings = FALSE)
  
  #rainyday_dir <- file.path(wd, years[i], 'rainyday_rc')
  #rc_dir <- file.path(wd, years[i], 'sdii_rc')
  #int_dir <- file.path(wd, years[i], 'int')
  #sdii_dir <-  file.path(wd, years[i], 'sdii')
  #r5d_dir <-  file.path(wd, years[i], 'r5d')
  output_dir <- file.path(wd, years[i], 'r95p')
  
  
  # initialize new raster stack for all prcp
  all_days <- stack()
  
  # initialize new raster stack for extreme prcp
  wet_days <- stack()
  
  output_file_name <- file.path(output_dir,
                                paste0(years[i],
                                       "_r95p.tif"))
  
  # output_file_name <- file.path(output_dir,
  #                               paste0('sample', "_r95p.tif"))

  # if the file exists already, go to next day. Allows quick
  # resume after error. Be sure to delete the error output file
  # before resuming!
#  if (file.exists(output_file_name)) {
#    # Copy r95p raster to wd2
#    output_file_name2 <- file.path(wd2, 
#                                   'r95pann',
#                                   paste0(years[i],
#                                          "_r95p.tif"))
#    
#    file.copy(from = output_file_name, 
#              to = output_file_name2,
#              overwrite = TRUE)
#    next
#    }
  
  
  cat("calculating r95p for", years[i], '\n')
  
  # cat("calculating r95p for 1990\n")
  
  all_days <- stack(data[[i]])
  wet_days <- stack(data[[i]]) %>% 
    reclassify(., rcl, include.lowest = TRUE)
  
  
  # reclass each day in the year to show only wet pixels
  # for (day in data[[i]]) {
  # # for (day in sample) {
  #   img_rc <- reclassify(
  #     raster(day),
  #     rcl,
  #     include.lowest = TRUE)
  #   
  #   print(day)
  # }
  
  # pixel-by-pixel total/annual prcp across all days
  print('TEST1')
  sum_prcp_all <- calc(all_days, fun = sum, na.rm = TRUE)
  print('TEST1 done')
  
  # calculates 95th %ile of prcp on wet days, pixel-by-pixel
  extreme_threshold <- calc(wet_days, fun = q95)
  print('TEST2 done')
  
  # finds which pixels had more prcp than the extreme threshold
  is_extreme <- wet_days > extreme_threshold
  print('TEST3 done')
  
  # names all layers w respective file name
  names(is_extreme) <- basenames[[i]]
  print('TEST4 done')
  # names(is_extreme) <- basenames[[1]][1:length(sample)]
  
  # prcp values only at extreme pixels
  prcp_extreme_only <- wet_days * is_extreme
  names(prcp_extreme_only) <- basenames[[i]]
  print('TEST5 done')
  
  # total precip at extreme pixels
  sum_prcp_extreme <- sum(prcp_extreme_only)
  print('TEST6 done')
  
  # calculate r95p
  img_r95p <- 100 * sum_extreme_prcp / total_prcp
  names(img_r95p) <- basename(tools::file_path_sans_ext(output_file_name))
  print('TEST7 done')
  
  # # plot r95p against a black background for better visual
  # par(mar = c(2, 2, 2, 2), width = 10, height = 5)
  # raster::plot(img_r95p, 
  #              col = rev(brewer.pal(11, 'RdBu')),
  #              colNA = 'black',
  #              main= names(img_r95p),
  #              axes = FALSE,
  #              box = FALSE)
  
  writeRaster(img_r95p,
              filename = output_file_name,
              overwrite = T,
              format = "GTiff",
              datatype = "FLT4S")
  print('TEST8 done')
  # Copy r95p raster to wd2

  # output_file_name2 <- file.path(wd2, 
  #                                'r95pann',
  #                                paste0(years[i],
  #                                       "_r95p.tif"))
  # 
  # writeRaster(img_r95p,
  #             filename = output_file_name2,
  #             overwrite = T,
  #             format = "GTiff",
  #             datatype = "FLT4S")
}

# free up the cores used for parallel processing
stopCluster(cl)
# 
# append the total annual prcp value to the running vector of annual prcp values
# append(ann_prcp, s)

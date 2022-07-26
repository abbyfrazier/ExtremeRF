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
             pattern = '.asc$',
             no.. = TRUE)
}
)

# use first twenty files of 1990 as a sample for testing. Can comment out in prod
sample <- data[[1]][1:20]

beginCluster(UseCores)
unregister_dopar()

rainyday_dir <- file.path(wd, '1990', 'rainyday_rc')
rc_dir <- file.path(wd, '1990', 'sdii_rc')
int_dir <- file.path(wd, '1990', 'int')
sdii_dir <-  file.path(wd, '1990', "sdii")
output_dir <-  file.path(wd, '1990', "r5d")

foreach (i = 1:length(data[1]), .packages = packages_vector) %dopar% {
  
  output_file_name <- file.path(output_dir,
                                paste0(tools::file_path_sans_ext(years[i]),
                                       "_r5d.asc"))
  
  # output_file_name <- file.path(output_dir,
  #                               paste0('sample', "_r5d.asc"))
  
  # if the file exists already, go to next day. Allows quick
  # resume after error. Be sure to delete the error output file
  # before resuming!
  if (file.exists(output_file_name)) next
  
  cat("calculating r5d for", years[i], '\n')
  
  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'rainyday_rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii_rc'), showWarnings = FALSE)
  # dir.create(file.path(wd, years[i], 'int'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'r5d'), showWarnings = FALSE)
  
  rainyday_dir <- file.path(wd, years[i], 'rainyday_rc')
  rc_dir <- file.path(wd, years[i], 'sdii_rc')
  int_dir <- file.path(wd, years[i], 'int')
  sdii_dir <-  file.path(wd, years[i], 'sdii')
  output_dir <-  file.path(wd, years[i], 'r5d')
  
  # create empty stacks
  x <- stack()
  
  # iterate through each file in the year
  # replace line 98 with line 97 in prod
  for (j in seq(length(data[[i]]) - 5)) {
  # for (j in seq(length(sample) - 5)) {
    # cat("calculating r5d for sample...\n")
    cat("calculating r5d for", j, "...\n")
    five_files <- data[[i]][j:j + 5]
    x <- stack(five_files)
    five_day_prcp <- calc(x, sum)
    if (j == 1) {
      max_5dp <- five_day_prcp
    }
    new_max <- max(five_day_prcp, max_5dp)
    max_5dp <- new_max
  }
  # 

  # convert back to float
  img_r5d <- max_5dp / 100
  writeRaster(img_r5d,
              filename =
                output_file_name,
              overwrite=T,
              format="ascii",
              datatype="FLT4S")
  }
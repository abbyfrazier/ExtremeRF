# To be run in this sequence:
# 1. convert_to_int.R
# 2. r25_reclass_BHenshaw.R
# 3. sdii_BHenshaw.R

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

rainyday_dir <- file.path(wd, '1990', 'rainyday_rc')
rc_dir <- file.path(wd, '1990', 'sdii_rc')
int_dir <- file.path(wd, '1990', 'int')
output_dir <-  file.path(wd, '1990', "sdii")

foreach (i = 1:length(data), .packages = packages_vector) %dopar% {

  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'rainyday_rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii_rc'), showWarnings = FALSE)
  # dir.create(file.path(wd, years[i], 'int'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'sdii'), showWarnings = FALSE)

  rainyday_dir <- file.path(wd, years[i], 'rainyday_rc')
  rc_dir <- file.path(wd, years[i], 'sdii_rc')
  int_dir <- file.path(wd, years[i], 'int')
  output_dir <-  file.path(wd, years[i], "sdii")

  # create empty stacks
  x <- stack()
  y <- stack()

  # iterate through each file in the year
  # replace line 86 with line 85 in prod
  # for (day in data[[i]]) {
  for (day in sample) {
    #  prepare new file name
    rd_file_name <- file.path(rainyday_dir, 
                              paste0(basename(tools::file_path_sans_ext(day)),
                               "_sdii_rainyday.asc"))
    
    rc_file_name <- file.path(rc_dir,
                              paste0(basename(tools::file_path_sans_ext(day)),
                                     "_sdii_rc.asc"))
    
    # if the file exists already, go to next day. Allows quick
    # resume after error. Be sure to delete the error output file
    # before resuming!
    if (file.exists(rc_file_name)) next
    
    # perform reclassification using reclass matrix
    cat("reclassing", tools::file_path_sans_ext(day), '\n\n')
    img_rc <- reclassify(
      raster(day),
      rcl,
      include.lowest = TRUE)
    
    # write reclassed raster to output dir as Boolean datatype
    writeRaster(img_rc,
                filename = rc_file_name,
                overwrite=T,
                format="ascii",
                datatype="INT2S")
    
    # add reclassed raster to stack
    x <- stack(x, img_rc)
    
    # perform reclassification using reclass matrix
    cat("reclassing", tools::file_path_sans_ext(day), '(rainy day)\n')
    img_rd <- reclassify(
      raster(day),
      rcl2,
      include.lowest = TRUE)
    
    # write reclassed raster to output dir as Boolean datatype
    writeRaster(img_rd,
                filename = rd_file_name,
                overwrite=T,
                format="ascii",
                datatype="LOG1S")
    
    y <- stack(y, img_rd)
  }
  
  output_file_name <- file.path(output_dir,
                                paste0(tools::file_path_sans_ext(years[i]),
                                       "_sdii.asc"))

  # output_file_name <- file.path(output_dir,
  #                               'sample.asc')

  # cat("calculating sdii for", years[i], '\n')
  cat("calculating sdii for sample...\n")
  
  # perform sum aggregation on raster stack
  img_sum <- calc(x,
                  sum)
  img_sdii_rd <- calc(y,
                      sum)
  img_sdii <- img_sum / (img_sdii_rd * 100)
  
  # write r25 output to output dir as integer type
  writeRaster(img_sdii,
              filename =
                output_file_name,
              overwrite=T,
              format="ascii",
              datatype="FLT4S")
  
}

# free up the cores used for parallel processing
stopCluster(cl)

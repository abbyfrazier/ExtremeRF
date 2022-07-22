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

# reclassification matrix for r25 (>25.4 mm = 1, everything else = 0)
rcl <- rbind(c(0, 2540, 0),
             c(2540, 9999999, 1)
)

beginCluster(UseCores)
unregister_dopar()

# run a for loop of each year's data with parallel processing
foreach (i = 1:length(data), .packages = packages_vector) %dopar% {

  # create directories inside each year's folder if they don't exist
  dir.create(file.path(wd, years[i], 'rc'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'int'), showWarnings = FALSE)
  dir.create(file.path(wd, years[i], 'r25'), showWarnings = FALSE)
  
  rc_dir <- file.path(wd,years[i], 'rc')
  int_dir <- file.path(wd, years[i], 'int')
  output_dir <-  file.path(wd, years[i], "r25")
  
  # create empty stack
  x <- stack()
  
  # iterate through each file in the year
  for (day in data[[i]]) {
    
    #  prepare new file name
    rc_file_name <- sub(pattern = int_dir, 
                        replacement = rc_dir,
                              paste0(tools::file_path_sans_ext(day),
                                     "_rc.asc"))
    
    # if the file exists already, go to next day. Allows quick
    # resume after error. Be sure to delete the error output file
    # before resuming!
    if (file.exists(rc_file_name)) next
    
    # perform reclassifcation using reclass matrix
    cat("reclassing", tools::file_path_sans_ext(day), '\n')
    img_rc <- reclassify(
    raster(day),
    rcl,
    include.lowest = TRUE)

    # write reclassed raster to output dir as Boolean datatype
    writeRaster(img_rc,
    filename = rc_file_name,
    overwrite=T,
    format="ascii",
    datatype="LOG1S")

    # add reclassed raster to stack
    x <- stack(x, img_rc)
    
  }
  
  output_file_name <- file.path(output_dir,
                                paste0(tools::file_path_sans_ext(years[i]),
                                       "_r25.asc"))
  cat("calculating r25 for", years[i], '\n')
  
  # perform sum aggregation on raster stack
  img_r25 <- calc(x,
                  sum)
  
  # write r25 output to output dir as integer type
  writeRaster(img_r25,
              filename =
                output_file_name,
              overwrite=T,
              format="ascii",
              datatype="INT2S")
  
}

# free up the cores used for parallel processing
stopCluster(cl)
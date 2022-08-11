# Created by: Billy Henshaw
# Edited 8/10/2022
# convert raw daily ascii rainfall grids into GeoTIFF integer rasters (much smaller files)

library(raster)

rm(list=ls())

# wd <- "F:/DATA/HawaiiEP/All_Islands_Daily_RF_asc" # Billy's directory
wd <- "D:/FromSeagate/WORKING/DailyMaps/All_Islands_Daily_RF_asc" # Abby's directory
setwd(wd)

years <- seq(1990, 2014)
# create year folders in workspace if they don't exist (okay to run even if folders already exist)
for (year in years) {
  dir.create(file.path(wd, year), showWarnings = FALSE)  
}

# MOVE ALL FILES FOR EACH YEAR INTO RESPECTIVE FOLDERS
# can use "move_files.py" code


# each year has a list of file paths (each element of list is 365)
data <- lapply(years, function(x) {
  list.files(file.path(wd, x), 
             full.names = TRUE, 
             all.files = TRUE,
             pattern = '.asc$')
}
)

# i loops through years
for (i in 1:length(years)) {
  start_time <- Sys.time()
  dir.create(file.path(wd, years[i], 'int'), showWarnings = FALSE)
  int_dir <- file.path(wd, years[i], 'int')
  
  # this loop goes through daily raster files within each year
  for (raster_file in data[[i]]) {
    int_file_name <- sub(pattern = file.path(wd, years[i]), 
                         replacement = int_dir,
                         paste0(tools::file_path_sans_ext(raster_file),
                                "_int.tif"))
    cat('\n', basename(raster_file), "-> int...", '\n')
    img_int <- round(raster(raster_file) * 100, digits = -1)
    writeRaster(img_int,
                filename = int_file_name,
                overwrite=T,
                format="GTiff",
                datatype="INT2S")
  }
  print(years[i])
  end_time <- Sys.time()
  cat(end_time - start_time)
}

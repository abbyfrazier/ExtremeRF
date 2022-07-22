library(raster)

wd <- "F:/DATA/HawaiiEP/All_Islands_Daily_RF_asc"
setwd(wd)

int_dir = file.path(wd, "int")

years <- seq(1990, 2014)
data <- lapply(years, function(x) {
  list.files(file.path(wd, x), 
             full.names = TRUE, 
             all.files = TRUE,
             pattern = '.asc$')
}
)

start_time <- Sys.time()
for (raster_file in data[[1]]) {
  int_file_name <- sub(pattern = wd, 
                       replacement = int_dir,
                       paste0(tools::file_path_sans_ext(data[i]),
                              "_int.asc"))
  cat('\n', basename(raster_file), "-> int...", '\n')
  img_int <- round(raster(raster_file) * 100, digits = -1)
  writeRaster(img_int,
              filename = int_file_name,
              overwrite=T,
              format="ascii",
              datatype="INT2S")
}
end_time <- Sys.time()
cat(end_time - start_time)

raster(data[[1]][1])
     
     
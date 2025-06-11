#location where all original rasters are located
wd="D:/data/climate_data/2019_UH_clim_data/hawaii_1990_2019_monthly_rf_mm/grid_surface_data/statewide/rf_mm/" 

#script will save all processed geotifs at the directory below
output_dir="D:/data/climate_data/2019_UH_clim_data/hawaii_1990_2019_monthly_rf_mm_simple/"

max_n_cores = 4 #number of cores to use in parallel processing (dont use all available, or else computer will be unusable)
# available cores found at as.integer(Sys.getenv('NUMBER_OF_PROCESSORS'))

################
#under the hood
setwd(wd)
library(raster)
library(stringr)

#look for subdirectories in original dataset and crease an equivalent folder structure in the output directory
wd_subfolders=list.dirs(wd, recursive = T) 
wd_subfolders_output=sub(pattern = wd, replacement = output_dir, wd_subfolders)
for (wd_subfolder_output in wd_subfolders_output) dir.create(wd_subfolder_output, recursive = T, showWarnings = F)

#list all tif files in original dataset
all_files=list.files(wd, recursive = T, full.names = T)
all_files=grep(all_files, pattern = ".tif$", value = T)


#split long list of rasters into equivalent-sized lists to be farmed out to cores
all_raster_list=split(all_files, sort(c(1:length(all_files))%%max_n_cores))


#stop sinks function (for stopping writing r terminal output to txt files specified below)
sink.reset <- function(){
  for(i in seq_len(sink.number())){
    sink(NULL)
  }
}

#raster_list=all_raster_list[[1]]

#set up function to process each raster list in parallel
sp_parallel_run=function(raster_list){
  library(raster)
  library(tools)
  library(stringr)
  worker=Sys.getpid()
  file_nm=paste0(wd,"log_",format(Sys.time(), "%a %b %d %H%M%S"),"_worker",worker, ".txt")
  con=file(file_nm, open="wt")
  sink(con)
  cat('\n', 'Started on ', date(), '\n') 
  ptm0 <- proc.time()
  
  for (raster_file in raster_list){
    output_file_name=sub(pattern = wd, replacement = output_dir, raster_file)
    cat("doing ",raster_file, "\n")
    img=raster(raster_file)
    img_int=round(img)
    #plot(img)
    writeRaster(img_int, filename = output_file_name, compress="LZW", overwrite=T, format="GTiff", datatype="INT2U")
  }
  
  #end code
  ptm1=proc.time() - ptm0
  jnk=as.numeric(ptm1[3])
  cat('\n','It took ', jnk, "seconds to process rasters")
  
  on.exit(sink.reset())
  on.exit(close(con), add=T)
}

#set parallel cluster, run function above in parallel
library(snowfall)
cpucores=max_n_cores #min(c(2, as.integer(Sys.getenv('NUMBER_OF_PROCESSORS')))) 
sfInit(parallel=TRUE, cpus=cpucores) # 
sfExportAll()
sfLapply(x=all_raster_list, fun=sp_parallel_run)

sfRemoveAll()
sfStop()


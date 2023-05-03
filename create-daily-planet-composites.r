library(tidyverse)
library(sf)
library(terra)
library(rjson)
library(pbapply)
library(parallel)
library(snowfall)

setwd('c:/workspace/meretschi-basten')
rm(list=ls())


indir = 'c:/workspace/meretschi-data/planet'
outdir = './planet-composites'


# get metadata
sr_files = list.files(indir, recursive=T, full.names=T, pattern='.*_SR_.*.tif$')
udm_files = list.files(indir, recursive=T, full.names=T, pattern='.*udm2.*.tif$')
meta_files = list.files(indir, recursive=T, full.names=T, pattern='.*metadata.json$')
image_ids = str_remove(str_extract(basename(sr_files),'.*_3B'),'_3B$')

# read all meta
meta = pblapply(meta_files, function(fn) fromJSON(readLines(fn)))
timestamps = as.POSIXct(sapply(meta, function(x) as.POSIXct(x$properties$acquired, tz = 'UTC', format = '%Y-%m-%dT%H:%M:%OSZ')),
                        origin = '1970-01-01 00:00:00', tz='UTC')
dates = as.Date(sapply(timestamps, function(x) format(x, '%Y-%m-%d')))
times = sapply(timestamps, function(x) format(x, '%H:%M:%S'))

images_per_day = tibble(
  date = sort(unique(dates)),
  n = purrr::map_int(date, function(x) sum(x == dates)),
)

# # get clipping data and make a clone reference raster
# out_res = 10
# watershed = st_transform(read_sf('c:/workspace/meretschi-data/hydrology/basin_upper_vector.gpkg'), 32632)
# clone_rast = terra::rast(watershed, resolution=out_res)
# basin_rast = rasterize(watershed, clone_rast, field=1, fun=mean)
# basin_cells = global(basin_rast, sum, na.rm=T)[1,1]

# set reference grid to the sentinel images
ref_img = rast('./sentinel-2_postproc/sentinel-2_meretschi_2017-04-20.tif')
clone_rast = ref_img[[1]]*0+1
basin_cells = global(clone_rast, sum, na.rm=T)[1,1]


# filter which images to process
images_per_day = images_per_day %>% filter(date > '2017-01-01', date <= '2022-12-31')

# loop over unique dates, load imagery and composite
# d = images_per_day$date[673]
d = images_per_day$date[56]


createComposite = function(d){

  d = as.Date(d, origin='1970-01-01')

  # initiate data container
  img_data = tibble(
    time = timestamps[d == dates],
    meta = meta[d == dates],
    img_id = image_ids[dates == d],
    category = map_chr(meta, function(x) x$properties$quality_category),
    sr   = lapply(sr_files[dates == d], rast),
    udm  = lapply(udm_files[dates == d], rast)
  )

  # discard all the "test" data
  # img_data = filter(img_data, category != 'test')

  # resample and clip SR and UDM rasters
  img_data = img_data %>%
    mutate(sr = map(sr, function(r) r[[names(r) %in% c('blue','green','red','nir')]]),
           sr = map(sr, function(r) project(r, clone_rast, method='bilinear', threads=T)),
           sr = map(sr, function(r) mask(r, clone_rast)),
           udm = map(udm, function(r) r[[names(r) %in% c('snow','shadow','cloud','haze_light','haze_heavy','confidence')]]),
           udm = map(udm, function(r) project(r, clone_rast, method='near', threads=T)),
           udm = map2(udm, sr, function(r1, r2) mask(r1, r2))
    )

  # get meta stats for basin area only
  img_data = img_data %>%
    mutate(miss_pix = map(sr, function(r) clone_rast - !is.na(r[[1]])),
           coverage = map_dbl(sr, function(r) global(r[[1]] * 0 + 1, sum, na.rm=T)[1,1] / basin_cells),
           snow_pct = map_dbl(udm, function(r) global(r['snow'], sum, na.rm=T)[1,1] / basin_cells),
           shadow_pct = map_dbl(udm, function(r) global(r['shadow'], sum, na.rm=T)[1,1] / basin_cells),
           cloud_pct = map_dbl(udm, function(r) global(r['cloud'], sum, na.rm=T)[1,1] / basin_cells),
           haze_pct = map_dbl(udm, function(r) global(r['haze_heavy'], sum, na.rm=T)[1,1] / basin_cells),
    )

  # remove any grids that do not cover the catchment
  img_data = na.omit(img_data)

  # if there are images in the domain
  if (nrow(img_data)){


    # compositing scheme =========================

    # initialize meta file
    img_data = img_data %>%
      mutate(
        instrument=map_chr(meta, function(x) x$properties$instrument),
        category=map_chr(meta, function(x) x$properties$quality_category),
        satellite_azimuth=map_dbl(meta, function(x) x$properties$satellite_azimuth),
        view_angle=map_dbl(meta, function(x) x$properties$view_angle),
        sun_azimuth=map_dbl(meta, function(x) x$properties$sun_azimuth),
        sun_elevation=map_dbl(meta, function(x) x$properties$sun_elevation),
      )

    # sort table to create optimal stacking order
    img_data = img_data %>%
      arrange(category, cloud_pct, desc(time)) %>%
      mutate(index=1:nrow(img_data))

    # create metadata file
    meta_out = img_data %>%
      select(
        index, img_id, instrument, time, category, satellite_azimuth, view_angle, sun_azimuth, sun_elevation,
        coverage, snow_pct, shadow_pct, cloud_pct, haze_pct
      )

    # PREFER LATER IMAGES FOR HIGHER SUN ELEV / LESS SHADOW?
    # MAKE SURE TO FAVOUR STANDARD IMAGES O


    # start with init layer
    sel_id = 1
    sr_comp  = img_data$sr[[sel_id]]
    udm_comp = img_data$udm[[sel_id]]
    missing  = img_data$miss_pix[[sel_id]]


    # keep track of images in composite
    comp_images = setNames(udm_comp[[1]] * 0 + sel_id, 'meta_index')

    # # apply cloud mask
    # sr_comp  = terra::ifel(udm_comp$cloud, NA, sr_comp)
    # udm_comp = terra::ifel(udm_comp$cloud, NA, udm_comp)
    # missing  = terra::ifel(udm_comp$cloud, 1, missing)

    if (nrow(img_data) > 1){

      # fill remainder iteratively using next best scene
      for (i in 1:(nrow(img_data)-1)){

        sel_id = sel_id + 1

        sr_comp  = terra::ifel(missing, img_data$sr[[sel_id]], sr_comp)
        udm_comp = terra::ifel(missing, img_data$udm[[sel_id]], udm_comp)

        # # apply cloud mask
        # sr_comp = terra::ifel(udm_comp$cloud, NA, sr_comp)
        # udm_comp = terra::ifel(udm_comp$cloud, NA, udm_comp)

        # recalculate missing pixels
        missing_before = missing
        missing = clone_rast - !is.na(sr_comp[[1]])
        updated_px = missing_before-missing

        # update the image index map
        comp_images = ifel(updated_px, sel_id, comp_images)

        # stop iteration as soon as whole catchment is covered by data
        if (!global(missing, max, na.rm=T)[1,1]){
         break
        }
      }
    }

    # setup output metadata


    # write output to disk
    out_sr_fn = paste0(outdir, '/', d,'_sr.tif')
    out_udm_fn = paste0(outdir, '/', d, '_udm.tif')
    out_meta_tbl_fn = paste0(outdir, '/', d, '_source_meta.csv')
    out_meta_img_fn = paste0(outdir, '/', d, '_source_index.tif')

    terra::writeRaster(sr_comp, out_sr_fn, datatype='FLT4S', overwrite=T)
    terra::writeRaster(udm_comp, out_udm_fn, datatype='INT2U', overwrite=T)
    terra::writeRaster(comp_images, out_meta_img_fn, datatype='INT2U', overwrite=T)
    write_csv(meta_out, out_meta_tbl_fn)
  }

  return(NULL)
}



# cl = makePSOCKcluster(4)
# clusterExport(cl, ls())
# loadlib = clusterEvalQ(cl, {library(tidyverse); library(terra)})
# out = pblapply(images_per_day$date, createComposite, cl=cl)
# stopCluster(cl)

out = pblapply(images_per_day$date, createComposite)





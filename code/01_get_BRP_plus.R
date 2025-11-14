
# --- OBS: WORK IN PROGRESS [2025-11-11] --- #

################################################################################
### Beräkning av BRP+ index
#
#   Skiptet använder bestämda indikatorer som på förhand är bestämda och
#   kategoriserade i aspekter, teman och aspekter. Dessa är sparade i 
#   en CSV-fil, start_BRP_plus_indikatorer_tbl.csv. All data för dessa indikatorer
#   hämtas från Kolada genom deras API. Datat standardiseras och beräknar indexet. 
#   
#   Skriptet är uppdelat i sektioner:
#   0. Ladda paket och importera start-filer (i.e. CSV-filerna för BRP+ och kodnycklar)
#   1. Hämta hem data från Kolada
#   
################################################################################

# 0. Ladda paket och importera start-filer ####

# PAKET
# Installera Pacman om det inte redan finns
if (!require("pacman")) install.packages("pacman") 
pacman::p_load(tidyverse, rKolada, readxl, pbapply, here, conflicted) # Ladda paket, eller installera dem om de inte finns och ladda dem sen
conflicted::conflict_prefer("filter", "dplyr") # Om flera paket har funktionen "filter()" använd alltid dplyr-paketet som sandard för "filter()" 


# IMPORT AV CSV-FILER
# Om nödvändigt, rensa Environment
# rm(list=ls())

# Hitta mapp där hela RProject är sparat och skapa path för /data CSV-filerna finns
csv_path  <- here::here("data")

# Lista alla CSV-filer i /data
csv_files <- list.files(csv_path, pattern = "^start.*\\.csv$", full.names = TRUE)

# Importera alla CSV-filer i /data
for (i in seq_along(csv_files)) {
  file_path <- csv_files[[i]]
  raw_name  <- tools::file_path_sans_ext(basename(file_path))  # Filnamn utan .csv
  dat <- readr::read_csv2(file_path) # Uför importen av CSV-filen, read_csv2() används för alla CSV-filer har semikolon som deliminator
  assign(raw_name, dat, envir = .GlobalEnv) # Skapa, namnge och placera CSV-filen som dataframe i global environment 
}

# Ta bort onödiga helpers
rm(dat, raw_name) 

# ####

# 1. Hämta hem data från Kolada ####
# Hämta metadata, däribland vilka vilka KPI som ska hämtas från Kolada
# OBS: Namnändringarna och tabellstrukturerna enbart görs för att koden ska kunna köras utan att ändra namnen senare i koden -- detta ska ändras
metadata_kpi <- start_BRP_plus_indikatorer_tbl %>%
  rename(kpi = Indikator_ID, kpi_text = Indikator_namn) %>% 
  drop_na(kpi)

# Hämta nycklar för geografi: 
# OBS: Namnändringarna och tabellstrukturerna enbart görs för att koden ska kunna köras utan att ändra namnen senare i koden -- detta ska ändras
# 1. nyckel för "region" och "län"
region_lan_nyckel <- start_region_kodnyckel_tbl %>% 
  transmute(municipality_id = Lan_kod, 
            region_lan = Lan_Namn,
            municipality = Kolada_lan_namn) %>%
  select(municipality_id, region_lan, municipality)
# 2. nyckel för municipality_id 
municipality_id_nyckel <- bind_rows(
  start_kommun_kodnyckel_tbl %>% distinct(Lan_kod_S, Lan_Namn) %>% transmute(m_id_text = Lan_kod_S, 
                                                                             municipality = Lan_Namn),
  start_kommun_kodnyckel_tbl %>% distinct(Kommun_kod_S, Kommun_namn) %>% transmute(m_id_text = Kommun_kod_S, 
                                                                                   municipality = Kommun_namn) 
) %>% 
  arrange(m_id_text)



# Hämta data från Kolada (med progress bar) 
# Skapa lista
kpi_list <- metadata_kpi %>%
  distinct(kpi) %>%
  pull()

names(kpi_list) <- kpi_list   # each list element now carries its KPI name

result_list_raw <- pblapply(kpi_list, function(.x) {
  # wrap each iteration in tryCatch so one failing KPI won't stop the whole run
  tryCatch({
    message("KPI: ", .x)
    
    temp_df <- rKolada::get_values(kpi = .x,           # Nyckltal i rKolada / API:et refereras som "KPI"
                                   period = 1990:2025, # Tidperioden som data hämtas hem
                                   verbose = FALSE)    # Skriv inte ut detaljer i loggen (pblapply används istället)
    
    # If the query returned NULL or an empty table, skip this KPI cleanly
    if (is.null(temp_df) || (is.data.frame(temp_df) && nrow(temp_df) == 0)) {
      message("  -> No data for ", .x)
      return(tibble())   # empty tibble, pblapply will keep list element
    }
    
    # proceed with cleaning/processing
    temp_df <- temp_df %>%
      filter(municipality_type %in% c("L", "K"))
    
    # compute start & end years safely (only from non-missing values)
    endyear <- temp_df %>% drop_na(value) %>% pull(year) %>% max()
    startyear <- temp_df %>% drop_na(value) %>% pull(year) %>% unique() %>% tail(6) %>% head(1)
    
    # safer gender detection (avoid referencing temp_df$gender inside mutate)
    genders <- temp_df$gender %>% na.omit() %>% unique()
    kon_ford <- ifelse(sum(str_detect(genders, "M|K")) == 2, 1, 0)
    
    temp_df %>%
      mutate(Lagar = startyear,
             Maxar = endyear,
             year = as.character(year),
             Kon_ford = kon_ford) %>%
      relocate(municipality, municipality_id, kpi, gender, year) %>%
      ungroup()
  }, error = function(e) {
    # on error, log and return empty tibble so overall process continues
    message("  -> ERROR for ", .x, " : ", conditionMessage(e))
    return(tibble())
  })
}, 
cl = NULL)   # cl=NULL makes it run sequentially; remove or set to a cluster if parallel


# Check if there are any empty KPIs (if they have names)
empty_kpis <- names(result_list_raw)[vapply(result_list_raw, nrow, integer(1)) == 0]
length(empty_kpis)  # how many
empty_kpis         # list of KPIs with no rows

# Drop them by name
result_list_clean <- result_list_raw[!names(result_list_raw) %in% empty_kpis]

# Sätt ihop listan av tibbles med data till en stor dataframe
# Detta steget används fortfarande för att kunna köra resten av koden 
# Resten av koden bör dock justeras så att man kan använda köra BRP_plus_data_base (skapas nedan)
brpplus_data_raw <- bind_rows(result_list_clean) 

# Sätt ihop listan av tibbles med data till en stor dataframe och lägg till kolumn "Datum_hamtad"
#BRP_plus_data_base <- bind_rows(result_list_clean) %>%
#                     mutate(Datum_hamtad = format(Sys.Date(), "%Y-%m-%d")) 

# ####

brpplus_data_raw %>% drop_na %>% select(kpi) %>% unique %>% pull %>% sort %>% length

# ####

# 2. Imputering
################################################################################
### Imputera saknade värden, stegvis:
# 1. Beräkna medelvärde per län och per riket, utifrån existerande data
# 2. Skapa kartesisk produkt: Alla kpi x regioner x årtal
# 3. Imputera saknade värden:
#   3a. kommuner NA = medelvärdet för länet
#   3b. län NA = medelvärde för kommunerna i detta län  (BÖR LIGGA FÖRE 3a ?)
#   3c. riket NA = medelvärde för kommunerna i riket

################################################################################
### Gruppering av kommuner per län
region_group_nyckel <- brpplus_data_raw %>% 
  mutate(municipality_id = as.numeric(municipality_id), 
         municipality_id_100 = if_else(str_detect(municipality, "[rR]egion"), municipality_id * 100, municipality_id), 
         region_group = municipality_id_100 %/% 100
  ) %>% 
  select(-municipality_id_100) %>% 
  distinct(municipality, municipality_id, region_group) 

################################################################################
### Imputering
### Skapa kartesisk produkt: alla kpi, alla kommuner, alla årtal
### OBS: Tar ca 5 minuter att köra
brpplus_data_imp <- brpplus_data_raw %>%
  distinct(kpi, gender) %>% 
  tidyr::crossing( brpplus_data_raw %>% distinct(year) ) %>% 
  tidyr::crossing( brpplus_data_raw %>% distinct(municipality) ) %>%
  
  # Sammanfoga kartesiska produkten med huvuddatan + den metadata vi själva skapat
  left_join(brpplus_data_raw %>% 
              select(municipality, municipality_type, year, kpi, gender, value, 
                     Lagar, Maxar, Kon_ford)
  ) %>% 
  
  # Sammanfoga med nyckeln för regiongrupper
  left_join(region_group_nyckel) %>% 
  
  # Ersätt några saknade värden för municipality_type (endast för riket)
  mutate(municipality_type = if_else(is.na(municipality_type) & municipality=="Riket", 
                                     "L", municipality_type)) %>% 
  
  ### Imputeringen
  # Ersätt NA riket med medelvärdet per kpi, kön, år 
  group_by(kpi, year, gender) %>% 
  mutate(value = if_else(municipality=="Riket" & is.na(value), mean(value, na.rm=TRUE), value)) %>% 
  
  # Beräkna medel för alla kommuner per kpi, kön, år, region
  group_by(region_group, 
           kpi, year, gender) %>% 
  mutate(value_mean_region = if_else( municipality_type=="K",  mean(value, na.rm=TRUE) , NA_real_) , 
         # Ersätt NA kommun med region-medel
         value = if_else(municipality_type=="K" & is.na(value) , value_mean_region, value ),
         # Ersätt NA region med region-medel
         value = if_else(municipality_type=="L" & is.na(value) , value_mean_region, value )
  ) %>% 
  drop_na(value) %>% 
  arrange(kpi, gender, year, municipality_id)

################################################################################
### Sammanfoga med metadata metadata fr burt
################################################################################
brpplus_data_imp_meta <- brpplus_data_imp %>% 
  left_join(metadata_kpi %>% 
              select(kpi, kpi_text, Del, Tema, Aspekt, Log, Omvand_skala)
  ) 

# Logaritmera de variabler som ska vara logaritmerade
brpplus_data_imp_meta_log <- brpplus_data_imp_meta %>% mutate(value = ifelse(Log == 1, log(value), value))

# Lägg till en kolumn med dagens datum
brpplus_data_base <- brpplus_data_imp_meta_log %>% mutate(Datum_hamtad = format(Sys.Date(), "%Y-%m-%d")) 
glimpse(brpplus_data_base)

# Export av zippad CSV för att slippa behöva hämta hem all data vid varje körning
# CSV-filen zippas för att den är över 100MB och därmed inte kan läggas upp på GitHub
{
  zip_file <- file.path(csv_path, "BRP_plus_data_base.zip") # Förbered export av ZIP-fil
  tmp_csv  <- tempfile(fileext = ".csv") # Förbered export av CSV-fil till temp
  
  write.csv2(brpplus_data_base, tmp_csv, row.names = FALSE) 
  zip(zipfile = zip_file, files = tmp_csv, flags = "-j") 
  unlink(tmp_csv) # Ta bort CSV-fil från temp
}



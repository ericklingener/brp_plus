
# Import av BRP_plus_data_base ####
if (!require("pacman")) install.packages("pacman") 
pacman::p_load(tidyverse, rKolada, readxl, pbapply, here, conflicted) # Ladda paket, eller installera dem om de inte finns och ladda dem sen
conflicted::conflict_prefer("filter", "dplyr") # Om flera paket har funktionen "filter()" använd alltid dplyr-paketet som sandard för "filter()" 

# Om nödvändigt, rensa Environment
# rm(list=ls())

# Hitta mapp där hela RProject är sparat och skapa path för /data ZIP-filen med data_base
zip_path  <- here::here("data", "BRP_plus_data_base.zip")

# One-block: unzip to temp, read CSV, remove temp automatically
brpplus_data_base <- {
                      tmp_dir <- tempdir() # create a temporary folder
                      unzip(zip_path, exdir = tmp_dir) # unzip the CSV to the temp folder
                      csv_file <- list.files(tmp_dir, pattern = "\\.csv$", full.names = TRUE) # find the CSV file (assumes only one CSV in the ZIP)
                      data <- read.csv2(csv_file, stringsAsFactors = FALSE) # read CSV
                      data # return the data
                      }

# check
head(brpplus_data_base)



################################################################################
### Standardisering enligt "max-min-metoden"
# Villkor: min != max + Omvänd skala

# LOG OCH KÖNSUPPDELAD = värden utanför 0 och 100
# max värde = senaste år + total (ej separat för män och kvinnor)

################################################################################
brpplus_stand_maxmin <- brpplus_data_base %>%
  
  ### Standardisera värden, per kommuntyp, genus, år
  group_by(kpi, municipality_type, gender, year) %>% 
  
  mutate(standard_value = case_when(
    # omvänd skala = 0: beräkna vanligt 
    Omvand_skala==1 & 
      min(value, na.rm=TRUE) != max(value, na.rm=TRUE)  ~ 
      100*( (max(value, na.rm=TRUE) - value)  / (max(value, na.rm=TRUE) - min(value, na.rm=TRUE)) )  ,
    
    # omvänd skala = 1: beräkna omvänt
    Omvand_skala==0 & 
      min(value, na.rm=TRUE) != max(value, na.rm=TRUE) ~ 
      100*( (value - min(value, na.rm=TRUE) )  / (max(value, na.rm=TRUE) - min(value, na.rm=TRUE)) ),
    
    # övriga = 0
    TRUE ~ 0
  )
  ) %>% 
  ungroup

### kontrollera så omvands_skala fortf är unik för varje kpi
# Detta anrop ska resultera i en tom tabell
brpplus_stand_maxmin %>% group_by(kpi, Omvand_skala) %>% tally %>% 
  pivot_wider(names_from = Omvand_skala, values_from=n) %>% drop_na

brpplus_stand_maxmin %>% 
  group_by(kpi_text, municipality_type, gender, year, Omvand_skala) %>% 
  summarise(min=min(standard_value), 
            max=max(standard_value),
            min_value = min(value), 
            max_value = max(value)
  ) 

brpplus_stand_maxmin %>% summary

brpplus_stand_maxmin %>% glimpse

################################################################################
### Exportera till 2 filer för kommun & region
# Exporterat innehåll klistras in i flikarna
# "Dataunderlag_region" och "Dataunderlag_kommun" i filen "Data och index BRP+ 2022.xlsx"
################################################################################
### Skapa funktion för att exportera resultatdata
exportera_brpplusdata <- function(obj, 
                                  filnamnet) {
  obj %>%
    # filter(municipality!="Riket") %>% 
    
    # Skriv om "region" till "län"
    left_join(region_lan_nyckel) %>% 
    mutate(municipality = if_else(!is.na(region_lan), 
                                  region_lan, 
                                  municipality)) %>% 
    
    # skapa variabel "Region" med läns och kommun-nr
    left_join(municipality_id_nyckel) %>% 
    # byt namn
    rename(municipality_id_numerical = municipality_id, 
           municipality_id = m_id_text) %>% 
    mutate(municipality_id = if_else(is.na(municipality_id), 
                                     as.character(municipality_id_numerical), 
                                     municipality_id)) %>% 
    # variabeln Region
    mutate(Region = paste0(municipality_id , " ", municipality)) %>% 
    
    # Skriv om variabel gender
    mutate(gender = case_when(gender=="T"~"Totalt",
                              gender=="K"~"Kvinnor",
                              gender=="M"~"Män",
                              TRUE~gender)) %>% 
    # Byt namn på variabler som passar BRP Excelark
    select(Ar = year, 
           Kon = gender, 
           Varde = standard_value, 
           Indikator_name = kpi_text,
           Tema, 
           Aspekt,
           Del,
           kpi, 
           Omrade_typ = municipality_type, 
           Region,
           Kon_ford, 
           Log, 
           Lagar, 
           Maxar, 
           Omvand_skala
    ) %>% 
    
    ### Filtrera på kommun/län
    #filter(Omrade_typ == kommun_eller_lan)  %>% 
    
    ### Ordna kolumnerna på samma sätt som i excelarket där det ska klistras in
    relocate(Indikator_name, Number=kpi, Region, Kon, Ar, Varde, 
             Del, Tema, Aspekt, 
             Kon_ford, 
             Maxar, Lagar, 
             Log, 
             Omvand_skala,
             Omrade_typ) %>% 
    
    arrange(Indikator_name, Region, Kon, Ar) %>%   
    
    # Export till csv (sep=";"), Använd write.csv2 för option fileEncoding
    # write.csv2(file=filnamnet, fileEncoding="ISO-8859-1", row.names=FALSE)
    writexl::write_xlsx(filnamnet)
}


# Oklart varför detta steg görs...
  # Filtrerar fram alla rader där kön saknas (gender = NA)
  brpplus_stand_maxmin %>% filter(is.na(gender)) 
  # Ersätt värdet i municipality_type med "L" för de rader där
  # municipality_type är NA *och* municipality är "Riket"; 
  # annars behålls det ursprungliga värdet.
  brpplus_stand_maxmin <- brpplus_stand_maxmin %>% mutate(municipality_type = if_else(is.na(municipality_type) & municipality=="Riket", 
                                                                                      "L", municipality_type))

brpplus_stand_maxmin %>% exportera_brpplusdata("brpplus_resultat.xlsx")

### Exportera län
# brpplus_stand_maxmin %>% export_data_lan_kommun("L","brp_plus dataunderlag_region.csv")
### Exportera kommun
# brpplus_stand_maxmin %>% export_data_lan_kommun("K","brp_plus dataunderlag_kommun.csv")



################################################################################
## kontrollera lite data
################################################################################
# Alla kpi-texter från Burt är desamma i slutresultatet
check_df <- metadata_kpi %>% distinct(kpi_text) %>% arrange(kpi_text) %>% rename(burt_kpi_text = kpi_text) %>% drop_na %>%  
  cbind(brpplus_data %>% distinct(kpi, kpi_text) %>% arrange(kpi_text)  ) %>% 
  mutate(check= if_else(burt_kpi_text == kpi_text, 1, 0))

#check_df %>% view
check_df %>% glimpse
check_df %>% str
check_df %>% tally(check)

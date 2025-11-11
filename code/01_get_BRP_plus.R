library("tidyverse")    # install.packages("tidyverse")
library("rKolada")
library("readxl")
library("conflicted")
conflict_prefer("filter", "dplyr")

# Radera alla objekt i environment
rm(list=ls())
# Definiera arbetskatalog = den katalog där skriptet är sparat
setwd( dirname( rstudioapi::getActiveDocumentContext()$path ) )

################################################################################
### Programmet BURT hämtar data från Kolada och sparar output i en xlsx-fil. 
  # Senaste filen just nu heter Utdata_2022-11-03.xlsx
  # Se BRP+ dokumentation för detaljer
  # I detta skript använder vi data från (en kopia av) filen fr Burt. 
  # Detta skript återskaper resultaten från grunden. Detta skript hämtar lista 
  # på KPI från Utdata-filen men laddar ned datan direkt fr Kolada via web-api.
  # Detta skript indexerar variablerna. Detta gjordes tidigare i SAS.
################################################################################

# Hämta metadata, däribland vilka vilka KPI som ska hämtas från Kolada
metadata_kpi <- read_xlsx("Utdata - filen fr Burt.xlsx", 
                          sheet="Metadata index") %>%
                rename(kpi = Number, 
                       kpi_text = Indikator_name) %>% 
                drop_na(kpi)

### Hämta nycklar för geografi: 
  # 1. nyckel för "region" och "län"
  region_lan_nyckel <- read_xlsx("län och regioner.xlsx", sheet="Blad1")
  # 2. nyckel för municipality_id 
  municipality_id_nyckel <- read_xlsx("län och regioner.xlsx", sheet="Blad2" )

### Hämta data fr Kolada, genom att anropa unika KPI 
brpplus_data <- metadata_kpi %>% distinct(kpi) %>% pull() %>% map_dfr(~{ 
  print(.x)
  
  # städa lite
  temp_df <- get_values(kpi=.x, period=1990:2022)  %>% 
    ### Filtrera, för kommun och län 
    filter(municipality_type %in% c("L","K")) 
  
  # Notera senaste år och år = -5, endast år med observationer räknas
  endyear <- temp_df %>% drop_na(value) %>% select(year) %>% max()
  startyear <- temp_df %>% drop_na(value) %>% select(year) %>% unique %>% tail(6) %>% head(1) %>% pull
  
  temp_df %>%   
    # spara start- och slutår som variabler
    mutate(Lagar = startyear, 
           Maxar = endyear, 
           year = as.character(year)) %>%
    
    # Skapa metainfo Kon_ford = är statistiken könsfördelad?
    mutate(Kon_ford = if_else(temp_df$gender %>% unique %>% str_detect("M|K") %>% sum()==2, 1,0)) %>% 
    
    # returnera färdig tabell
    relocate(municipality, municipality_id, kpi, gender, year ) %>% 
    ungroup %>% 
    return()
  }) 
################################################################################
## ska nu vara 74 kpi, vilket verkar stämma
brpplus_data %>% drop_na %>% select(kpi) %>% unique %>% pull %>% sort %>% length



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
region_group_nyckel <- brpplus_data %>% 
  mutate(municipality_id = as.numeric(municipality_id), 
         municipality_id_100 = if_else(str_detect(municipality, "[rR]egion"), municipality_id * 100, municipality_id), 
         region_group = municipality_id_100 %/% 100
  ) %>% 
  select(-municipality_id_100) %>% 
  distinct(municipality, municipality_id, region_group) 

################################################################################
### Imputering
### Skapa kartesisk produkt: alla kpi, alla kommuner, alla årtal
  # tidyr::crossing() 
brpplus_data <- brpplus_data %>%
  distinct(kpi, gender) %>% 
  crossing( brpplus_data %>% distinct(year) ) %>% 
  crossing( brpplus_data %>% distinct(municipality) ) %>%
  
  # Sammanfoga kartesiska produkten med huvuddatan + den metadata vi själva skapat
  left_join(brpplus_data %>% 
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
brpplus_data <- brpplus_data %>% 
  left_join(metadata_kpi %>% 
              select(kpi, kpi_text, Del, Tema, Aspekt, Log, Omvand_skala)
            ) 
################################################################################


################################################################################
### Log och standardisering
  # Villkor: min != max + Omvänd skala

  # LOG OCH KÖNSUPPDELAD = värden utanför 0 och 100
  # max värde = senaste år + total (ej separat för män och kvinnor)

################################################################################
brpplus_data <- brpplus_data %>%   
  # Logaritmera de variabler som ska vara logaritmerade
  mutate(value= if_else(Log==1, log(value), value)) %>% 

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
brpplus_data %>% group_by(kpi, Omvand_skala) %>% tally %>% 
  pivot_wider(names_from = Omvand_skala, values_from=n) %>% drop_na

brpplus_data %>% 
  group_by(kpi_text, municipality_type, gender, year, Omvand_skala) %>% 
  summarise(min=min(standard_value), 
            max=max(standard_value),
            min_value = min(value), 
            max_value = max(value)
           ) 

brpplus_data %>% summary

brpplus_data %>% glimpse

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

brpplus_data %>% filter(is.na(gender))
brpplus_data <- brpplus_data %>% mutate(municipality_type = if_else(is.na(municipality_type) & municipality=="Riket", 
                                   "L", municipality_type))

brpplus_data %>% exportera_brpplusdata("brpplus_resultat.xlsx")

### Exportera län
# brpplus_data %>% export_data_lan_kommun("L","brp_plus dataunderlag_region.csv")
### Exportera kommun
#brpplus_data %>% export_data_lan_kommun("K","brp_plus dataunderlag_kommun.csv")
  
  
  
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

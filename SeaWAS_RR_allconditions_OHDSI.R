#Copyright: Mary Regina Boland
#Climate-Birth Month Disease Risk Study with OHDSI
#For details on method please see Boland et al. 2015 http://jamia.oxfordjournals.org/content/early/2015/06/01/jamia.ocv046.full
#Please direct any questions or comments to Mary Boland at mb3402@columbia.edu

###########################################################
#           SeaWAS Proportions and RR Script              #
###########################################################

# Install any needed libraries
pkgTest <- function(x)
{
  if (!require(x,character.only = TRUE))
  {
    install.packages(x,dep=TRUE)
    if(!require(x,character.only = TRUE)) stop("Package not found")
  }
}
pkgTest('devtools')

install_github("ohdsi/SqlRender")
install_github("ohdsi/DatabaseConnector")

# Load libraries
library(devtools)
library(SqlRender)
library(DatabaseConnector)


###########################################################
# Parameters: Please change these to the correct values:  #
###########################################################

folder        = "F:/Documents/OHDSI/StudyProtocols/Study4_SeaWAS_ClimateBirthMonth_OHDSI/R_scripts" # Folder containing the R files, which will also be used to store output
folder_for_storing_output  = paste(folder, "/SeaWAS_proportions_output/", sep="") #output folder - subfolder of above for storing output of script

user <- NULL 
pw <- NULL 
port <- NULL 
server <- "server_name"  #server or host 

#make sure below is properly updated
connectionDetails <- createConnectionDetails(dbms = "sql server", server = server)

cdmDatabaseSchema <- "omop4.dbo"
resultsDatabaseSchema <- "omop4_cohort_results.dbo"  #This script doesn't require a results database

connection <- connect(connectionDetails)

###########################################################
#                  End of Parameters                      #
###########################################################


###########################################################
#      List of Conditions with at least 1000 Patients     #
###########################################################

SQL <- paste("SELECT DISTINCT a.condition_concept_id, c.concept_name, count(distinct a.person_id) AS num_patients_with_condition ",
             "FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE a ",
             "JOIN @cdmDatabaseSchema.CONCEPT c ON a.condition_concept_id=c.concept_id ",
             "JOIN @cdmDatabaseSchema.PERSON b ON a.person_id=b.person_id ",
             "WHERE b.year_of_birth>=1900 AND b.year_of_birth<=2000 ",
             "GROUP BY (a.condition_concept_id) ",
             "ORDER BY (count(distinct a.person_id)) desc;", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
conditions_with_ptcounts <- querySql(connection, sql)

#trimming the list to only retain the conditions with at least 1000 patients
conditions_with_ptcounts = subset(conditions_with_ptcounts, conditions_with_ptcounts$num_patients_with_condition>=1000)

###########################################################
#  End of List of Conditions with at least 1000 Patients  #
###########################################################




###########################################################
# Getting All Patients with a Birth Month and a Diagnosis #
###########################################################
#getting patient data from server 
SQL <- paste("SELECT distinct a.person_id, a.month_of_birth, a.day_of_birth ",
             "FROM @cdmDatabaseSchema.PERSON a ",
             "JOIN @cdmDatabaseSchema.CONDITION_OCCURRENCE b ON a.person_id=b.person_id ",
             "WHERE a.year_of_birth>=1900 AND a.year_of_birth<=2000;", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
pts_w_condition <- querySql(connection, sql)


rs = dbSendQuery(cumc, query)
birthmonths_allpts = fetch(rs, n=-1)  


daily_index_forweeks=seq(from=1,to=366, by=7)
matrix_pop_birthmonth = table(birthmonths_allpts$month_of_birth,birthmonths_allpts$day_of_birth)

###########################################################
#              End Getting Data from Server               #
###########################################################





###########################################################
#                Smoothed and Raw Proportions             #
###########################################################

i=1
week_proportion_all=c()
month_proportion_all=c()
month_proportion_all_SD = c()
week_proportion_all_SD = c()

raw_month_proportion_all_STAN=c()

while (i<=dim(conditions_with_ptcounts)[1]){
  condition=conditions_with_ptcounts$condition_concept_id[i]
  SQL <- paste("SELECT DISTINCT person_id ",
               "FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE ",
               "WHERE condition_concept_id=",
               conditions_with_ptcounts$condition_concept_id[i],
               ";", sep="")

  sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
  sql <- translateSql(sql, targetDialect = 'sql server')$sql
  pts_w_condition <- querySql(connection, sql)
  
  birthmonth_pts_w_condition = merge(birthmonths_allpts, pts_w_condition, by='person_id')
  
  #getting and saving the raw proportions
  raw_month_proportion_all_STAN[[i]] = table(birthmonth_pts_w_condition$month_of_birth)
  
  matrix_birthmonth_wcondition = table(birthmonth_pts_w_condition$month_of_birth,birthmonth_pts_w_condition$day_of_birth)
  
  proportion_condition_over_pop = matrix_birthmonth_wcondition/matrix_pop_birthmonth
  proportion_condition_over_pop_daily=c()
  proportion_condition_over_pop_daily =c(proportion_condition_over_pop[1, ], proportion_condition_over_pop[2 , ], proportion_condition_over_pop[3 , ], proportion_condition_over_pop[4 , ], proportion_condition_over_pop[5 , ], proportion_condition_over_pop[6, ], proportion_condition_over_pop[7, ], proportion_condition_over_pop[8, ], proportion_condition_over_pop[9, ], proportion_condition_over_pop[10, ], proportion_condition_over_pop[11, ], proportion_condition_over_pop[12 , ])
  proportion_condition_over_pop_daily = na.omit(proportion_condition_over_pop_daily)
  
  daily_index_fordays = seq(from=1, to=366, by=1)
  
  names(proportion_condition_over_pop_daily) = daily_index_fordays
  
  #first calculate the smoothed per day proportion
  smoothed_proportion_condition_over_pop_daily = c()
  
  daily_index_formonths=seq(from=0,to=372, by=31)
  m=1
  while (m < length(daily_index_formonths)){
    start_month = daily_index_formonths[m]
    end_month = daily_index_formonths[m+1]
    smoothed_proportion_condition_over_pop_daily[m] = mean(proportion_condition_over_pop_daily[start_month:end_month])
    m=m+1
  }
  
  #smoothing daily proportion
  smoothed_2mo_daily_proportion=c()
  for (n in 1: length(proportion_condition_over_pop_daily)){  #processing over each day
    if ((n-31)<0){
      k=n-31
      begin_2mo_window = 366 + k
    }else{
      begin_2mo_window = n-31  
    }
    if((n+31)>366){
      d=n+31
      end_2mo_window = d-366
    }else{
      end_2mo_window = n+31  
    }
    if (((n-31)<0)||((n+31)>366)){
      smoothed_2mo_daily_proportion[n] = mean(c(proportion_condition_over_pop_daily[begin_2mo_window:366],  proportion_condition_over_pop_daily[1:end_2mo_window]))
    }else{
      smoothed_2mo_daily_proportion[n] = mean(proportion_condition_over_pop_daily[begin_2mo_window:end_2mo_window])      
    } 
  }
  r=1
  smoothed_2mo_daily_proportion_weeks=c()
  smoothed_2mo_daily_proportion_weeks_sd = c()
  while (r < length(daily_index_forweeks)){
    start_week = daily_index_forweeks[r]  
    end_week = daily_index_forweeks[r+1]
    smoothed_2mo_daily_proportion_weeks[r] = mean(smoothed_2mo_daily_proportion[start_week:end_week])
    smoothed_2mo_daily_proportion_weeks_sd[r] = sd(smoothed_2mo_daily_proportion[start_week:end_week])
    r=r+1
  }
  smoothed_2mo_daily_proportion_months = c()
  smoothed_2mo_daily_proportion_months_sd = c()
  k=1
  while (k < length(daily_index_formonths)){
    start_month = daily_index_formonths[k]
    end_month = daily_index_formonths[k+1]
    if(k==12){
      smoothed_2mo_daily_proportion_months[k]= mean(smoothed_2mo_daily_proportion[start_month:366])
      smoothed_2mo_daily_proportion_months_sd[k] = sd(smoothed_2mo_daily_proportion[start_month:366])
    }else{
      smoothed_2mo_daily_proportion_months[k]= mean(smoothed_2mo_daily_proportion[start_month:end_month])
      smoothed_2mo_daily_proportion_months_sd[k] = sd(smoothed_2mo_daily_proportion[start_month:end_month])
    }
    k=k+1
  }
  
  condition_name = conditions_with_ptcounts$concept_name[conditions_with_ptcounts$condition_concept_id==condition]
  week_proportion_all[[i]] = smoothed_2mo_daily_proportion_weeks[1:52]
  month_proportion_all[[i]] =smoothed_2mo_daily_proportion_months
  month_proportion_all_SD[[i]] = smoothed_2mo_daily_proportion_months_sd
  week_proportion_all_SD[[i]] = smoothed_2mo_daily_proportion_weeks_sd[1:52]
  
  cat("iteration", i, "condition", condition, "\n")
  i = i + 1
}

#condition names
conditions_all=conditions_with_ptcounts$condition_concept_id
condition_names_all = list()
for (i in 1:length(conditions_all)){
  condition_names_all[i] = as.character(conditions_with_ptcounts$concept_name[conditions_with_ptcounts$condition_concept_id==conditions_all[i]] )
}
head(conditions_with_ptcounts)

#########Relative Risk
#for all conditions 
i=1
total_number_people_in_db = length(birthmonths_allpts$person_id)
relative_risk_allconditions = list()
while (i <=length(month_proportion_all)){
  v = month_proportion_all[[i]] 
  total_number_disease = conditions_with_ptcounts$num_patients_with_condition[conditions_with_ptcounts$concept_name==condition_names_all[i]]
  relative_risk_allconditions[[i]] = (v  / (total_number_disease / total_number_people_in_db))
  i = i+1
}

i=1
SeaWAS_RR_all1688conditions = data.frame()
while (i <=length(relative_risk_allconditions)){
  rr_per_month = unlist(relative_risk_allconditions[[i]])
  
  seawas_data_per_month = data.frame(concept_id = conditions_all[i], concept_name = condition_names_all[i], Jan = rr_per_month[1], Feb = rr_per_month[2], Mar = rr_per_month[3], Apr = rr_per_month[4], May = rr_per_month[5], Jun = rr_per_month[6], Jul = rr_per_month[7], Aug = rr_per_month[8], Sept = rr_per_month[9], Oct = rr_per_month[10], Nov = rr_per_month[11], Dec = rr_per_month[12])
  colnames(seawas_data_per_month)[2] = c("concept_name")
  SeaWAS_RR_all1688conditions = rbind(SeaWAS_RR_all1688conditions, seawas_data_per_month)
  
  rm(seawas_data_per_month)
  i = i + 1
}


#for the monthly proportions
i=1
SeaWAS_MonthProp_all1688conditions = data.frame()

while (i <=length(month_proportion_all)){
  prop_per_month = unlist(month_proportion_all[[i]])
  SD_prop_per_month = unlist(month_proportion_all_SD[[i]])
  seawas_data_per_month = data.frame(concept_id = conditions_all[i], concept_name = condition_names_all[i], Jan = prop_per_month[1], Jan_SD = SD_prop_per_month[1], Feb = prop_per_month[2], Feb_SD = SD_prop_per_month[2], Mar = prop_per_month[3], Mar_SD = SD_prop_per_month[3], Apr = prop_per_month[4], Apr_SD = SD_prop_per_month[4], May = prop_per_month[5], May_SD = SD_prop_per_month[5], Jun = prop_per_month[6], Jun_SD = SD_prop_per_month[6], Jul = prop_per_month[7], Jul_SD = SD_prop_per_month[7], Aug = prop_per_month[8], Aug_SD = SD_prop_per_month[8], Sept = prop_per_month[9], Sept_SD = SD_prop_per_month[9], Oct = prop_per_month[10], Oct_SD = SD_prop_per_month[10], Nov = prop_per_month[11], Nov_SD = SD_prop_per_month[11], Dec = prop_per_month[12], Dec_SD = SD_prop_per_month[12])
  colnames(seawas_data_per_month)[2] = c("concept_name")
  SeaWAS_MonthProp_all1688conditions = rbind(SeaWAS_MonthProp_all1688conditions, seawas_data_per_month)
  
  rm(seawas_data_per_month)
  i = i + 1
}


#for the weekly proportion
i=1
SeaWAS_WeekProp_all1688conditions = data.frame()
while (i <=length(week_proportion_all)){
  prop_per_month = unlist(week_proportion_all[[i]])
  SD_prop_per_week = unlist(week_proportion_all_SD[[i]])
  seawas_data_per_week = data.frame(concept_id = conditions_all[i], concept_name = condition_names_all[i], Week1 = seawas_data_per_week[1], Week1_SD = SD_prop_per_week[1],  Week2 = seawas_data_per_week[2], Week2_SD = SD_prop_per_week[2],  Week3 = seawas_data_per_week[3], Week3_SD = SD_prop_per_week[3],  Week4 = seawas_data_per_week[4], Week4_SD = SD_prop_per_week[4],  Week5 = seawas_data_per_week[5], Week5_SD = SD_prop_per_week[5], Week6 = seawas_data_per_week[6], Week6_SD = SD_prop_per_week[6], Week7 = seawas_data_per_week[7], Week7_SD = SD_prop_per_week[7], Week8 = seawas_data_per_week[8], Week8_SD = SD_prop_per_week[8], Week9 = seawas_data_per_week[9], Week9_SD = SD_prop_per_week[9],  Week10 = seawas_data_per_week[10], Week10_SD = SD_prop_per_week[10],  Week11 = seawas_data_per_week[11], Week11_SD = SD_prop_per_week[11],  Week12 = seawas_data_per_week[12], Week12_SD = SD_prop_per_week[12],  Week13 = seawas_data_per_week[13], Week13_SD = SD_prop_per_week[13],  Week14 = seawas_data_per_week[14], Week14_SD = SD_prop_per_week[14], Week15 = seawas_data_per_week[15], Week15_SD = SD_prop_per_week[15], Week16 = seawas_data_per_week[16], Week16_SD = SD_prop_per_week[16], Week17 = seawas_data_per_week[17], Week17_SD = SD_prop_per_week[17], Week18 = seawas_data_per_week[18], Week18_SD = SD_prop_per_week[18], Week19 = seawas_data_per_week[19], Week19_SD = SD_prop_per_week[19], Week20 = seawas_data_per_week[20], Week20_SD = SD_prop_per_week[20], Week21 = seawas_data_per_week[21], Week21_SD = SD_prop_per_week[21], Week22 = seawas_data_per_week[22], Week22_SD = SD_prop_per_week[22], Week23 = seawas_data_per_week[23], Week23_SD = SD_prop_per_week[23], Week24 = seawas_data_per_week[24], Week24_SD = SD_prop_per_week[24], Week25 = seawas_data_per_week[25], Week25_SD = SD_prop_per_week[25], Week26 = seawas_data_per_week[26], Week26_SD = SD_prop_per_week[26], Week27 = seawas_data_per_week[27], Week27_SD = SD_prop_per_week[27], Week28 = seawas_data_per_week[28], Week28_SD = SD_prop_per_week[28], Week29 = seawas_data_per_week[29], Week29_SD = SD_prop_per_week[29], Week30 = seawas_data_per_week[30], Week30_SD = SD_prop_per_week[30], Week31 = seawas_data_per_week[31], Week31_SD = SD_prop_per_week[31], Week32 = seawas_data_per_week[32], Week32_SD = SD_prop_per_week[32], Week33 = seawas_data_per_week[33], Week33_SD = SD_prop_per_week[33], Week34 = seawas_data_per_week[34], Week34_SD = SD_prop_per_week[34], Week35 = seawas_data_per_week[35], Week35_SD = SD_prop_per_week[35], Week36 = seawas_data_per_week[36], Week36_SD = SD_prop_per_week[36], Week37 = seawas_data_per_week[37], Week37_SD = SD_prop_per_week[37], Week38 = seawas_data_per_week[38], Week38_SD = SD_prop_per_week[38], Week39 = seawas_data_per_week[39], Week39_SD = SD_prop_per_week[39], Week40 = seawas_data_per_week[40], Week40_SD = SD_prop_per_week[40], Week41 = seawas_data_per_week[41], Week41_SD = SD_prop_per_week[41], Week42 = seawas_data_per_week[42], Week42_SD = SD_prop_per_week[42], Week43 = seawas_data_per_week[43], Week43_SD = SD_prop_per_week[43], Week44 = seawas_data_per_week[44], Week44_SD = SD_prop_per_week[44], Week45 = seawas_data_per_week[45], Week45_SD = SD_prop_per_week[45], Week46 = seawas_data_per_week[46], Week46_SD = SD_prop_per_week[46], Week47 = seawas_data_per_week[47], Week47_SD = SD_prop_per_week[47], Week48 = seawas_data_per_week[48], Week48_SD = SD_prop_per_week[48], Week49 = seawas_data_per_week[49], Week49_SD = SD_prop_per_week[49], Week50 = seawas_data_per_week[50], Week50_SD = SD_prop_per_week[50], Week51 = seawas_data_per_week[51], Week51_SD = SD_prop_per_week[51], Week52 = seawas_data_per_week[52], Week52_SD = SD_prop_per_week[52])
  colnames(seawas_data_per_month)[2] = c("concept_name")
  SeaWAS_WeekProp_all1688conditions = rbind(SeaWAS_WeekProp_all1688conditions, seawas_data_per_week)
  
  rm(seawas_data_per_week)
  i = i + 1
}


###For Stan algorithm
i=1
SeaWAS_monthprop_all1688conditions_raw_STAN = data.frame()
while (i <=length(raw_month_proportion_all_STAN)){
  month_prop_per_month_STAN = unlist(raw_month_proportion_all_STAN[[i]])
  
  seawas_data_per_month = data.frame(concept_id = conditions_all[i], concept_name = condition_names_all[i], Jan = month_prop_per_month_STAN[1], Feb = month_prop_per_month_STAN[2], Mar = month_prop_per_month_STAN[3], Apr = month_prop_per_month_STAN[4], May = month_prop_per_month_STAN[5], Jun = month_prop_per_month_STAN[6], Jul = month_prop_per_month_STAN[7], Aug = month_prop_per_month_STAN[8], Sept = month_prop_per_month_STAN[9], Oct = month_prop_per_month_STAN[10], Nov = month_prop_per_month_STAN[11], Dec = month_prop_per_month_STAN[12])
  colnames(seawas_data_per_month)[2] = c("concept_name")
  SeaWAS_monthprop_all1688conditions_raw_STAN = rbind(SeaWAS_monthprop_all1688conditions_raw_STAN, seawas_data_per_month)
  
  rm(seawas_data_per_month)
  i = i + 1
}

#Overall Month Distribution for Stan
overall_month_dist = table(birthmonths_allpts$month_of_birth)  #this is the overall month distribution-RAW
seawas_data_per_month = data.frame(concept_id = "00000", concept_name = "Overall Month Dist", Jan = overall_month_dist[1], Feb = overall_month_dist[2], Mar = overall_month_dist[3], Apr = overall_month_dist[4], May = overall_month_dist[5], Jun = overall_month_dist[6], Jul = overall_month_dist[7], Aug = overall_month_dist[8], Sept = overall_month_dist[9], Oct = overall_month_dist[10], Nov = overall_month_dist[11], Dec = overall_month_dist[12])
colnames(seawas_data_per_month)[2] = c("concept_name")
#Binding to same DataFrame
SeaWAS_monthprop_all1688conditions_raw_STAN = rbind(SeaWAS_monthprop_all1688conditions_raw_STAN, overall_month_dist)


#Writing Output to Disk
#RR
write.csv(SeaWAS_RR_all1688conditions, paste(folder_for_storing_output, "SeaWAS_RR_all_1688conditions.csv", sep=""))

#Month Proportions - Smoothed
write.csv(SeaWAS_MonthProp_all1688conditions,  paste(folder_for_storing_output, "SeaWAS_MonthProp_all1688conditions.csv"))

#Week Proportions - Smoothed
write.csv(SeaWAS_WeekProp_all1688conditions,  paste(folder_for_storing_output, "SeaWAS_WeekProp_all1688conditions.csv"))

#Raw Month Proportions for Stan Model
write.csv(SeaWAS_monthprop_all1688conditions_raw_STAN, paste(folder_for_storing_output, "SeaWAS_MonthProp_all1688conditions_raw_STAN.csv"))


###########################################################
# Please Remember to Email (to mb3402@columbia.edu):      #
# 1. SeaWAS_RR_all_1688conditions.csv                     #
# 2. SeaWAS_MonthProp_all1688conditions.csv               #
# 3. SeaWAS_WeekProp_all1688conditions.csv                #
# 4. SeaWAS_MonthProp_all1688conditions_raw_STAN.csv      #
###########################################################

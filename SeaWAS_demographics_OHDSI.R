#Copyright: Mary Regina Boland
#Climate-Birth Month Disease Risk Study with OHDSI
#For details on method please see Boland et al. 2015 http://jamia.oxfordjournals.org/content/early/2015/06/01/jamia.ocv046.full
#Please direct any questions or comments to Mary Boland at mb3402@columbia.edu

###########################################################
#               SeaWAS Demographics Script                #
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

# Folder containing the R files, which will also be used to store output
folder        = "F:/Documents/OHDSI/StudyProtocols/Study4_SeaWAS_ClimateBirthMonth_OHDSI/R_scripts"

#output folder - subfolder of above for storing output of script
folder_for_storing_output  = paste(folder, "/SeaWAS_demographics_output/", sep="")

user <- NULL
pw <- NULL 
port <- NULL 
server <- "server_name"  #server or host 

#make sure below is properly updated
connectionDetails <- createConnectionDetails(dbms = "sql server", server = server)

cdmDatabaseSchema <- "omop4.dbo"
resultsDatabaseSchema <- "omop4_cohort_results.dbo"  #The results database is not used in this script

connection <- connect(connectionDetails)

###########################################################
#                  End of Parameters                      #
###########################################################




###########################################################
#      Demographics (Table 1):Sex, Race, Ethnicity        #
###########################################################

###Generate sex/gender related data for each site
SQL <- paste("SELECT DISTINCT gender_concept_id AS concept_id, count(distinct person_id) AS num_pts ",
      "FROM @cdmDatabaseSchema.PERSON ",
      "WHERE gender_concept_id<>0 AND year_of_birth>=1900 AND year_of_birth<=2000 ",
      "GROUP BY (gender_concept_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
sex_data <- querySql(connection, sql)


###Generate ethnicity related data for each site
SQL <- paste("select distinct ethnicity_concept_id as concept_id, count(distinct person_id) as num_pts ",
             "from @cdmDatabaseSchema.PERSON ",
             "where ethnicity_concept_id<>0 and year_of_birth>=1900 and year_of_birth<=2000 ",
             "group by (ethnicity_concept_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
ethnicity_data <- querySql(connection, sql)


###Generate race related data for each site
SQL <- paste("select distinct race_concept_id as concept_id, count(distinct person_id) as num_pts ",
             "from @cdmDatabaseSchema.PERSON ",
             "where race_concept_id<>0 and year_of_birth>=1900 and year_of_birth<=2000 ",
             "group by (race_concept_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
race_data <- querySql(connection, sql)


sex_data$demo_type = "sex"
ethnicity_data$demo_type = "ethnicity"
race_data$demo_type = "race"

#combine data into one large dataframe
demographic_data_df = rbind(sex_data, ethnicity_data)
demographic_data_df = rbind(demographic_data_df, race_data)

write.csv(demographic_data_df, paste(folder_for_storing_output, "demographic_data_set1.csv", sep=""))
###########################################################
#   End of Demographics (Table 1):Sex, Race, Ethnicity    #
###########################################################




###########################################################
# Demographics:                                           #
# Age, Total Codes, Distinct Codes, Followup              #
###########################################################

#now I need the median and intraquartile ranges for: 
#age (year of service-year of birth), 
#total number of SNOMED-CT codes, 
#distinct number of SNOMED-CT codes, and years of follow-up

#calculating age (year_of_service - year_of_birth)
SQL <- paste("select distinct b.person_id, b.year_of_birth, a.year_of_service ",
  "from ( ",
    "select distinct person_id, year(condition_start_date) as year_of_service ",
    "from @cdmDatabaseSchema.CONDITION_OCCURRENCE ",
  ") a ",
  "join @cdmDatabaseSchema.PERSON b on a.person_id=b.person_id ",
  "where b.year_of_birth>=1900 and b.year_of_birth<=2000;", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
age_data <- querySql(connection, sql)


age_data$age = age_data$year_of_service-age_data$year_of_birth
fivenum_age = fivenum(age_data$age)
median_age = fivenum_age[3]
lowerIQR_age = fivenum_age[2]
upperIQR_age = fivenum_age[4]
age = c("age", lowerIQR_age, median_age, upperIQR_age)
###

#calculating total number of SNOMED-CT codes per patient
SQL <- paste("select a.person_id, count(a.condition_concept_id) as total_codes_per_pt ",
            "from @cdmDatabaseSchema.CONDITION_OCCURRENCE a ",
            "join @cdmDatabaseSchema.PERSON b on a.person_id=b.person_id ",
            "where b.year_of_birth>=1900 and b.year_of_birth<=2000 ",
            "group by (a.person_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
total_OMOP_codes_per_pt <- querySql(connection, sql)


fivenum_totalcodes = fivenum(total_OMOP_codes_per_pt$total_codes_per_pt)
median_totalcodes = fivenum_totalcodes[3]
lowerIQR_totalcodes = fivenum_totalcodes[2]
upperIQR_totalcodes = fivenum_totalcodes[4]
totalcodes = c("total_codes", lowerIQR_totalcodes, median_totalcodes, upperIQR_totalcodes)
###


#calculating the distinct number of SNOMED-CT codes per patient
SQL <- paste("select a.person_id, count(distinct a.condition_concept_id) as distinct_codes_per_pt ",
             "from @cdmDatabaseSchema.CONDITION_OCCURRENCE a ",
             "join @cdmDatabaseSchema.PERSON b on a.person_id=b.person_id ",
             "where b.year_of_birth>=1900 and b.year_of_birth<=2000 ",
             "group by (a.person_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
distinct_OMOP_codes_per_pt <- querySql(connection, sql)


fivenum_distinctcodes = fivenum(distinct_OMOP_codes_per_pt$distinct_codes_per_pt)
median_distinctcodes = fivenum_distinctcodes[3]
lowerIQR_distinctcodes = fivenum_distinctcodes[2]
upperIQR_distinctcodes = fivenum_distinctcodes[4]
distinctcodes = c("distinct_codes", lowerIQR_distinctcodes, median_distinctcodes, upperIQR_distinctcodes)
###

#calculating the number of distinct years that a patient has data (called follow-up here) using CONDITION_ERA table
SQL <- paste("select distinct c.person_id, count(distinct c.year_of_service) as years_of_followup ",
             "from (select distinct b.person_id, b.year_of_birth, a.year_of_service ",
             "from (select distinct person_id, year(condition_start_date) as year_of_service ",
             "from @cdmDatabaseSchema.CONDITION_OCCURRENCE) a ",
             "join @cdmDatabaseSchema.PERSON b on a.person_id=b.person_id ",
             "where b.year_of_birth>=1900 and b.year_of_birth<=2000) c ",
             "group by (c.person_id);", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
followup_data <- querySql(connection, sql)


fivenum_followup = fivenum(followup_data$years_of_followup)
median_followup= fivenum_followup[3]
lowerIQR_followup = fivenum_followup[2]
upperIQR_followup = fivenum_followup[4]
followup = c("followup", lowerIQR_followup, median_followup, upperIQR_followup)
###

#combining data into one dataframe for output
demographic_data_df_set2 = rbind(age, totalcodes)
demographic_data_df_set2 = rbind(demographic_data_df_set2, distinctcodes)
demographic_data_df_set2 = rbind(demographic_data_df_set2, followup)
colnames(demographic_data_df_set2)<-c("demographic_attribute", "lower_IQR", "median", "upper_IQR")

write.csv(demographic_data_df_set2, paste(folder_for_storing_output, "demographic_data_set2.csv", sep=""))

###########################################################
# End of Demographics:                                    #
# Age, Total Codes, Distinct Codes, Followup              #
###########################################################


###########################################################
#     Demographics: Top 100 Conditions Per Institution    #
###########################################################
SQL <- paste("select distinct a.condition_concept_id, count(distinct a.person_id) as num_pts ",
      "from @cdmDatabaseSchema.CONDITION_OCCURRENCE a ",
      "join @cdmDatabaseSchema.PERSON b on a.person_id=b.person_id ",
      "where b.year_of_birth>=1900 and b.year_of_birth<=2000 ",
      "group by (a.condition_concept_id) ",
      "order by (count(distinct a.person_id)) desc;", sep="")

sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
top100_conditions <- querySql(connection, sql)

#trimming the list to only retain the top 100
top100_conditions <- top100_conditions[1:100, ]  #returns only the top 100 conditions

write.csv(top100_conditions, paste(folder_for_storing_output, "demographic_data_top100conditions.csv", sep=""))

###########################################################
# End of Demographics: Top 100 Conditions Per Institution #
###########################################################




###########################################################
# Please Remember to Email (to mb3402@columbia.edu):      #
# 1. demographic_data_set1.csv                            #
# 2. demographic_data_set2.csv                            #
# 3. demographic_data_top100conditions.csv                #
###########################################################


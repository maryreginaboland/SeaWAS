#Copyright: Mary Regina Boland
#Climate-Birth Month Disease Risk Study with OHDSI
#For details on method please see Boland et al. 2015 http://jamia.oxfordjournals.org/content/early/2015/06/01/jamia.ocv046.full
#Please direct any questions or comments to Mary Boland at mb3402@columbia.edu

####################################################################################
#                    SeaWAS Main Association Analysis Script                       #
####################################################################################

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

#install_github("ohdsi/SqlRender")
#install_github("ohdsi/DatabaseConnector")

# Load libraries
library(devtools)
library(SqlRender)
library(DatabaseConnector)


####################################################################################
#               Parameters: Please change these to the correct values:             #
####################################################################################

folder        <- "F:/Documents/OHDSI/StudyProtocols/Study4_SeaWAS_ClimateBirthMonth_OHDSI/R_scripts/" # Folder containing the R files, which will also be used to store output
folder_for_storing_output  <- paste(folder, "SeaWAS_association_output/", sep="") #output folder - subfolder of above

user <- NULL
pw <- NULL
port <- NULL
server <- "server_name"  #server or host

connectionDetails <- createConnectionDetails(dbms = "sql server", server = server)

cdmDatabaseSchema <- "omop4.dbo"                     #this is the OMOP CDM formatted instance with read access
resultsDatabaseSchema <- "omop4_cohort_results.dbo"  #this is the local instance with write access
#Results database is required for this script to run

connection <- connect(connectionDetails)

####################################################################################
#                                End of Parameters                                 #
####################################################################################




####################################################################################
#                                Beginning of Main                                 #
####################################################################################
#Function to convert query result colnames into lowercase
#dataframe = data.frame(PERSON_ID= c(1,2,3,4), CONDITION_CONCEPT_ID = c(100, 200, 300, 5000))  #dataframe for testing function
queryColnamesToLowercase <- function(dataframe)
{
  colnames(dataframe) <- tolower(colnames(dataframe))
  return(dataframe)
}
#queryColnamesToLowercase(dataframe)  #code for calling function

###Generate a list of conditions with at least 1000 patients born between 1900-2000
###CUMC Resulted in 1688 conditions (this will likely vary across institutions)
SQL <- paste("select distinct c.condition_concept_id, c.num_pts_w_condition ",
             "from (select distinct b.condition_concept_id, count(distinct b.person_id) as num_pts_w_condition ",
             "from (select distinct person_id ",
             "from @cdmDatabaseSchema.PERSON ",
             "where year_of_birth>=1900 and year_of_birth<=2000) a ",
             "join @cdmDatabaseSchema.CONDITION_OCCURRENCE b on a.person_id=b.person_id ",
             "group by (condition_concept_id)) c ",
             "where c.num_pts_w_condition>=1000;", sep="")
sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
theo_conditions <- querySql(connection, sql)
theo_conditions <- queryColnamesToLowercase(theo_conditions)

write.csv(theo_conditions, paste(folder, "list_of_conditions_with_atleast_1000pts.csv"))


#data for people, month of birth - this doesn't change for each disease
SQL <- paste("select distinct a.person_id, a.year_of_birth, a.month_of_birth, a.day_of_birth ",
             "from @cdmDatabaseSchema.PERSON a ",
             "join @cdmDatabaseSchema.CONDITION_OCCURRENCE b on a.person_id=b.person_id ",
             "where a.year_of_birth>=1900 and a.year_of_birth<=2000;", sep="")
sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
sql <- translateSql(sql, targetDialect = 'sql server')$sql
birthmonths_allpts <- querySql(connection, sql)
birthmonths_allpts <- queryColnamesToLowercase(birthmonths_allpts)

#variables that shouldn't change in loop
pop_data_per_month = as.numeric(table(birthmonths_allpts$month_of_birth))
months <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec")
months_as_num <- c(1:12)
set.seed(1234)
i <- 1
while(i <= length(theo_conditions$condition_concept_id)) {
  SQL <- paste("select distinct condition_concept_id, person_id, condition_source_value ",
               "from @cdmDatabaseSchema.CONDITION_OCCURRENCE ",
               "where condition_concept_id='", theo_conditions$condition_concept_id[i], "';", sep = "")
  sql <- renderSql(SQL, cdmDatabaseSchema=cdmDatabaseSchema)$sql
  sql <- translateSql(sql, targetDialect = 'sql server')$sql
  query_1 <- querySql(connection, sql)
  query_1 <- queryColnamesToLowercase(query_1)
  
  query_2 <- merge(birthmonths_allpts, query_1, by="person_id") 
  
  create_table_query = paste("CREATE TABLE @resultsDatabaseSchema.SeaWAS_automatedquery ( ",
                             "person_id int NOT NULL, ", 
                             "year_of_birth int DEFAULT NULL, ",
                             "month_of_birth int DEFAULT NULL, ",
                             "day_of_birth int DEFAULT NULL, ",
                             "condition_concept_id int NOT NULL, ",
                             "condition_source_value varchar(50) DEFAULT NULL ",
                             ");", sep="")
  sql <- renderSql(create_table_query, resultsDatabaseSchema=resultsDatabaseSchema)$sql
  sql <- translateSql(sql, targetDialect = 'sql server')$sql
  executeSql(connection, sql)
  
  #Below is a simulated dbWriteTable
  for (k in seq(from=1, to=dim(query_2)[1], by=990)) {
    sql <- "INSERT INTO @resultsDatabaseSchema.SeaWAS_automatedquery (person_id, year_of_birth, month_of_birth, day_of_birth, condition_concept_id, condition_source_value) VALUES "
    max <- k+990
    if (max > dim(query_2)[1])
      max <- dim(query_2)[1]
    
    for (j in k:max){
      sql <- paste(sql, "(", paste(query_2$person_id[j], query_2$year_of_birth[j], query_2$month_of_birth[j], query_2$day_of_birth[j], query_2$condition_concept_id[j], query_2$condition_source_value[j], sep=","), "), ", sep="")  
    }
    sql <- renderSql(sql, resultsDatabaseSchema = resultsDatabaseSchema)$sql
    sql <- translateSql(sql, targetDialect = 'sql server')$sql
    sql <- gsub(', $', '', sql)
    executeSql(connection, sql, progressBar=F, reportOverallTime=F)
  }
  
  
  SQL <- c("select distinct person_id, year_of_birth, month_of_birth, day_of_birth from @resultsDatabaseSchema.SeaWAS_automatedquery;")
  sql <- renderSql(SQL, resultsDatabaseSchema=resultsDatabaseSchema)$sql
  sql <- translateSql(sql, targetDialect = 'sql server')$sql
  final_result <- querySql(connection, sql)
  final_result <- queryColnamesToLowercase(final_result)
  
  #Predicting disease based on birth month
  final_result$status <- 1 #case
  
  #need to get the control population
  num_patients_with_condition <- length(unique(final_result$person_id))
  
  #excluding patients with the given condition
  control_pop_fullset <- birthmonths_allpts[!(birthmonths_allpts$person_id %in% final_result$person_id), ]  
  
  #sampling from the set of the population that does not have the condition
  test_pop <- sample(x=control_pop_fullset$person_id, size=num_patients_with_condition*10, replace=TRUE)
  test_control_pop <- birthmonths_allpts[birthmonths_allpts$person_id %in% test_pop, ]  
  test_control_pop$status <- 0; #since this is the control
  final_result <- data.frame(person_id=final_result$person_id, year_of_birth=final_result$year_of_birth, month_of_birth=final_result$month_of_birth, day_of_birth=final_result$day_of_birth, status=final_result$status)
  cases_and_controls <- rbind(test_control_pop, final_result)
  
  #testing overall months
  overallmonths <- glm(cases_and_controls$status ~ as.numeric(cases_and_controls$month_of_birth), family="binomial")
  
  soverallmonths <- summary(overallmonths)
  soverallmonths.p <- coef(soverallmonths)[8] #this is the overall model p-value
  soverallmonths.estimate <- coef(soverallmonths)[2] #estimate
  soverallmonths.standarderror <- coef(soverallmonths)[4]  #standarderror
  soverallmonths.OR <- exp(coef(overallmonths)) # exponentiated coefficients
  soverallmonths.ORCI <- exp(confint(overallmonths)) # 95% CI for exponentiated coefficients
  
  chi_fit <- anova(overallmonths, test="Chisq")  #chi-square test on the estimates from the model fit above
  chi_p <- chi_fit$'Pr(>Chi)'[2]  #disease-birth month p-value 
  
  #saving results for each disease to file
  output_to_file <- data.frame(concept_id=theo_conditions$condition_concept_id[i], num_patients_with_condition=num_patients_with_condition, Allmonths.p=soverallmonths.p, Allmonths.estimate=soverallmonths.estimate, Allmonths.standarderror=soverallmonths.standarderror,  Allmonths.OR= soverallmonths.OR[2], Allmonths.ORCILowerbound = soverallmonths.ORCI[2], Allmonths.ORCIUpperbound = soverallmonths.ORCI[4], Chi_New = chi_p)
  write.csv(output_to_file, paste(folder_for_storing_output, theo_conditions$condition_concept_id[i], ".csv", sep=""))
  
  #clearing all local values 
  rm(initial_set, final_result, query_1, query_2, chi_fit, chi_p, num_patients_with_condition, soverallmonths.p, soverallmonths.estimate, soverallmonths.standarderror, soverallmonths.OR, soverallmonths.ORCI, overallmonths, soverallmonths, cases_and_controls, test_control_pop, test_pop, control_pop_fullset)
  
  #need to clear SeaWAS_automatedquery at the end of each iteration
  SQL <- c("drop table @resultsDatabaseSchema.SeaWAS_automatedquery;") 
  sql <- renderSql(SQL, resultsDatabaseSchema=resultsDatabaseSchema)$sql
  sql <- translateSql(sql, targetDialect = 'sql server')$sql
  executeSql(connection, sql)
  
  cat("iteration", i, "condition", theo_conditions$condition_concept_id[i], "\n")
  i = i + 1
}

####################################################################################
#                             End of Main and Script                               #
####################################################################################


####################################################################################
# Please Remember to Email (to mb3402@columbia.edu):                               #
# 1. compress all files in folder_for_storing_output into one zip file and email   #
# 2. list_of_conditions_with_atleast_1000pts.csv  (stored in folder)               #
####################################################################################


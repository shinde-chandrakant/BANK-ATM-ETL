![spar-nord-bank](/src/assets/spar-nord-bank.png)  

## [Spar Nord Bank’s](https://www.bing.com/ck/a?!&&p=96ba0d5b63eab20aJmltdHM9MTY5MzY5OTIwMCZpZ3VpZD0wNDQ0NzA5Mi0yMDMyLTYwY2UtMTcwMy02M2NkMjE4MDYxODQmaW5zaWQ9NTY4Nw&ptn=3&hsh=3&fclid=04447092-2032-60ce-1703-63cd21806184&psq=spar+nord+danish+bank&u=a1aHR0cHM6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvU3Bhcl9Ob3Jk&ntb=1) ATM refill ETL Pipeline: A Data-Driven Approach to ATM Refill Management

### Problem Statement
Banks have to refill the ATMs when the money goes below a specific threshold limit. This depends on the activity and the area where a particular ATM is located as well as the weather, day of the week, etc.  

In This project, **Spar Nord Bank** is trying to observe the withdrawal behavior and the corresponding dependent factors to optimally manage the refill frequency. Apart from this, other insights also have to be drawn from the data.  

**Build a batch ETL pipeline** to read transactional data from RDS, transform and load it into target dimensions and facts on **Redshift Data Mart(Schema)**.  

---
**Danish ATM Transactions Data Set**  
This dataset comprises around 2.5 million records of withdrawal data along with weather information at the time of the transactions from around 113 ATMs from the year 2017.     
[Data dictionary](/src/assets/RDS+Data+dictionary.pdf)  
[Source of the Data Set in Kaggle](https://www.kaggle.com/sparnord/danish-atm-transactions)  

> This data set contains various types of transactional data as well as the weather data at the time of the transaction, such as:  
**Transaction Date and Time:** Year, month, day, weekday, hour  
**Status of the ATM:** Active or inactive  
**Details of the ATM:** ATM ID, manufacturer name along with location details such as longitude, latitude, street name, street number and zip code  
**The weather of the area near the ATM during the transaction:** Location of measurement such as longitude, latitude, city name along with the type of weather, temperature, pressure, wind speed, cloud and so on  
**Transaction details:** Card type, currency, transaction/service type, transaction amount and error message (if any) 

---
**Task:**  
-> Extracting the transactional data from a given MySQL RDS server to HDFS(EC2) instance using **Sqoop**.  
-> Transforming the transactional data according to the given target schema using **PySpark**.  
-> This transformed data is to be loaded to an **S3 bucket**.  
-> Creating the **Redshift** tables according to the [Dimensional Model](/src/assets/Schema+for+Dimensions+and+Fact.pdf).  
-> Loading the data from Amazon S3 to Redshift tables.  
-> Performing the analysis queries.  

---
**BI Analysis:**

1. Top 10 ATMs where most transactions are in the ’inactive’ state  
2. Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions  
3. Top 10 ATMs with the most number of transactions throughout the year  
4. Number of overall ATM transactions going inactive per month for each month  
5. Top 10 ATMs with the highest total amount withdrawn throughout the year  
6. Number of failed ATM transactions across various card types  
7. Top 10 records with the number of transactions ordered by the ATM_number, ATM_manufacturer, location, weekend_flag and then total_transaction_count, on weekdays and on weekends throughout the year  
8. Most active day in each ATMs from location "Vejgaard"  

---
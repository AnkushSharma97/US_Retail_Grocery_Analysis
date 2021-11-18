# US_Retail_Grocery_Analysis
## Introduction:-
This project is regarding the Data Processing and Data Analysis of one of the largest US Retail Grocery Industry  . It includes data processing using SPARK SQL to process the data present in Grocery's Data Lake. After processing the data we will use it for gaining insights and strategic business decision making so as to gain an edge over the competitors.

## Problem Statement:-
The retail grocery industry in the United States faces a precarious economic environment. Due primarily to competition from warehouse clubs, supercentres, and e-commerce, retail grocery sales have underperformed the U.S. retail sector and the overall U.S. economy, and employment growth in the industry has been stagnant. Yet, a large proportion of consumers maintain a strong preference for shopping at retail grocery stores, and total grocery industry sales and employment still exceed sales and employment at warehouse clubs/super-centres and e-commerce retailers. To compete in this setting, many retail grocers are turning to third-party online grocery delivery services offering online shopping and same-day grocery delivery, the largest of which is the current retail store.
One of the retail company and its team came up with a business problem in which after solving, can help the online grocery stores in managing their business to gain an edge over the market. The specific business problem is to drive higher sales volume and customer retention. The solution involved building

## Objective:-
As part of solution to above problem, we will build an ETL Pipeline as part of data engineering solution. We will clean the data, do some preprocessing on it as per the business requirement and finally we will load the data to destination (which is a datalake). In the end we will also do some analysis on the data which we have cleaned and saved in the datalake to see how it can be helpful for the business.

## Steps and important links:-

## A) ETL/ELT part:-

### Step - 1: Understanding Problem Statement and The Data given:
Initially let's look at the data provided to us in the Problem Statement. After that we will make an approach to achieve the goal. The data provided is as follows:-
#### 1.a) All the Data_Sets are present here:
   [Data_Sets](https://github.com/AnkushSharma97/US_Retail_Grocery_Analysis/tree/main/Project/Data_sets)
   All the datasets are in csv format.
 
#### 1.b) Data dicitionary:-
   [Data_Dicitionary](https://github.com/AnkushSharma97/US_Retail_Grocery_Analysis/tree/main/Data%20Dicitionary)
  
#### 1.c) Data Model:-
  [Data Model](https://github.com/AnkushSharma97/US_Retail_Grocery_Analysis/blob/main/Data%20Dicitionary/DataModel.PNG)
  
### Step - 2: Creating ETL Pipeline:-
Once we have understood the Problem Statement and the given data, the next step will be to create a Pipeline to Process and Transform the data.
##### Note:- 
From below code please modify all the paths as per your data locations.

#### Code:-
[ETL PipeLine Using Spark SQL](https://github.com/AnkushSharma97/US_Retail_Grocery_Analysis/blob/main/Project/Retail_store_etl_pipeline.ipynb)

#### 2.a) Extracting the data- Loading  unprocessed data from data lake:-
Firstly we will create a spark session and after that we will load all the required data from the data lake for data cleaning and  processing purposes.
All the required data sets can be found in the link already shared above.

#### 2.b) Transforming the data:- Doing all cleansing, processing and aggregations of the data as per business requirements:-
Once the extraction part is done then after that we will start processing the data. 
After analysing the data in step ! wwe came to know that there are too much noises in various files. So in this phase we will remove all those noises and clean the data.
Once data is cleaned, we will aggregate it using Spark SQl as per business requirements.

#### 2.c) Loading cleaned data back to the the data to Datalake:-
Once we are done with all required data processing, the final step will be to right all the data back tp specified location in the data lake so that it can be used for
Analysis purposes by the respective teams.
##### Note:-
The final processed data can be found at this location in this project:- [Final_Processed_Data](https://github.com/AnkushSharma97/US_Retail_Grocery_Analysis/tree/main/Project/Data_sets/output)

## B) Visualization part:-
Once we have cleaned the data now we will use that data for performing various analayis. 







<img  src="./readme/title1.svg"/>

<div>

> Hello world! This is the project’s summary that describes the project, plain and simple, limited to the space available.
**[PROJECT PHILOSOPHY](#project-philosophy) • [PROTOTYPING](#prototyping) • [IMPLEMENTATION](#demo) • [TECH STACKS](#stacks) • [HOW TO RUN?](#run)**

</div> 
  

><i> Disclamer: The analysis presented here is solely for educational purposes. The author maintains an unbiased perspective and has no religious or political affiliations influencing the interpretation of the data. Readers are encouraged to conduct their own research and seek professional advice before making any financial decisions. The author disclaims responsibility for the accuracy or completeness of the information and is not liable for actions taken based on the content of this analysis.</i>
<br><br>

<!-- project philosophy -->

<a  name="philosophy" ></a>
<img  src="./readme/title2.svg" id="project-philosophy"/>

> A Python based ETL project that would extract, transform data from different web sources and load them into a tabular database (PostgreSQL) in an attempt to provide a full scope analytical understanding of the USA recession. 


  

### User Types

1. Data Engineers.
2. Data Analysts.
3. Economists.
4. Policy Makers.
5. Journalist.
  
<br>

### User Stories

  
1. As a Data Engineer:  
	- I want to automatically scrape various economic indicators from reputable sources so that our dataset is always up-to-date.
	- I want to integrate different data sources seamlessly.
	- Ensure fault tolerance in our data pipelines, so that potential failures don't interrupt our analyses.
2. As an Analyst:  
	- I want to query the database.
	- I want to view the sentiment analysis results to understand public sentiment around economic conditions.
	- I want to visualize the data using PowerBI.
3. As an Economist:  
	- I want to access a dataset that provides a holistic view of the US economic landscape post-COVID-19.
4. As a Policy Maker:  
	- I want to understand if the US is still in recession to make informed decisions about economic policies.
	- I want to view reports highlighting key economic indicators and their trend over time.
5. As a Journalist:  
	- I want to access clear visualizations.
	- I want to understand public sentiments.
	- I want an access on the historical view of the economic indicators to showcase trends.


<br><br>

<!-- Prototyping -->
<a name="prototyping"></a>
<img  src="./readme/title3.svg"  id="prototyping"/>

> We have designed our projects to webscrape, through an ETL project and including it in a PowerBI Sample Dashboard, 

### AWS Lambda Logs

The ETL (Extract, Transform, Load) files, along with their dependencies, have been containerized using Docker and packaged into a container image. This Docker image has been used to create a deployment package for an AWS Lambda function. The deployment package, including the Lambda function code and dependencies, has been uploaded to AWS Lambda for serverless execution. 

ETL runtime duration on AWS: 12 minutes


| AWS Lambda Full Result |
|---|
|![aws_lambda_logs_part_1](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/fc91642c-4def-46df-baa0-5aea07fb8967)
![aws_lambda_logs_part_2](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/a93cecee-2e73-424f-9e90-459c0a7dd354)
|

| AWS Lambda Log Output |
|---|
|![AWS_lambda_logs](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/78db472d-b96d-4f88-88f1-ab9be2e42d54)|

### Logger File

ETL runtime duration locally: 10 minutes


| CLI logs |
|---|
|![cli_logs](./readme/logs/cli_logs.png)|


### Data Flow Diagrams

- Prehook:  
	- Initialize staging tables
	- Create UDFs
	- Create schema
- Hook:
	- Extract:
		- Politician speeches
		- U.S.A economic data
		- Stock market prices
	- Transform:
		- Initialize dimension, fact and aggregate tables
		- Analyze stocks news and politicians speeches sentiments
	- Load:
		- Initialize and load data into views
- Posthook:
	- Truncate staging tables

| ETL-Hook Cycle |
|---|
|![etl_hook](./readme/etl_hook_diagram/etl_hook.png)|



<br>
<br>

<!-- Implementation -->

<a  name="demo"></a>
<img  src="./readme/title4.svg" id="demo" />

<br>

### Dashboard Slides

| Landing Page |
| -----------------|
| ![landing_page](./readme/pbi/landing_page.png)|

| National Overview |
| -----------------|
| ![national_overview](./readme/pbi/national_overview.png)|

| State Wise Overview |
| -----------------|
| ![state_wise_overview_1](./readme/pbi/state_wise_overview_1.png)|

| FAANG Stocks Overview |
| -----------------|
| ![state_wise_overview_2](./readme/pbi/FAANG_stock_prices.png)|

### Dashboard - Quick Demo

https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/6d9124e2-3b75-4745-a990-db0f9193264d


### Data Extraction

#### Webscrape USA Economic Data

-	Leverage Selenium to automatically set the date into the ETL date, enabling the retrieval of all data from that specified date onward.
-	Retrieve the downloadable csv link and ingest its contents into the database  

<br>

| Webscrape USA economic data demo |
| --------------------|
|![webscrape usa economic data](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/4e9102ff-7fe4-4b35-8da4-c32ca7dc839c)|

#### Webscrape USA Ppolitician Speeches

-	Part 1: utilize Selenium to automatically access the main politician speeches page and scroll down until reaching the ETL date. 
-	Part 2: access the headlines one by one and webscrape the transcript text of the speeches.


| Webscrape USA politician speeches (part 1) | Webscrape USA politician speeches (part 2) |
| ---| ---|
| ![webscrape politicians speeches part 1](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/95d6cbbc-19d4-4bd0-ae6d-a2ff744ee875) | ![webscrape politicians speeches part 2](https://github.com/FawziElZein/usa_recession_analysis/assets/56543752/4bd65616-4d4d-44a4-ab14-2df3413b58a2)| 


<br><br>

<!-- Tech Stacks -->

<a  name="Stacks"  ></a>
<img  src="./readme/title5.svg" id="#stacks"/>

> Show command line of ETL performance - Logger view

  

Bin Tracker is built using the following technologies:


## Frontend

Interactive Power BI Dashboard:
A central dashboard where viewers can check:

1. Economic Indicators: Graphs, charts and visualizations displaying key economic metrics over time.
2. Sentiment Analysis: Representations of public sentiment about economic conditions through pie charts, and sentiment bars.
3. Predictive Analysis: A visualization of the ML model's performance about the US economic recovery compared to actual data.
4. Interactive filters: Options to filter data by date, region, or specific economic indicators for customized views.


## Backend

1. Web scraping & Automation.
2. ETL Pipeline: Using Python and pandas, raw data is extracted, transformed into a usable format and loaded into PostgreSQL database.
3. Database: Schema Design - Indexing - Data Integrity - Backup & Recovery.
4. Sentiment Analysis: using OpenAI's Large Language Model. 


<br><br>

<!-- How to run -->

<a  name="run"  ></a>
<img  src="./readme/title6.svg" id="run"/>
  

> To set up **USA Recession Analysis and Prediction** follow these steps:

### Prerequisites


**Hardware & Software**:

-   A computer/server with sufficient RAM and processing power.
-   Operating system: Linux (preferred for production) or Windows.
-   Required software: Python (3.x), PostgreSQL, Git (for version control)
  
**Dependencies**:

-   Install the necessary Python libraries: `psycopg2-binary`, `boto3`,`pandas`, `requests`, `urllib3`,`yahoofinancials`,`langchain`,`tqdm`,`python-dotenv`, `selenium`,`bs4`,`pytz`
-   Install database connectors/drivers for PostgreSQL.
  

### **Setting Up the Environment**:

**Clone the Repository**:


```sh

git clone https://github.com/FawziElZein/usa_recession_analysis

```

  
**Set Up the Database**:

-   Start the PostgreSQL server.
-   Create a new database and user with the appropriate permissions.

**Set Up Environment Variables**:
-	Create a new file in the project directory titled .env, which will include all the environment variables related to the ETL: DATABASE_NAME, HOST, PORT_NUMBER, USER, PASSWORD, and OPENAI_API_KEY. The example below illustrates the file format.  

	- DATABASE_NAME = "*usa_recession_db*"  
	- HOST= "*localhost*"  
	- PORT_NUMBER= *5432*  
	- USER=  "*postgres*"  
	- PASSWORD= "*your_password*"  
	- OPENAI_API_KEY= "*your_open_ai_api_key*"

### **Running the Backend**:

This ETL process has been containerized using Docker and packaged into a container image to facilitate the creation of a deployment package for an AWS Lambda function. For local testing purposes, please update the main file by adding a call to the lambda_handler() function at the end of it. The final version should look like the example below:

```
import hook
import prehook
import posthook

def lambda_handler(event=None, context=None):
    prehook.execute_prehook()
    hook.execute_hook()
    posthook.execute_posthook()

lambda_handler()

```


**Start the Data Ingestion & ETL Process**:
`python lambda_function.py`


You should be able to check the app.log file to see the ETL work.

As for the dashboard access, please use this [link](https://app.powerbi.com/view?r=eyJrIjoiNmI2ZDMzZDEtYjI3Ny00M2Y3LTg2NGItNmJmOTE2MWYzZWI2IiwidCI6IjJhZDk2OTM0LTQzZTUtNDFjMi05NzYxLWYzMzVmZTIxNGNjMyIsImMiOjl9) to access the data.

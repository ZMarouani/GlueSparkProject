# GlueSparkProject
Project using Aws glue with sparksql jobs, Athena , Aws lambda and Quicksight 

Architecture decisions and Implementation  : 
This architecture relies on serverlesse computing services :

-Aws Glue for the ETL workflow , using web crawlers , pyspark jobs and Glue triggers  , 
the choice of Glue over Aws Emr was to be able to achieve the job in less time because 
the serverless developement environement will allow you to only concentrate on the code and deployement of the
code to work . 

-Aws Lambda used to trigger start the Glue crawler to import data to the Glue data catalogue whenever 
new data is added to the S3 input bucket .

-The choice of S3 buckets instead of relational databases like postgres or mysql with RDS
is because S3 is much more cheaper an accept encryption , versionning , can be private , 
region independent .. which gives all the advantage to S3 since also we are not going to need to 
query from the dataset directly , but import then process it through the ETL .

-Athena is also serverless service that can be integrated with AWS Glue Data Catalog, 
allowing you to create a unified metadata repository across various services , it also 
because we need it to visualize the data later in Aws QuickSight .

-Aws QuickSight will allow to visualize the data queried with Athena or even directly from the S3 output bucket .

  
Execution :
- Uploading the .Csv data to gluejobcsvdatabucket/read/ will trigger the lambda function , which will start
The Glue workflow : The crawler will import the csv files to Glue data catalogue and after finishing
will start the Glue spark jobs (Glue spark jobs code is included in GlueJobs directory ) in parallele , 
the Glue jobs will save the outputs as json files to S3 bucket gluejobtasksoutput/write/ .
(The S3 buckets are for public access and without encryption just for the purpouse of this exercice .) 

Another glue crawler was also created  fetch data from the output bucket into the Glue data catalogue ,
and then we will visualize the data in Quicksight .

The charts photos are included in Quicksight-Visualization/ directory .

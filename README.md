INTRODUCTION
------------

Spark application for Hello fresh assignment to parse the json data and generate CSV file as ouput to calculate the average cooking time based on difficulty


REQUIREMENTS
------------

This spark application requires following python modules installed on cluster

argparse
json
logging
findspark
configparser
pyspark


CONFIGURATION
-------------

Please configure the file path for input json file, output csv file & log file in config.ini file. 

Note --> script will generate report.csv folder and create output file instead of output folder
 
Change the spark app name & provide path to setMaster in python script at line 75
conf = pyspark.SparkConf().setAppName('appName').setMaster('local')

Deploy & run the script on cluster as "spark-submit hellofresh_assignment.py"

MAINTAINERS
-----------

Current maintainers:
Mrugesh Patel 
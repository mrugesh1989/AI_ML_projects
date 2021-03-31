#!/usr/bin python3



import argparse
import json
import logging
import findspark
import configparser

findspark.init()
findspark.find()

import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType



class Hellofreshdataparser:

    def __init__(self, input_filepath, output_filepath):
        self.inputfile = input_filepath
        self.outputfile = output_filepath

    def hours_to_min(self, hours_in):
        # Method to parse the string and find the hour to convert into minutes
        try:
            logging.info('Parsing & converting cookTime, prepTime Columns')
            if len(hours_in) < 6:
                input1 = hours_in[2:]
                if input1[-1] == 'M':
                    return int(input1[:-1])
                else:
                    logging.info('Parsing & conversion successfull')
                    return (int(input1[:-1]) * 60)
            else:
                input1 = hours_in[2:]
                hours = int(input1[0:1]) * 60
                minutes = int(input1[2:-1])
                logging.info('Parsing & conversion successful')
                return (hours + minutes)

        except Exception as e:
            logging.info("Parsing & conversion failed for cookTime, prepTime Columns")
            logging.error("Error occurred :" + str(e))

    def sum(self, val1, val2):
        """ Method to calculate total cooking time """
        try:
            logging.info('Calculating total cooking time')
            return (val1 + val2)
        except Exception as e:
            logging.info('Can not calculate total cooking time')
            logging.error('Error occurred ' + str(e))

    def calc_difficulty(self, val):
        """ Method to check the difficulty based on total cook time """
        try:
            logging.info('Calculating difficulty based on total cooking time')
            if val < 30:
                return "easy"
            elif (val >= 30 and val <= 60):
                return "medium"
            else:
                return "hard"
        except Exception as err:
            logging.info('Can not calculate difficulty')
            logging.error('Error occurred ' + str(err))

    def main(self):
    
        conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
        sc = pyspark.SparkContext(conf=conf)
        spark = SparkSession(sc)
        """read the file and turn each line into an element of the RDD """
        
        logging.info("Load jsonline file as text in spark dataframe")
        try:
            raw_data = sc.textFile(self.inputfile)
                    #"C:\\Users\\mruge\\OneDrive\\Desktop\\hellofresh\\hellofresh.json")
        except IOError as e:
            logging.info("Error loading jsonl file")
            logging.error("Error occurred " + str(e))

        #Each element in RDD is single string json value. 
        #below step will convert into python dictionary to analyze more easily. 

        dataset = raw_data.map(json.loads)
        dataset.persist()

        # Each element in the RDD dataset is now a dictionary mapping keys to values or in valid json format to create rdd.
        rdd = spark.read.json(dataset)

        #Filter rdd for beef ingredients
        rdd_filter_beef = rdd.filter("ingredients like '%Beef%'")
        #rdd_filter_beef.select("cookTime", "prepTime").show(50)

        # Spark UDF functions to convert string of cookTime and prepTime into Minutes, to count total cook time & difficulty
        hours_to_min_udf = udf(self.hours_to_min, IntegerType())
        sum_cols = udf(self.sum, IntegerType())
        difficulty = udf(self.calc_difficulty)

        #RDD tranformations with UDF functions
        rdd_parse_cooktime = rdd_filter_beef.withColumn("cookTimeUpdated", hours_to_min_udf("cookTime"))
        rdd_parse_preptime = rdd_parse_cooktime.withColumn("prepTimeUpdated", hours_to_min_udf("prepTime"))

        #calculate Total cook time & difficulty
        rdd_calc_totalcooktime = rdd_parse_preptime.withColumn("total_cook_time", sum_cols("cookTimeUpdated", "prepTimeUpdated"))
        rdd_calc_difficulty = rdd_calc_totalcooktime.withColumn("difficulty", difficulty("total_cook_time"))
        #rdd_calc_difficulty.select("cookTimeUpdated", "cookTime", "prepTimeUpdated", "prepTime", "total_cook_time", "difficulty").show(50)

        #Find the average for each difficulty and write it to csv
        rdd_calc_avg_cooktime = rdd_calc_difficulty.groupBy("difficulty").agg({'total_cook_time':'avg'})
        output = rdd_calc_avg_cooktime.select(col("difficulty").alias("difficulty"), col("avg(total_cook_time)").alias("avg_total_cooking_time"))
        output.show()
        output.printSchema()


        # write output to the csv file
        try:
            logging.info("Write output RDD to CSV")
            output.coalesce(1).write.format('com.databricks.spark.csv').option('header', True).save(self.outputfile)
            logging.info("Output CSV generated")
                    #'C:\\Users\\mruge\\OneDrive\\Desktop\\report.csv')
        except Exception as e:
            logging.info("Can not create csv !!!, please check logs for error.")
            logging.error("Error occurred: " + str(e))
            
        sc.stop()
        

if __name__ == "__main__":
    
    config = configparser.ConfigParser()
    config.read('C:\\Users\\mruge\\OneDrive\\Desktop\\hellofresh\\config.ini')
    default_input_file = config['hellofresh']['default_input_file']
    default_output_file = config['hellofresh']['default_output_file']
    FILE_NAME = config['hellofresh']['FILE_NAME']
    
    #configure logging

    logging.basicConfig(filename=FILE_NAME, level="INFO", format="%(asctime)s::%(levelname)s::%(message)s")

    parser_arg = argparse.ArgumentParser()
    parser_arg.add_argument("--input_file_path", "-ifile", type=str, default=default_input_file, \
                            help="Input json file path to parse")
    parser_arg.add_argument("--output_file_path", "-ofile", type=str, default=default_output_file, \
                                        help="Output json file path to parse")

    args = parser_arg.parse_args()

    hellofresh_data_parser = Hellofreshdataparser(args.input_file_path, args.output_file_path)
    hellofresh_data_parser.main()
    

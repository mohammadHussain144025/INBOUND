from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, lit, when, current_timestamp,concat, concat_ws,substring, hash, sha1
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, DecimalType, BooleanType
import time
import logging
from datetime import datetime
import paramiko
import os
import hashlib
import psycopg2
import os
from dotenv import load_dotenv
import gzip
import shutil
import gnupg

 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine which .env file to load based on the environment
environment = os.getenv('ENVIRONMENT', 'local')  # Default to local if not set
env_file = f'.env.{environment}'

# Load environment variables from the appropriate .env file
load_dotenv(dotenv_path=env_file)

# Retrieve environment variables
spark_app_name = os.environ.get('SPARK_APP_NAME')
spark_jars = os.environ.get('SPARK_JARS')
spark_driver_memory = os.environ.get('SPARK_DRIVER_MEMORY')
db_url = os.environ.get('DB_URL')
db_driver = os.environ.get('DB_DRIVER')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host=os.environ.get('DB_HOST')
db_port=os.environ.get('DB_PORT')
db_name=os.environ.get('DB_NAME')

sftp_host = os.environ.get('SFTP_HOST')
sftp_port = os.environ.get('SFTP_PORT')
sftp_username = os.environ.get('SFTP_USERNAME')
pem_file_location = os.environ.get('PEM_FILE_LOCATION')
private_key_passphrase = os.environ.get('PRIVATE_KEY_PASSPHRASE')
remote_directory = os.environ.get('REMOTE_DIRECTORY')
local_download_path = os.environ.get('LOCAL_DOWNLOAD_PATH')
output_csv_file = os.environ.get('OUTPUT_CSV_FILE')
private_key_file = os.environ.get('PRIVATE_KEY_FILE')
passphrase = os.environ.get('PASSPHRASE')

file_mapping = {
    "AUS": "AUSPART_2024"  
}

inbound_spark_management = {
    "file_name": "",           # The name of the file being processed
    "table_name": "part",  # The name of the table being affected
    "region": "",              # The region associated with the file (if applicable)
    "csv_count": 0,            # The number of rows in the CSV file
    "data_insert_count": 0,    # The number of rows inserted into the table
    "data_update_count": 0,    # The number of rows updated in the table
    "status": "",              # Overall status of the process (e.g., "Success", "Failed")
    "created_date": datetime.now(),  # Timestamp of when the process was started or the record was created
    "file_code": "",           # A unique code or hash representing the file for tracking
    "error_message": "",       # Detailed error message if an error occurs
    "success_message":"",        #Log the Succes Message 
    "start_time": datetime.now(),    # Timestamp when the process started
    "end_time": None,          # Timestamp when the process ended (if applicable)
    "duration": 0              # Duration of the process in seconds
}

def log_time(start_time, str):
    end_time = time.time()
    time_taken = end_time - start_time
    logger.info(f"{str} process took {time_taken:.2f} seconds")

schema_dict = {
    "id": IntegerType(),
    "name": StringType(),
    "dwatt_1": IntegerType(),
    "dwatt_3": IntegerType(),
    "endprodt": TimestampType(),
    "part_num": StringType(),
    "code": StringType(),
    "unpck": IntegerType(),
    "new_reman": StringType(),
    "flo_wgt": IntegerType(),
    "cat_code": StringType(),
    "cr_dt": TimestampType(),
    "cr_by": StringType(),
    "yroutprod": IntegerType(),
    "srvce_ex": StringType(),
    "updated_by": StringType(),
    "updated_date": TimestampType(),
    "validationstatus": StringType(),
    "is_valid": BooleanType(),
    "created_date": TimestampType(),
    "created_by": StringType(),
    "comment": StringType()
}
    
def simplify_colum_with_table(df):
    try:
        # Drop rows where 'code' is null
        df = df.filter(col('part_num').isNotNull())
        df = df.dropDuplicates(['part_num'])
        logger.info("Rows with null 'part_num' dropped and duplicates based on 'part_num' removed")
        return df
    except Exception as e:
        logger.error(f"Error in simplify_colum_with_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in simplify_colum_with_table method: {e}'
        # Handling locally, not propagating the exception
        return df

def connect_sftp():
    try:
        # Load the private key
        private_key = paramiko.RSAKey.from_private_key_file(pem_file_location, password=private_key_passphrase)

        # Initialize the Transport object
        transport = paramiko.Transport((sftp_host, int(sftp_port)))

        # Connect to the SFTP server
        transport.connect(username=sftp_username, pkey=private_key)

        # Create an SFTP client from the transport object
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info("SFTP connection established.")

        check_and_download_file(sftp)
    except Exception as e:
        logger.error(f"Failed to establish SFTP connection: {e}")
        inbound_spark_management["error_message"] = f'Failed to establish SFTP connection: {e}'
        inbound_spark_management["status"] = 'Failed'
        insertAuditTable(inbound_spark_management)
    finally:
        if transport:
            transport.close()

def check_and_download_file(sftp):
    if sftp is None:
        inbound_spark_management["status"] = 'Failed'
        inbound_spark_management["error_message"] = 'Error in the SFTP connection: SFTP is None'
        return
    
    spark = None
    file_present=False  
    try:
        spark = SparkSession.builder \
            .appName(spark_app_name) \
            .config("spark.jars", spark_jars) \
            .config("spark.driver.memory", spark_driver_memory) \
            .getOrCreate()

        properties = {
            "driver": db_driver,
            "user": db_user,
            "password": db_password
        }

        current_date = datetime.now().strftime('%Y%m%d')

        for region, filename_prefix in file_mapping.items():
            inbound_spark_management['region'] = region
            remote_region_dir = f"{remote_directory}/{region}"
            logger.info(f"Checking directory: {remote_region_dir} for files with prefix: {filename_prefix} and date: {current_date}")

            try:
                files = sftp.listdir(remote_region_dir)
                for file in files:
                    logger.info(f"Checking file: {file}")
                    if file.startswith(filename_prefix) and current_date in file:
                        file_present=True
                        inbound_spark_management['file_name'] = file
                        inbound_spark_management['file_code'] = hashlib.sha256(file.encode()).hexdigest()
                        remote_file_path = f"{remote_region_dir}/{file}"
                        local_file_path = f"{local_download_path}_{file}"
                        sftp.get(remote_file_path, local_file_path)
                        logger.info(f"Downloaded file: {remote_file_path} to {local_file_path}")

                        try:
                            start_time = datetime.now()
                            base_filename = file[:-8]  # Extract base filename without extension
                            output_csv_path = os.path.join(output_csv_file, base_filename + ".csv")
                            decrypt_file_and_extract_csv(local_file_path, output_csv_path, private_key_file, passphrase)
                            main(output_csv_path, spark, properties, db_url)
                            part_table_step_2_insert_into_main(spark,properties,db_url)
                            part_table_step_3_update_into_main()
                            end_time = datetime.now()
                            duration = end_time - start_time
                            inbound_spark_management["start_time"] = start_time.strftime('%Y-%m-%d %H:%M:%S')
                            inbound_spark_management["end_time"] = end_time.strftime('%Y-%m-%d %H:%M:%S')
                            inbound_spark_management["duration"] = str(duration)
                            inbound_spark_management["success_message"] = 'File Successfully Uploaded To Database'
                            inbound_spark_management["status"] = 'Success'
                            insertAuditTable(inbound_spark_management)
                            logger.info("--------------------------Full Process Complete Succesfully----------------------------")  
                        except Exception as e:
                            logger.error(f"An error occurred: {str(e)}")
                            inbound_spark_management["error_message"] = f'Error while executing main method: {e}'
                            inbound_spark_management["status"] = 'Failed'
                            insertAuditTable(inbound_spark_management)
            except Exception as e:
                logger.error(f"Error listing files in directory {remote_region_dir}: {e}")
                inbound_spark_management["error_message"] = f'Error listing files in directory {remote_region_dir}: {e}'
                inbound_spark_management["status"] = 'Failed'
                insertAuditTable(inbound_spark_management)
                logger.info("--------------------------Full Process Complete Succesfully----------------------------")  

    except Exception as e:
        logger.error(f"Error while executing check_and_download_file: {e}")
        inbound_spark_management["error_message"] = f'Error while executing check_and_download_file method: {e}'
        inbound_spark_management["status"] = 'Failed'
        insertAuditTable(inbound_spark_management)
        if spark:
            spark.stop()
    finally:
        if sftp:
            sftp.close()
        try:    
            if spark:
             spark.stop()
        except:
            pass
        finally:    
            if file_present:  
             if os.path.exists(local_file_path):
                os.remove(local_file_path)
                logger.info(f"Deleted the local file: {local_file_path}")
                     
def decrypt_file_and_extract_csv(encrypted_file, output_csv_file, private_key_file, passphrase):
    # Temporary file for GZIP output
    decrypted_gzip_file = output_csv_file + '.gz'

    # Initialize the GPG object, specifying the path to the gpg executable
    gpg = gnupg.GPG(gpgbinary=r'C:\Program Files (x86)\gnupg\bin\gpg.exe')

    # Step 1: Import the private key
    with open(private_key_file, 'r') as key_file:
        key_data = key_file.read()
        import_result = gpg.import_keys(key_data)
        
        # Check if the key was successfully imported
        if import_result.count == 0:
            print("Failed to import the private key.")
            return False
        print("Key import result:", import_result)

    # Ensure the output directory exists
    output_dir = os.path.dirname(output_csv_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Step 2: Decrypt the file to a GZIP archive
    with open(encrypted_file, 'rb') as f:
        decrypted_data = gpg.decrypt_file(f, passphrase=passphrase, output=decrypted_gzip_file, always_trust=True)

    if decrypted_data.ok:
        # Extract CSV from GZIP
        with gzip.open(decrypted_gzip_file, 'rb') as gz_file:
            with open(output_csv_file, 'wb') as csv_file:
                shutil.copyfileobj(gz_file, csv_file)

        # Remove temporary GZIP file
        os.remove(decrypted_gzip_file)

        print(f"Decryption and extraction successful: {output_csv_file}")
        return True
    else:
        print("Decryption failed.")
        print(f"Status: {decrypted_data.status}")
        print(f"Stderr: {decrypted_data.stderr}")
        return False       

def read_csv_data(path_to_csv, spark):
    try:
        df = spark.read.csv(path_to_csv, header=True, sep=',')
        logger.info(f"CSV data loaded from {path_to_csv}")
        
        df = df.toDF(*(c.lower() for c in df.columns))

        for column_name ,data_type  in schema_dict.items():
            if column_name in df.columns:
                df = df.withColumn(column_name,col(column_name).cast(data_type))

        csv_count = df.count()
        inbound_spark_management["csv_count"] = csv_count
        logger.info(f"CSV row count: {csv_count}")

        df = simplify_colum_with_table(df)
        return df
    except Exception as e:
        logger.error(f"Error in read_csv_data: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in read_csv_data method: {e}'
        raise

def data_to_insert_stagging_table(csv, hash_table_db, url, properties):
    try:
        missing_ids_csv = csv.join(hash_table_db, "code", "left_anti")
        logger.info(f"data to be insert: {missing_ids_csv.count()}")

        if missing_ids_csv.count() == 0:
            return
        
        # insert staging table
        csv_to_insert_stage = missing_ids_csv.drop('hash_value','check_sum')
        
        start_time = time.time()
        csv_to_insert_stage = csv_to_insert_stage.drop('code')
        csv_to_insert_stage.write.jdbc(url=url, table="part_insert_staging", mode="append", properties=properties)
        log_time(start_time=start_time,str="csv to insert stage")

        # insert hash table
        missing_code_to_hash_table = missing_ids_csv.select('code', 'hash_value','check_sum')
        start_hash_time = time.time()
        missing_code_to_hash_table.write.jdbc(url=url, table="part_hash", mode="append", properties=properties)
        log_time(start_time=start_hash_time, str="csv hash to hash stage")

    except Exception as e:
        logger.error(f"Error in data_to_insert_stagging_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'

def data_to_update_stagging_table(csv, hash_table_db, url, properties):
    try:
        code_matching = hash_table_db.join(csv,"code","inner")
        code_matching = code_matching.drop(hash_table_db['hash_value'])
        code_matching = code_matching.drop(hash_table_db['check_sum'])
        code_matching = code_matching.drop(hash_table_db['id'])
        print("code_matching Count BeforePerfprming Join  In hash table with hash_value -----------",code_matching.count())
        updated_hash_csv = code_matching.join(hash_table_db, "hash_value", "left_anti" )
        print("updated_hash_csv Count Before enting insert Method -----------",updated_hash_csv.count())
        
        if updated_hash_csv.count() == 0:
            return

        # insert update staging table
        csv_to_update_stage = updated_hash_csv.drop('hash_value', 'check_sum')
        start_time = time.time()
        csv_to_update_stage = csv_to_update_stage.drop('code')
        csv_to_update_stage.write.jdbc(url=url, table="part_update_staging", mode="append", properties=properties)    
        log_time(start_time=start_time, str="csv to update stage")

        # insert hash temp table
        mismatch_hash_to_hash_temp = updated_hash_csv.select("code", "hash_value", "check_sum")
        start_hash_time = time.time()
        mismatch_hash_to_hash_temp.write.jdbc(url=url, table="part_hash_staging", mode="append", properties=properties)    
        log_time(start_time=start_hash_time, str="csv hash to hash stage")
    except Exception as e:
        logger.error(f"Error in data_to_update_stagging_table: {e}")

def match_csv_and_x_digit(csvdf, xdigitdf, columnNameToAddWithMatchDf):
    try:
        match_df = xdigitdf.join(csvdf, "code", "inner")
        match_df = match_df.filter(col("code").isNotNull())
        match_df = match_df.withColumn(columnNameToAddWithMatchDf, col("id"))
        drop_columns = ['id', 'name', 'updated_by', 'updated_date', 'validationstatus', 'is_valid', 'created_date', 'created_by', 'comment']
        match_df = match_df.drop(*drop_columns)
        match_df = match_df.distinct()
        return match_df
    except Exception as e:
        logger.error(f"Error in match_csv_and_part: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in match_csv_and_part: {e}'

def compare_code_csv_with_x1digit(csvdf, x1digitdf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('dwatt_1')).filter(col('dwatt_1').isNotNull())
        unmatched_csv = csvdf.join(x1digitdf, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("admin"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("admin"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )

        logger.info(f"__________ Rows Count in Rule 1 Before drop Duplicate based on dwatt_1: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_dw_1_digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_dw_1_digit Without null: {selected_cols_unmatched.count()}")
    
        selected_cols_unmatched.write.jdbc(url=url, table="x_dw_1_digit", mode="append", properties=properties)
        newx1digitdf = spark.read.jdbc(url=url, table="x_dw_1_digit", properties=properties)

        match_df = match_csv_and_x_digit(csvdf, newx1digitdf, columnNameToAddWithMatchDf="dwatt_1")
        match_df.drop('code')
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_dw_1_digit: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_dw_1_digit: {e}'

def compare_code_csv_with_x3digit(csvdf, xdigitdf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('dwatt_3')).filter(col('dwatt_3').isNotNull())
        unmatched_csv = csvdf.join(xdigitdf, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("admin"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("admin"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )

        logger.info(f"__________ Rows Count in Rule 2 Before drop Duplicate based on dwatt_3: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_dw_3_digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_dw_3_digit Without null: {selected_cols_unmatched.count()}")
    
        selected_cols_unmatched.write.jdbc(url=url, table="x_dw_3_digit", mode="append", properties=properties)
        newx1digitdf = spark.read.jdbc(url=url, table="x_dw_3_digit", properties=properties)

        match_df = match_csv_and_x_digit(csvdf, newx1digitdf, columnNameToAddWithMatchDf="dwatt_3")
        match_df.drop('code')
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_dw_3_digit: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_dw_3_digit: {e}'

def compare_code_csv_with_pservice_exchange(csvdf, pservicedf, spark, url, properties):
    try:
        if pservicedf.count() == 0:
            csvdf=csvdf.withColumn("srvce_ex",lit(" "))
            return csvdf
        pservicedf = pservicedf.select("code", "name")
        # Join CSV dataframe with x_service_exchange dataframe on 'code'
        matched_df = csvdf.join(pservicedf, "code", "inner")
        if matched_df.count() == 0:
            csvdf=csvdf.withColumn("srvce_ex",lit(" "))
            return csvdf
        # If the code exists in x_service_exchange, set 'srvce_ex' to the name, otherwise set it to a space
        matched_df = matched_df.withColumn("srvce_ex", when(pservicedf["code"].isNull(), lit(" ")).otherwise(pservicedf["name"]))
        matched_df = matched_df.drop(pservicedf["name"])
        
        # Check if matched_df is empty
        if matched_df.count() == 0:
            return csvdf
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_p_service_exchange: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_p_service_exchange: {e}'

def compare_code_csv_with_p_last_production_date(csvdf, xproductiondf, spark, url, properties):
    try:
    
        if xproductiondf.count() == 0:
            csvdf = csvdf.withColumn("endprodt", lit("2023-05-13").cast("timestamp"))
            csvdf = csvdf.withColumn("yroutprod", (year(current_timestamp()) - year(col("endprodt"))).cast("long") )
            return csvdf
        xproductiondf = xproductiondf.select("code", "name")

        # Join csvdf with xproductiondf_renamed on 'code'
        matched_df = csvdf.join(xproductiondf, "code", "inner")
        if matched_df.count() == 0:
            csvdf = csvdf.withColumn("endprodt", lit("2023-05-13").cast("timestamp"))
            csvdf = csvdf.withColumn("yroutprod", (year(current_timestamp()) - year(col("endprodt"))).cast("long") )
            return csvdf

        # Set 'endprodt' column: If 'plast_code' in xproductiondf_renamed is null, set 'endprodt' to null; otherwise, set it to 'name'
        matched_df = matched_df.withColumn("endprodt", when(xproductiondf["code"].isNull(), lit(None)).otherwise(xproductiondf["name"]))

        # Calculate the difference in years between the current year and the year of 'endprodt'
        matched_df = matched_df.withColumn("yroutprod", year(current_timestamp()) - year("endprodt"))
        
        matched_df = matched_df.drop(xproductiondf["name"])

        print("compare_code_csv_with_p_last_production_date Column names in csvdf:", matched_df.columns)
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_p_last_production_date: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_p_last_production_date: {e}'

def main(local_file_path,spark,properties,url):

    try:
        start_process_time = time.time()
        logging.info('-----------------------starting data migration------------------------------')
        log_start_time = time.time()

        csv = read_csv_data(path_to_csv=local_file_path, spark=spark)
        log_time(log_start_time,"Time Taken in First Stage Wich is Reading data FRom Csv and Casting to Proper dataType")
        csv_count = csv.count()
        logger.info(f"readCsv rows count: {csv_count}")
        
        log_time_start = time.time()
        x1digit = spark.read.jdbc(url=url, table="x_dw_1_digit", properties=properties)
        csv = compare_code_csv_with_x1digit(csvdf=csv, x1digitdf=x1digit, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "------------------Time Taken To for Implementing Rule-1 x_dw_1_digit Table---------------")
        print("CSV Count after rule 1.-----------",csv.count())

        log_time_start = time.time()
        xdigit = spark.read.jdbc(url=url, table="x_dw_3_digit", properties=properties)
        csv = compare_code_csv_with_x3digit(csvdf=csv, xdigitdf=xdigit, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "------------------Time Taken To for Implementing Rule-2 x_dw_3_digit Table---------------")
        print("CSV Count after rule 2.-----------",csv.count())
        
        log_time_start = time.time()
        pservice = spark.read.jdbc(url=url, table="p_service_exchange", properties=properties)
        csv = compare_code_csv_with_pservice_exchange(csvdf=csv, pservicedf=pservice, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "------------------Time Taken To for Implementing Rule-3 p_service_exchange Table---------------")
        print("CSV Count after rule 3.-----------",csv.count())
        
        log_time_start = time.time()
        xproduction = spark.read.jdbc(url=url, table="p_last_production_date", properties=properties)
        csv = compare_code_csv_with_p_last_production_date(csvdf=csv, xproductiondf=xproduction, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "------------------Time Taken To for Implementing Rule-4 p_last_production_date Table---------------")
        print("CSV Count after rule 4.-----------",csv.count())

        csv = csv.withColumn('code', col('part_num'))\
                 .withColumn('updated_by', lit("admin"))\
                 .withColumn('updated_date', lit(datetime.now()))\
                 .withColumn('validationstatus', lit(" "))\
                 .withColumn('is_valid', lit(False))\
                 .withColumn('created_date', lit(datetime.now()))\
                 .withColumn('created_by', lit("admin"))\
                 .withColumn('comment', lit(" "))\
                 .filter(col('code').isNotNull())

       # Assuming 'csv' is your DataFrame
        columns_to_concat = [col for col in csv.columns if col not in ['created_date', 'updated_date']]
        csv = csv.withColumn("concatenated", concat_ws("", *columns_to_concat))
        csv = csv.withColumn("concatenated", csv['concatenated'].cast("string"))
        csv = csv.withColumn("hash_value", sha1(csv['concatenated']))
        csv = csv.withColumn("check_sum", hash(csv['concatenated']))
        csv = csv.drop("concatenated")

        insert_time = time.time()
        logger.info("Insertion process start")
        logger.info(f"CSV Count Before entering insert Method: {csv.count()}")
        csv = csv.withColumn('code', col('part_num'))
        hash_table_db = spark.read.jdbc(url=url, table="part_hash", properties=properties)
        data_to_insert_stagging_table(csv, hash_table_db, url, properties)
        print("____ insertion process end ______")
        log_time(insert_time, "Time Taken for inserting data into table")
        logger.info(f"CSV Count After insert Method: {csv.count()}")

        insert_time = time.time()
        logger.info("Update Table process start")
        data_to_update_stagging_table(csv, hash_table_db, url, properties)
        log_time(insert_time, "Time Taken for Update Method")
    except Exception as e:
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'
        logger.error(f"Error in main method: {e}")

def part_table_step_2_insert_into_main(spark, properties, db_url):
    connection = None
    try:
        # psycopg2 Property
        logger.info("Step 2 Started . method NAme part_table_step_2_insert_into_main")
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()
        
        # Load data from source table into a DataFrame
        start_time_main = time.time()
        inbound_spark_management["start_time"] = datetime.now()
        start_time = time.time()
        
        logger.info("Loading data from source table into a DataFrame...")
        sourceDF = spark.read.jdbc(db_url, table="part_insert_staging", properties=properties)
        log_time(start_time, "Time Taken To Load The data")
        source_count = sourceDF.count()
        logger.info(f"Total Rows Count = {source_count}")
        
        logger.info("Writing data into the target table...")
        columns_to_insert = [col for col in sourceDF.columns if col not in ['id', 'code']]
        sourceDF.select(columns_to_insert).write.jdbc(db_url, table="part", mode="append", properties=properties)
        
        # Insert data count into the logging dictionary
        inbound_spark_management["data_insert_count"] = sourceDF.count()
        
        # Truncate staging table
        cursor.execute("TRUNCATE TABLE part_insert_staging")
        connection.commit()
        logger.info("Staging table truncated successfully.")
        
        log_time(start_time_main, "-----------Step Two Loading data from insert Staging to main Table---------------------")
        
        # Log success message
        inbound_spark_management["status"] = "Success"
        inbound_spark_management["success_message"] = "part_table_step_2_insert_into_main executed successfully"
    except Exception as e:
        logger.error(f"Error in part_table_step_2_insert_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def part_table_step_3_update_into_main():
    connection = None
    try:
        logger.info("----------------Step 3 Started Method Name part_table_step_3_update_into_main ")
        # psycopg2 Property
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        start_time_main = time.time()

        sql_update_query  = """
            BEGIN;
            UPDATE part
            SET
                name = sourceTable.name,
                dwatt_1 = sourceTable.dwatt_1,
                dwatt_3 = sourceTable.dwatt_3,
                endprodt = sourceTable.endprodt,
                part_num = sourceTable.part_num,
                unpck = sourceTable.unpck,
                new_reman = sourceTable.new_reman,
                flo_wgt = sourceTable.flo_wgt,
                cat_code = sourceTable.cat_code,
                cr_dt = sourceTable.cr_dt,
                cr_by = sourceTable.cr_by,
                yroutprod = sourceTable.yroutprod,
                srvce_ex = sourceTable.srvce_ex,
                updated_by = sourceTable.updated_by,
                updated_date = sourceTable.updated_date,
                validationstatus = sourceTable.validationstatus,
                is_valid = sourceTable.is_valid,
                created_date = sourceTable.created_date,
                created_by = sourceTable.created_by,
                comment = sourceTable.comment
            FROM 
                part_update_staging AS sourceTable
            WHERE
                part.part_num = sourceTable.part_num;

            TRUNCATE TABLE part_update_staging;

            UPDATE part_hash  
            SET
                hash_value = temp.hash_value
            FROM
                part_hash_staging temp
            WHERE
                part_hash.code = temp.code;

            TRUNCATE TABLE part_hash_staging;

            COMMIT;
        """

        # Execute the SQL update query
        logger.info("Executing SQL update query...")
        start_time = time.time()
        cursor.execute(sql_update_query)
        connection.commit()
        log_time(start_time, "SQL update query executed in")
        
        log_time(start_time_main, "------------Step Thre Updating the Data From Staging Update to main Table Completed ----------")
    except Exception as e:
        logger.error(f"Error in part_table_step_3_update_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = f'Error in part_table_step_3_update_into_main: {e}'
    finally:
        if connection:
            cursor.close()
            connection.close()

def insertAuditTable(data_table_values):
   
    connection = None
    try:
        logger.info("==============Logs Management Insertion Start ===================")
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        # Store data table values in key-value pairs
        key_value_pairs = ", ".join([f"'{key}': '{value}'" for key, value in data_table_values.items()])

        # Generate insert query
        insert_query = f"""
            INSERT INTO public.inbound_spark_management ({', '.join(data_table_values.keys())})
            VALUES ({', '.join([f"'{value}'" for value in data_table_values.values()])})
        """
        # Execute insert query
        logger.info("Executing insert query...")
        cursor.execute(insert_query)
        connection.commit()
        logger.info("==============Logs Management Insertion Completed ===================")
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Error inserting data: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == '__main__':
    connect_sftp()
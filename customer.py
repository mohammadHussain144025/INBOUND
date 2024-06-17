from pyspark.sql import SparkSession
import time
import logging
from pyspark.sql.functions import col, lit, concat_ws, sha1,hash, substring
from pyspark.sql.types import IntegerType, StringType, BooleanType, DoubleType, TimestampType
from datetime import datetime 
import paramiko
import os
import hashlib
import psycopg2
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
    "AUS": "customer_as4us",
    "OCA": "OCACUST_2024"
}

inbound_spark_management = {
    "file_name": "",           # The name of the file being processed
    "table_name": "customer",  # The name of the table being affected
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
    "chainc": StringType(),
    "chnl": StringType(),
    "cbseg": StringType(),
    "grp": StringType(),
    "mngrp": StringType(),
    "subgrp_d": StringType(),
    "mngrp_d": StringType(),
    "shpto_d": StringType(),
    "sldto_d": StringType(),
    "chaind": StringType(),
    "chnldesc": StringType(),
    "grp_d": StringType(),
    "shpto": StringType(),
    "sldto": StringType(),
    "custid": StringType(),
    "name": StringType(),
    "custid_d": StringType(),
    "custsts": StringType(),
    "csubtype": StringType(),
    "ctype": StringType(),
    "export": StringType(),
    "fds": StringType(),
    "pdcflag": StringType(),
    "sales": StringType(),
    "bgrp": StringType(),
    "cprog": StringType(),
    "cdisccd": IntegerType(),
    "cdiscp": DoubleType(),
    "curr": StringType(),
    "subgrp": IntegerType(),
    "cusgrp": StringType(),
    "region": StringType(),
    "catcd": StringType(),
    "geo": StringType(),
    "xrefcd": StringType(),
    "housef": StringType(),
    "mkupf": StringType(),
    "mercf": StringType(),
    "discf": StringType(),
    "euccf": StringType(),
    "eucprcl": StringType(),
    "mmexcl": StringType(),
    "updated_by": StringType(),
    "updated_date": TimestampType(),
    "validationstatus": StringType(),
    "is_valid": BooleanType(),
    "created_date": TimestampType(),
    "created_by": StringType(),
    "comment": StringType(),
    "code": StringType()
}

def simplify_colum_with_table(df):
    try:
        # Drop rows where 'custid' is null
        df = df.filter(col('custid').isNotNull())
        df = df.dropDuplicates(['custid'])
        df = df.drop("sldtd")
        logger.info("Rows with null 'custid' dropped and duplicates based on 'custid' removed")
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
    local_file_path = None
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
                        customer_table_step_2_insert_into_main(spark)
                        customer_table_step_3_update_into_main()
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
        logger.error(f"Error while executing check_and_download_file: {e}")
        inbound_spark_management["error_message"] = f'Error while executing check_and_download_file method: {e}'
        inbound_spark_management["status"] = 'Failed'
        insertAuditTable(inbound_spark_management)
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

        # Adding columns for testing purposes
        df = df.withColumn("grp", lit("")) 
        df = df.withColumn("mngrp", lit(""))
        df = df.toDF(*(c.lower() for c in df.columns))
        
        # Casting columns to specified data types
        for column_name, data_type in schema_dict.items():
            if column_name in df.columns:
                df = df.withColumn(column_name, col(column_name).cast(data_type))
        
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
        
        # These data are wrong in csv later on we will remove it 
        csv_to_insert_stage = missing_ids_csv.drop('hash_value', 'check_sum', 'sldtoD', 'mmexcll', 'record_action')
        # csv_to_insert_stage.printSchema()
        
        start_time = time.time()
        csv_to_insert_stage = csv_to_insert_stage.drop('code')
        csv_to_insert_stage.write.jdbc(url=url, table="customer_insert_staging", mode="append", properties=properties)
        log_time(start_time=start_time, str="csv to insert stage")

        # Insert hash table
        missing_code_to_hash_table = missing_ids_csv.select('code', 'hash_value', 'check_sum')
        start_hash_time = time.time()
        missing_code_to_hash_table.write.jdbc(url=url, table="customer_hash", mode="append", properties=properties)
        log_time(start_time=start_hash_time, str="csv hash to hash stage")
        
        # log_to_sparj_management('SUCCESS', 'data_to_insert_stagging_table executed successfully')
    except Exception as e:
        logger.error(f"Error in data_to_insert_stagging_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'

def data_to_update_stagging_table(csv, hash_table_db, url, properties):
    try:
        code_matching = hash_table_db.join(csv, "code", "inner")
        code_matching = code_matching.drop(hash_table_db['hash_value'])
        code_matching = code_matching.drop(hash_table_db['check_sum'])
        code_matching = code_matching.drop(hash_table_db['id'])
        logger.info(f"code_matching Count Before Performing Join in hash table with hash_value: {code_matching.count()}")
        updated_hash_csv = code_matching.join(hash_table_db, "hash_value", "left_anti")
        logger.info(f"updated_hash_csv Count Before inserting into Update StagingTable: {updated_hash_csv.count()}")
        
        if updated_hash_csv.count() == 0:
            return

        # Insert update staging table
        csv_to_update_stage = updated_hash_csv.drop('hash_value', 'check_sum', 'sldtoD', 'mmexcll', 'record_action')
        start_time = time.time()
        csv_to_update_stage = csv_to_update_stage.drop('code')
        csv_to_update_stage.write.jdbc(url=url, table="customer_update_staging", mode="append", properties=properties)
        log_time(start_time=start_time, str="csv to update stage")

        # Insert hash temp table
        mismatch_hash_to_hash_temp = updated_hash_csv.select("code", "hash_value", 'check_sum')
        start_hash_time = time.time()
        mismatch_hash_to_hash_temp.write.jdbc(url=url, table="customer_hash_staging", mode="append", properties=properties)
        log_time(start_time=start_hash_time, str="csv hash to hash stage")
        
        # log_to_sparj_management('SUCCESS', 'data_to_update_stagging_table executed successfully')
    except Exception as e:
        logger.error(f"Error in data_to_update_stagging_table: {e}")
        # log_to_sparj_management('FAILURE', f'data_to_update_stagging_table failed: {e}')
        # Handling locally, not propagating the exception

def match_csv_and_customer(csvdf, newxchaindf, columnNameToAddWithMatchDf):
    try:
        matched_df = newxchaindf.join(csvdf, "code", "inner")
        matched_df = matched_df.withColumn(columnNameToAddWithMatchDf, newxchaindf["id"])
        drop_columns = ['id', 'name', 'updated_by', 'updated_date', 'validationstatus', 'is_valid', 'created_date', 'created_by', 'comment']
        matched_df = matched_df.drop(*drop_columns)
        matched_df = matched_df.distinct()
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in match_csv_and_customer: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in match_csv_and_customer: {e}'

def join_operation_with_x_chain(csvdf, xchaindf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('chainc'))
        unmatched_csv = csvdf.join(xchaindf, "code", "left_anti")
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

        logger.info(f"__________ Rows Count in Rule 1 Before drop Duplicate based on chainc: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x chain digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x chain digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_chain", mode="append", properties=properties)
        newxchaindf = spark.read.jdbc(url=url, table="x_chain", properties=properties)
        
        match_df = match_csv_and_customer(csvdf, newxchaindf, columnNameToAddWithMatchDf="chainc")
        matched_df = match_df.drop("code")
        
        # log_to_sparj_management('SUCCESS', 'join_operation_with_x_chain executed successfully')
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_chain: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_chain: {e}'

def join_operation_with_x_channel(csvdf, xchanneldf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('chnl'))
        unmatched_csv = csvdf.join(xchanneldf, "code", "left_anti")
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

        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_channel digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_channel digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_channel", mode="append", properties=properties)
        new_x_channel = spark.read.jdbc(url=url, table="x_channel", properties=properties)

        match_df = match_csv_and_customer(csvdf, new_x_channel, columnNameToAddWithMatchDf="chnl")
        matched_df = match_df.drop("code")
        
        # log_to_sparj_management('SUCCESS', 'join_operation_with_x_channel executed successfully')
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_channel: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_channel: {e}'

def join_operation_with_x_customer_group(csvdf, xcustomergroupdf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('grp'))
        unmatched_csv = csvdf.join(xcustomergroupdf, "code", "left_anti")
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

        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_customer_group digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_customer_group digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_customer_group", mode="append", properties=properties)
        new_x_customer_group = spark.read.jdbc(url=url, table="x_customer_group", properties=properties)

        match_df = match_csv_and_customer(csvdf, new_x_customer_group, columnNameToAddWithMatchDf="grp")
        matched_df = match_df.drop("code")

        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_customer_group: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_customer_group: {e}'

def join_operation_with_x_customer_main_group(csvdf, xcustomermaingroupdf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('mngrp'))
        unmatched_csv = csvdf.join(xcustomermaingroupdf, "code", "left_anti")
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

        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_customer_main_group digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_customer_main_group digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_customer_main_group", mode="append", properties=properties)
        new_x_customer_main_group = spark.read.jdbc(url=url, table="x_customer_main_group", properties=properties)

        match_df = match_csv_and_customer(csvdf, new_x_customer_main_group, columnNameToAddWithMatchDf="mngrp")
        matched_df = match_df.drop("code")
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_customer_main_group: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_customer_main_group: {e}'

def join_operation_with_x_ship_to(csvdf, xshiptodf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', substring(col('shpto'), 1, 6))
        unmatched_csv = csvdf.join(xshiptodf, "code", "left_anti")
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

        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_ship_to digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_ship_to digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_ship_to", mode="append", properties=properties)
        new_x_ship_to = spark.read.jdbc(url=url, table="x_ship_to", properties=properties)

        match_df = match_csv_and_customer(csvdf, new_x_ship_to, columnNameToAddWithMatchDf="shpto")
        matched_df = match_df.drop("code")
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_ship_to: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_ship_to: {e}'

def join_operation_with_x_sold_to(csvdf, xsoldtodf, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', substring(col('sldto'), 1, 6))
        unmatched_csv = csvdf.join(xsoldtodf, "code", "left_anti")
        
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

        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_sold_to digit: {selected_cols_unmatched.count()}")

        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_sold_to digit Without null: {selected_cols_unmatched.count()}")

        selected_cols_unmatched.write.jdbc(url=url, table="x_sold_to", mode="append", properties=properties)
        new_x_sold_to = spark.read.jdbc(url=url, table="x_sold_to", properties=properties)

        match_df = match_csv_and_customer(csvdf, new_x_sold_to, columnNameToAddWithMatchDf="sldto")
        matched_df = match_df.drop("code")
        
        return matched_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_sold_to: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_sold_to: {e}'

def main(local_file_path,spark,properties,url):
    try:
        # spark = SparkSession.builder \
        #     .appName("spriced poc") \
        #     .config("spark.jars", "postgresql-42.7.3.jar") \
        #     .config("spark.driver.memory", "16g") \
        #     .getOrCreate()
        
        # url = "jdbc:postgresql://localhost:5432/spriced_meritor_poc_30_may"
        # properties = {
        #     "driver": "org.postgresql.Driver",
        #     "user": "postgres",
        #     "password": "root"
        # }      

        start_process_time = time.time()
        logger.info('-----------------------starting data migration------------------------------')
        log_start_time = time.time()
        
        csv = read_csv_data(path_to_csv=local_file_path, spark=spark)
        log_time(log_start_time, "Time Taken in First Stage: Reading data from CSV and casting to proper data type")
        csv_count = csv.count()
        logger.info(f"readCsv rows count: {csv_count}")

        #inbound_spark_management['csv_count'] = csv_count

        # Rule 1
        log_time_start = time.time()
        x_chain = spark.read.jdbc(url=url, table="x_chain", properties=properties)
        csv = join_operation_with_x_chain(csvdf=csv, xchaindf=x_chain, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-1: x_chain Table")
        logger.info(f"CSV Count after rule 1: {csv.count()}")

        # Rule 2
        log_time_start = time.time()
        x_channel = spark.read.jdbc(url=url, table="x_channel", properties=properties)
        csv = join_operation_with_x_channel(csvdf=csv, xchanneldf=x_channel, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-2: x_channel Table")
        logger.info(f"CSV Count after rule 2: {csv.count()}")

        # Rule 3
        log_time_start = time.time()
        x_customer_group = spark.read.jdbc(url=url, table="x_customer_group", properties=properties)
        csv = join_operation_with_x_customer_group(csvdf=csv, xcustomergroupdf=x_customer_group, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-3: x_customer_group Table")
        logger.info(f"CSV Count after rule 3: {csv.count()}")

        # Rule 4
        log_time_start = time.time()
        x_customer_main_group = spark.read.jdbc(url=url, table="x_customer_main_group", properties=properties)
        csv = join_operation_with_x_customer_main_group(csvdf=csv, xcustomermaingroupdf=x_customer_main_group, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-4: x_customer_main_group Table")
        logger.info(f"CSV Count after rule 4: {csv.count()}")

        # Rule 5
        log_time_start = time.time()
        x_ship_to = spark.read.jdbc(url=url, table="x_ship_to", properties=properties)
        csv = join_operation_with_x_ship_to(csvdf=csv, xshiptodf=x_ship_to, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-5: x_ship_to Table")
        logger.info(f"CSV Count after rule 5: {csv.count()}")

        # Rule 6
        log_time_start = time.time()
        x_sold_to = spark.read.jdbc(url=url, table="x_sold_to", properties=properties)
        csv = join_operation_with_x_sold_to(csvdf=csv, xsoldtodf=x_sold_to, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "Time Taken for Implementing Rule-6: x_sold_to Table")
        logger.info(f"CSV Count after rule 6: {csv.count()}")

        csv = csv.withColumn('code', col('custid'))\
                 .withColumn('updated_by', lit("admin"))\
                 .withColumn('updated_date', lit(datetime.now()))\
                 .withColumn('validationstatus', lit(" "))\
                 .withColumn('is_valid', lit(False))\
                 .withColumn('created_date', lit(datetime.now()))\
                 .withColumn('created_by', lit("admin"))\
                 .withColumn('comment', lit(" "))\
                 .filter(col('code').isNotNull())

      
        # Concatenate columns and add hash and checksum columns
        columns_to_concat = [col for col in csv.columns if col not in ['created_date', 'updated_date']]
        csv = csv.withColumn("concatenated", concat_ws("", *columns_to_concat))
        csv = csv.withColumn("concatenated", csv['concatenated'].cast("string"))
        csv = csv.withColumn("hash_value", sha1(csv['concatenated']))
        csv = csv.withColumn("check_sum", hash(csv['concatenated']))
        csv = csv.drop("concatenated")
        

        logger.info(f"CSV Count after all transformations: {csv.count()}")
        
        insert_time = time.time()
        logger.info("Insertion process start")
        logger.info(f"CSV Count Before entering insert Method: {csv.count()}")
        hash_table_db = spark.read.jdbc(url=url, table="customer_hash", properties=properties)
        data_to_insert_stagging_table(csv, hash_table_db, url, properties)
        logger.info("Insertion process end")
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
    finally:
        pass
       
def customer_table_step_2_insert_into_main(spark):
    connection = None
    try:
        logger.info("Step 2 Started . method NAme customer_table_step_2_insert_into_main")
        # psycopg2 Property
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        # Load data from source table into a DataFrame
        start_time_main = time.time()
        inbound_spark_management["start_time"] = datetime.now()

        start_time = time.time()
        properties = {"driver": db_driver, "user": db_user,"password": db_password}
        logger.info("Loading data from source table into a DataFrame...")
        sourceDF = spark.read.jdbc(db_url, table="customer_insert_staging", properties=properties)
        log_time(start_time, "Time Taken To Load The data")
        logger.info(f"Total Rows Count = {sourceDF.count()}")

        logger.info("Writing data into the target table...")
        columns_to_insert = [col for col in sourceDF.columns if col not in ['id', 'code']]
        sourceDF.select(columns_to_insert).write.jdbc(db_url, table="customer", mode="append", properties=properties)

        # Insert data count into the logging dictionary
        inbound_spark_management["data_insert_count"] = sourceDF.count()

        # Truncate staging table
        cursor.execute("TRUNCATE TABLE customer_insert_staging")
        connection.commit()
        logger.info("Staging table truncated successfully.")
        log_time(start_time_main, "-----------Step Two Loading data from insert Staging to main Table---------------------")

        # Log success message
        inbound_spark_management["status"] = "Success"
        inbound_spark_management["success_message"] = "customer_table_step_2_insert_into_main executed successfully"
    except Exception as e:
        logger.error(f"Error in customer_table_step_2_insert_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def customer_table_step_3_update_into_main():
    connection = None
    try:
        
        logger.info("----------------Step 3 Started Method Name customer_table_step_3_update_into_main ")
        # psycopg2 Property
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        start_time_main = time.time()

        # SQL update query
        sql_update_query = """
                BEGIN;
                UPDATE customer
                SET
                    chainc = sourceTable.chainc,
                    chnl = sourceTable.chnl,
                    cbseg = sourceTable.cbseg,
                    grp = sourceTable.grp,
                    mngrp = sourceTable.mngrp,
                    subgrp_d = sourceTable.subgrp_d,
                    mngrp_d = sourceTable.mngrp_d,
                    shpto_d = sourceTable.shpto_d,
                    chaind = sourceTable.chaind,
                    chnldesc = sourceTable.chnldesc,
                    grp_d = sourceTable.grp_d,
                    shpto = sourceTable.shpto,
                    sldto = sourceTable.sldto,
                    custid = sourceTable.custid,
                    name = sourceTable.name,
                    custid_d = sourceTable.custid_d,
                    custsts = sourceTable.custsts,
                    csubtype = sourceTable.csubtype,
                    ctype = sourceTable.ctype,
                    export = sourceTable.export,
                    fds = sourceTable.fds,
                    pdcflag = sourceTable.pdcflag,
                    sales = sourceTable.sales,
                    bgrp = sourceTable.bgrp,
                    cprog = sourceTable.cprog,
                    cdisccd = sourceTable.cdisccd,
                    cdiscp = sourceTable.cdiscp,
                    curr = sourceTable.curr,
                    subgrp = sourceTable.subgrp,
                    cusgrp = sourceTable.cusgrp,
                    region = sourceTable.region,
                    catcd = sourceTable.catcd,
                    geo = sourceTable.geo,
                    xrefcd = sourceTable.xrefcd,
                    housef = sourceTable.housef,
                    mkupf = sourceTable.mkupf,
                    mercf = sourceTable.mercf,
                    discf = sourceTable.discf,
                    euccf = sourceTable.euccf,
                    eucprcl = sourceTable.eucprcl,
                    mmexcl = sourceTable.mmexcl,
                    updated_by = sourceTable.updated_by,
                    updated_date = sourceTable.updated_date,
                    validationstatus = sourceTable.validationstatus,
                    is_valid = sourceTable.is_valid,
                    created_date = sourceTable.created_date,
                    created_by = sourceTable.created_by,
                    comment = sourceTable.comment
                FROM 
                    customer_update_staging AS sourceTable
                WHERE
                    customer.custid = sourceTable.custid;

                TRUNCATE TABLE customer_update_staging;

                UPDATE customer_hash  
                SET
                    hash_value = temp.hash_value
                FROM
                    customer_hash_staging temp
                WHERE
                    customer_hash.code = temp.code;

                TRUNCATE TABLE customer_hash_staging;

                COMMIT;
        """

        # Execute the SQL update query
        logger.info("Executing SQL update query...")
        start_time = time.time()
        cursor.execute(sql_update_query)
        connection.commit()
        log_time(start_time, "SQL update query executed in")
        # update_count = cursor.fetchone()[0]
        # inbound_spark_management["data_update_count"] = update_count
        log_time(start_time_main, "------------Step Thre Updating the Data From Staging Update to main Table Completed ----------")
    except Exception as e:
        logger.error(f"Error in customer_table_step_3_update_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(f'Error in customer_table_step_3_update_into_main: {e}')
    finally:
        if connection:
            cursor.close()
            connection.close()

def insertAuditTable(data_table_values):
    # psycopg2 Property 
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

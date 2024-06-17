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
    "OCA": "OCACUSPNUM_2024",
    "AUS": "AUSCUSPNUM_2024"
}

inbound_spark_management = {
    "file_name": "",           # The name of the file being processed
    "table_name": "customer_part_numbers",  # The name of the table being affected
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
    "new_code":IntegerType(),
    "record_action": StringType(),
    "rgcd": StringType(),
    "pkgspc": IntegerType(),
    "id": IntegerType(),
    "tfcusxrf": IntegerType(),
    "xfmcar": StringType(),
    "code": IntegerType(),
    "name": StringType(),
    "xcuscd": StringType(),
    "xdesc": StringType(),
    "cpalqt": IntegerType(),
    "xrfptno": StringType(),
    "cmstqt": IntegerType(),
    "ukgspc": StringType(),
    "cuntuk": IntegerType(),
    "cuntpk":IntegerType(),
    "xdiskno": StringType(),
    "repack": StringType(),
    "xricno": StringType(),
    "xstat": StringType(),
    "xdate": TimestampType(),   
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
        # Drop rows where any of 'rgcd', 'xrfptno', 'xricno' is null
        df = df.filter((col('rgcd').isNotNull()) & 
                       (col('xrfptno').isNotNull()) & 
                       (col('xricno').isNotNull()))
        
        # Drop duplicates based on 'rgcd', 'xrfptno', 'xricno'
        df = df.dropDuplicates(['rgcd', 'xrfptno', 'xricno'])
        
        logger.info("Rows with null values in 'rgcd', 'xrfptno', 'xricno' dropped "
                    "and duplicates based on 'rgcd', 'xrfptno', 'xricno' removed")
        return df
    except Exception as e:
        logger.error(f"Error in simplify_colum_with_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in simplify_colum_with_table method: {e}'
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
                            customer_part_numbers_table_step_2_insert_into_main(spark,properties,db_url)
                            customer_part_numbers_table_step_3_update_into_main()
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
        # Add 'new_code' column rgcd', 'xrfptno', 'xricno'
        df = df.withColumn('code', concat_ws("",col('rgcd'), col('xrfptno'), col('xricno')))
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
        csv_to_insert_stage = csv_to_insert_stage.drop('code', 'record_action', 'pkgspc', 'cuntpk')
        csv_to_insert_stage.write.jdbc(url=url, table="customer_part_numbers_insert_staging", mode="append", properties=properties)
        log_time(start_time=start_time,str="csv to insert stage")

        # insert hash table
        missing_code_to_hash_table = missing_ids_csv.select('code', 'hash_value','check_sum')
        start_hash_time = time.time()
        missing_code_to_hash_table.write.jdbc(url=url, table="customer_part_numbers_hash", mode="append", properties=properties)
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
        csv_to_update_stage = csv_to_update_stage.drop('code', 'record_action', 'pkgspc', 'cuntpk')
        csv_to_update_stage.write.jdbc(url=url, table="customer_part_numbers_update_staging", mode="append", properties=properties)    
        log_time(start_time=start_time, str="csv to update stage")

        # insert hash temp table
        mismatch_hash_to_hash_temp = updated_hash_csv.select("code", "hash_value", "check_sum")
        start_hash_time = time.time()
        mismatch_hash_to_hash_temp.write.jdbc(url=url, table="customer_part_numbers_hash_staging", mode="append", properties=properties)    
        log_time(start_time=start_hash_time, str="csv hash to hash stage")
    except Exception as e:
        logger.error(f"Error in data_to_update_stagging_table: {e}")
 
def compare_and_insert(df, main_table_df):
    try:
        # Compare 'code' column with main table 'code' column
        mismatch_df = df.join(main_table_df, "new_code", "left_anti")

        # If there are unmatched records, generate hash and insert into main table
        if mismatch_df.count() > 0:
            mismatch_df = mismatch_df.withColumn("hash_value", sha1(mismatch_df['concatenated']))
            mismatch_df = mismatch_df.withColumn("check_sum", hash(mismatch_df['concatenated']))
            mismatch_df = mismatch_df.drop("concatenated")
            mismatch_df.write.jdbc(url=db_url, table="customer_part_numbers", mode="append")
    except Exception as e:
        logger.error(f"Error in compare_and_insert: {e}")
        
def main(local_file_path,spark,properties,url):
    
    try:
        start_process_time = time.time()
        logging.info('-----------------------starting data migration------------------------------')
        log_start_time = time.time()
        
        csv = read_csv_data(path_to_csv=local_file_path, spark=spark)
        log_time(log_start_time,"Time Taken in First Stage Wich is Reading data FRom Csv and Casting to Proper dataType")
        
        csv = csv .withColumn('updated_by', lit("admin"))\
                 .withColumn('updated_date', lit(datetime.now()))\
                 .withColumn('validationstatus', lit(" "))\
                 .withColumn('is_valid', lit(False))\
                 .withColumn('created_date', lit(datetime.now()))\
                 .withColumn('created_by', lit("admin"))\
                 .withColumn('comment', lit(" "))\

       # Assuming 'csv' is your DataFrame
        columns_to_concat = [col for col in csv.columns if col not in ['created_date', 'updated_date']]
        csv = csv.withColumn("concatenated", concat_ws("", *columns_to_concat))
        csv = csv.withColumn("concatenated", csv['concatenated'].cast("string"))
        csv = csv.withColumn("hash_value", sha1(csv['concatenated']))
        csv = csv.withColumn("check_sum", hash(csv['concatenated']))
        csv = csv.drop("concatenated")
       
        hash_table_db = spark.read.jdbc(url=url, table="customer_part_numbers_hash", properties=properties)
        data_to_insert_stagging_table(csv, hash_table_db, url, properties)
        insert_time = time.time()
        logger.info("Update Table process start")
        data_to_update_stagging_table(csv, hash_table_db, url, properties)
        log_time(insert_time, "Time Taken for Update Method")
        
    except Exception as e:
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'
        logger.error(f"Error in main method: {e}")
        
def customer_part_numbers_table_step_2_insert_into_main(spark, properties, db_url):
    connection = None
    logger.info("Step 2 Started. Method Name: customer_part_numbers_table_step_2_insert_into_main")
    
    try:
        # Establish database connection
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()
        
        # Load data from source table into a DataFrame
        logger.info("Loading data from source table into a DataFrame...")
        start_time = time.time()
        sourceDF = spark.read.jdbc(db_url, table="customer_part_numbers_insert_staging", properties=properties)
        source_count = sourceDF.count()
        logger.info(f"Total Rows Count = {source_count}")
        
        # Write data into the target table
        columns_to_insert = [col for col in sourceDF.columns if col not in ['id', 'code']]
        sourceDF.select(columns_to_insert).write.jdbc(db_url, table="customer_part_numbers", mode="append", properties=properties)
        inbound_spark_management["data_insert_count"] = source_count
        
        # Truncate staging table
        cursor.execute("TRUNCATE TABLE customer_part_numbers_insert_staging")
        connection.commit()
        logger.info("Staging table truncated successfully.")
        
        # Log success message
        inbound_spark_management["status"] = "Success"
        inbound_spark_management["success_message"] = "customer_part_numbers_table_step_2_insert_into_main executed successfully"
        
    except Exception as e:
        logger.error(f"Error in customer_part_numbers_table_step_2_insert_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(e)
        
    finally:
        # Close cursor and database connection
        if connection:
            cursor.close()
            connection.close()
                   
def customer_part_numbers_table_step_3_update_into_main():
    connection = None
    try:
        logger.info("----------------Step 3 Started Method Name customer_part_numbers_table_step_3_update_into_main ")
        # psycopg2 Property
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        start_time_main = time.time()

        sql_update_query  = """
            BEGIN;
            UPDATE customer_part_numbers
            SET
                name = sourceTable.name,
                tfcusxrf = sourceTable.tfcusxrf,
                xfmcar = sourceTable.xfmcar,
                xdesc = sourceTable.xdesc,
                xcuscd = sourceTable.xcuscd,
                cpalqt = sourceTable.cpalqt,
                xrfptno = sourceTable.xrfptno,
                cmstqt = sourceTable.cmstqt,
                ukgspc = sourceTable.ukgspc,
                cuntuk = sourceTable.cuntuk ,
                xdiskno = sourceTable.xdiskno,
                repack = sourceTable.repack,
                xricno = sourceTable.xricno,
                xstat = sourceTable.xstat,
                xdate = sourceTable.xdate,
                updated_by = sourceTable.updated_by,
                updated_date = sourceTable.updated_date,
                validationstatus = sourceTable.validationstatus,
                is_valid = sourceTable.is_valid,
                created_date = sourceTable.created_date,
                created_by = sourceTable.created_by,
                comment = sourceTable.comment
            FROM 
                customer_part_numbers_update_staging AS sourceTable
            WHERE
                customer_part_numbers.rgcd = sourceTable.rgcd AND
                customer_part_numbers.xrfptno = sourceTable.xrfptno AND
                customer_part_numbers.xricno = sourceTable.xricno;

            TRUNCATE TABLE customer_part_numbers_update_staging;

            UPDATE customer_part_numbers_hash  
            SET
                hash_value = temp.hash_value
            FROM
                customer_part_numbers_hash_staging temp
            WHERE
                customer_part_numbers_hash.code = temp.code;

            TRUNCATE TABLE customer_part_numbers_hash_staging;

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
        logger.error(f"Error in customer_part_numbers_table_step_3_update_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = f'Error in customer_part_numbers_table_step_3_update_into_main: {e}'
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
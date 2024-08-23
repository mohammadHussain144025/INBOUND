from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, year, concat_ws, lit, substring, hash,when, to_timestamp, sha1
from pyspark.sql.types import StructType, StructField, DecimalType, StringType, IntegerType, TimestampType, BooleanType
import time
import logging
from datetime import datetime
import paramiko
import hashlib
import psycopg2
import os
from dotenv import load_dotenv
import gzip
import shutil
import gnupg
from setup import envFile, file_mapping_invoice_details

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__) 

# Determine which .env file to load based on the environment
environment = os.getenv('ENVIRONMENT', envFile)  # Default to local if not set
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
zip_directory = os.environ.get('ZIP_DIRECTORY')
pgp_directory = os.environ.get('PGP_DIRECTORY')
csv_directory = os.environ.get('CSV_DIRECTORY')
private_key_file = os.environ.get('PRIVATE_KEY_FILE')
passphrase = os.environ.get('PASSPHRASE')
gnupg_local = os.environ.get("GNUPG_LOCATION")

inbound_spark_management = {
    "file_name": "",           # The name of the file being processed
    "table_name": "coninv",  # The name of the table being affected
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
    "name": StringType(),
    "code": IntegerType(),
    "cpartno":StringType(),
    "ctrck":StringType(),
    "cprccd":StringType(),
    "csolcd":StringType(),
    "cfill1":StringType(),
    "cprbtg":StringType(),
    "crefcd":StringType(),
    "cdirsh":StringType(),
    "cshpfr":StringType(),
    "cshpto":StringType(),
    "cuntdn":StringType(),
    "cdistg":StringType(),
    "ccuspo":StringType(),
    "cttm":StringType(),
    "cstate":StringType(),
    "crdeal":StringType(),
    "creman":StringType(),
    "crdlr":StringType(),
    "comgen":StringType(),
    "crapid":StringType(),
    "cfill3":StringType(),
    "cintfl":StringType(),
    "cintin":StringType(),
    "cfill2":StringType(),
    "cordno": DecimalType(16, 6),
    "cinvno": DecimalType(16, 6),
    "cuntpr": DecimalType(16, 6),
    "cqtshp": DecimalType(16, 6),
    "csdcst": DecimalType(16, 6),
    "cnslpr": DecimalType(16, 6),
    "cpbamt": DecimalType(16, 6),
    "ccordt": TimestampType(),
    "crcore": DecimalType(16, 6),
    "cinvdt": TimestampType(),
    "cshpdt": TimestampType(),
    "updated_by": StringType(),
    "updated_date": TimestampType(),
    "validationstatus": StringType(),
    "is_valid": BooleanType(),
    "created_date": TimestampType(),
    "created_by": StringType(),
    "comment": StringType(),
    "rgcd":StringType()
}

def simplify_colum_with_table(df):
    try:
        # Drop rows where 'code' is null
        df = df.filter((col('cordno').isNotNull()) & 
                       (col('cinvno').isNotNull()) & 
                       (col('cpartno').isNotNull()) & 
                       (col('rgcd').isNotNull()))
        print('simplify_colum_with_table check ----------------> ', df.count())
        df = df.dropDuplicates(['cordno', 'cinvno', 'cpartno', 'rgcd'])
        print('simplify_colum_with_table----------------> ', df.count())
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
        for region, filename_prefix in file_mapping_invoice_details.items():
            inbound_spark_management['region'] = region
            remote_region_dir = f"{remote_directory}/{region}"
            logger.info(f"Checking directory: {remote_region_dir} for files with prefix: {filename_prefix} and date: {current_date}")

            try:
                files = sftp.listdir(remote_region_dir)
                for file in files:
                    logger.info(f"Checking file: {file}")
                    if file.startswith(filename_prefix) and current_date in file:
                        inbound_spark_management['file_name'] = file
                        inbound_spark_management['file_code'] = hashlib.sha256(file.encode()).hexdigest()
                        remote_file_path = f"{remote_region_dir}/{file}"
                        pgp_file_path = f"{pgp_directory}/{file}"
                        csv_file_path=f"{csv_directory}/{file}"
                        # sftp.get(remote_file_path, pgp_file_path)

                        output_csv_path = None
                        try:
                            start_time = datetime.now()
                            if file.endswith('.pgp'):
                                logger.info(f" Inside The PGP Loop ")
                                sftp.get(remote_file_path, pgp_file_path)
                                logger.info(f"Downloaded file: {remote_file_path} to {pgp_file_path}")
                                output_csv_path = decrypt_file_and_extract_csv(
                                    pgp_file_path, pgp_directory, zip_directory, private_key_file, passphrase)
                            elif file.endswith('.csv'):
                                logger.info(f" Inside The Csv Loop ")
                                sftp.get(remote_file_path, csv_file_path)
                                logger.info(f"Downloaded file: {remote_file_path} to {csv_file_path}")
                                output_csv_path = csv_file_path
                            else:
                                raise ValueError('Unsupported file type from SFTP')
                            
                            if output_csv_path:
                                main(output_csv_path, spark, properties, db_url)
                                invoice_table_step_2_insert_into_main(spark,properties,db_url)
                                invoice_table_step_3_update_into_main()
                                end_time = datetime.now()
                                duration = end_time - start_time
                                inbound_spark_management["start_time"] = start_time.strftime('%Y-%m-%d %H:%M:%S')
                                inbound_spark_management["end_time"] = end_time.strftime('%Y-%m-%d %H:%M:%S')
                                inbound_spark_management["duration"] = str(duration)
                                inbound_spark_management["success_message"] = 'File Successfully Uploaded To Database'
                                inbound_spark_management["status"] = 'Success'
                                insertAuditTable(inbound_spark_management)
                                logger.info("Full Process Complete Successfully")
                            else:
                                raise ValueError('Failed to determine the output CSV path')  
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
    except Exception as e:
        logger.error(f"Error while executing check_and_download_file: {e}")
        inbound_spark_management["error_message"] = f'Error while executing check_and_download_file method: {e}'
        inbound_spark_management["status"] = 'Failed'
        insertAuditTable(inbound_spark_management)
    finally:    
        if sftp:
            sftp.close()
        if spark:
            spark.stop()

def decrypt_file_and_extract_csv(encrypted_file, csv_file_directory, temp_file_directory, private_key_file, passphrase):
    try:
        base_filename = os.path.basename(encrypted_file).replace('.pgp', '')
        decrypted_temp_file = os.path.join(temp_file_directory, base_filename)
         # Clean the base filename to remove '.gz' and '.zip'
        cleaned_filename = base_filename.replace('.gz', '').replace('.zip', '')
        
        # Determine the final CSV output path based on filename
        if base_filename.endswith('.gz'):
            output_csv_file = os.path.join(csv_file_directory, cleaned_filename + '.csv')
        else:
            output_csv_file = os.path.join(csv_file_directory, cleaned_filename)
        logger.info(f"Base filename: {base_filename}")

        gpg = gnupg.GPG(gpgbinary=gnupg_local)

        with open(private_key_file, 'r') as key_file:
            key_data = key_file.read()
            import_result = gpg.import_keys(key_data)

            if import_result.count == 0:
                logger.error("Failed to import the private key.")
                return None

        if not os.path.exists(csv_file_directory):
            os.makedirs(csv_file_directory)

        if not os.path.exists(temp_file_directory):
            os.makedirs(temp_file_directory)

        with open(encrypted_file, 'rb') as f:
            decrypted_data = gpg.decrypt_file(f, passphrase=passphrase, output=decrypted_temp_file, always_trust=True)
        if decrypted_data.ok:
        # Extract CSV from GZIP
            with gzip.open(decrypted_temp_file, 'rb') as gz_file:
                with open(output_csv_file, 'wb') as csv_file:
                    shutil.copyfileobj(gz_file, csv_file)

            # Remove temporary GZIP file
            os.remove(decrypted_temp_file)

            print(f"Decryption and extraction successful: {output_csv_file}")
            return output_csv_file
        else:
            print("Decryption failed.")
            print(f"Status: {decrypted_data.status}")
            print(f"Stderr: {decrypted_data.stderr}")
            return False
    except Exception as e:
        logger.error(f"Error during decryption: {str(e)}")
        return None

def read_csv_data(path_to_csv, spark):
    try:
        df = spark.read.csv(path_to_csv, header=True, sep=',')
        logger.info(f"CSV data loaded from {path_to_csv}")

          # Check if the DataFrame is empty
        if df.rdd.isEmpty():
            logger.warning(f"The CSV at {path_to_csv} contains no data.")
            return None
            
        df = df.toDF(*(c.lower() for c in df.columns))

        for column_name ,data_type  in schema_dict.items():
            if column_name in df.columns:
                df = df.withColumn(column_name,col(column_name).cast(data_type))

        csv_count = df.count()
        inbound_spark_management["csv_count"] = csv_count
        logger.info(f"CSV row count: {csv_count}")

        df = simplify_colum_with_table(df)
        df = df.withColumn('code', concat_ws("",col('cordno'), col('cinvno'), col('cpartno'), col('rgcd')))
        return df
    except Exception as e:
        logger.error(f"Error in read_csv_data: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in read_csv_data method: {e}'
        raise

def data_to_insert_staging_table(csv, hash_table_db, url, properties):
    try:
        missing_ids_csv = csv.join(hash_table_db, "code", "left_anti")
        logger.info(f"data to be insert: {missing_ids_csv.count()}")

        if missing_ids_csv.count() == 0:
            return
        
        # insert staging table
        csv_to_insert_stage = missing_ids_csv.drop('hash_value','check_sum')
        
        start_time = time.time()
        csv_to_insert_stage = csv_to_insert_stage.drop('code')
        csv_to_insert_stage.write.jdbc(url=url, table="coninv_insert_staging", mode="append", properties=properties)
        log_time(start_time=start_time,str="csv to insert stage")

        # insert hash table
        missing_code_to_hash_table = missing_ids_csv.select('code', 'hash_value','check_sum')
        start_hash_time = time.time()
        missing_code_to_hash_table.write.jdbc(url=url, table="coninv_hash", mode="append", properties=properties)
        log_time(start_time=start_hash_time, str="csv hash to hash stage")

    except Exception as e:
        logger.error(f"Error in data_to_insert_staging_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'

def data_to_update_staging_table(csv, hash_table_db, url, properties):
    try:
        code_matching = hash_table_db.join(csv,"code","inner")
        code_matching = code_matching.drop(hash_table_db['hash_value'])
        code_matching = code_matching.drop(hash_table_db['check_sum'])
        code_matching = code_matching.drop(hash_table_db['id'])
        print("code_matching Count Before performing Join  In hash table with hash_value -----------",code_matching.count())
        updated_hash_csv = code_matching.join(hash_table_db, "hash_value", "left_anti" )
        print("updated_hash_csv Count Before enting insert Method -----------",updated_hash_csv.count())
        
        if updated_hash_csv.count() == 0:
            return

        # insert update staging table
        csv_to_update_stage = updated_hash_csv.drop('hash_value', 'check_sum')
        start_time = time.time()
        csv_to_update_stage = csv_to_update_stage.drop('code')
        csv_to_update_stage.write.jdbc(url=url, table="coninv_update_staging", mode="append", properties=properties)    
        log_time(start_time=start_time, str="csv to update stage")

        # insert hash temp table
        mismatch_hash_to_hash_temp = updated_hash_csv.select("code", "hash_value", "check_sum")
        start_hash_time = time.time()
        mismatch_hash_to_hash_temp.write.jdbc(url=url, table="coninv_hash_staging", mode="append", properties=properties)    
        log_time(start_time=start_hash_time, str="csv hash to hash stage")
    except Exception as e:
        logger.error(f"Error in data_to_update_staging_table: {e}")

def main(local_file_path, spark, properties, url):
    try:
        start_process_time = time.time()
        logging.info('-----------------------starting data migration------------------------------')
        log_start_time = time.time()

        csv = read_csv_data(path_to_csv=local_file_path, spark=spark)
        log_time(log_start_time,"Time Taken in First Stage Wich is Reading data FRom Csv and Casting to Proper dataType")
        csv_count = csv.count()
        logger.info(f"readCsv rows count: {csv_count}")

        if csv_count == 0:
            logger.info("CSV row count is 0.")
 
        csv = csv.withColumn('updated_by', lit("system@spriced.com"))\
                 .withColumn('updated_date', lit(datetime.now()))\
                 .withColumn('validationstatus', lit("true"))\
                 .withColumn('is_valid', lit(False))\
                 .withColumn('created_date', lit(datetime.now()))\
                 .withColumn('created_by', lit("system@spriced.com"))\
                 .withColumn('comment', lit(" "))\

       # Assuming 'csv' is your DataFrame
        columns_to_concat = [col for col in csv.columns if col not in ['created_date', 'updated_date']]
        csv = csv.withColumn("concatenated", concat_ws("", *columns_to_concat))
        csv = csv.withColumn("concatenated", csv['concatenated'].cast("string"))
        csv = csv.withColumn("hash_value", sha1(csv['concatenated']))
        csv = csv.withColumn("check_sum", hash(csv['concatenated']))
        csv = csv.drop("concatenated")

        hash_table_db = spark.read.jdbc(url=url, table="coninv_hash", properties=properties)
        data_to_insert_staging_table(csv, hash_table_db, url, properties)
        insert_time = time.time()
        logger.info("Update Table process start")
        data_to_update_staging_table(csv, hash_table_db, url, properties)
        log_time(insert_time, "Time Taken for Update Method")
        
    except Exception as e:
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'
        logger.error(f"Error in main method: {e}")

def invoice_table_step_2_insert_into_main(spark, properties, db_url):
    connection = None
    try:
        # psycopg2 Property
        logger.info("Step 2 Started . method Name invoice_table_step_2_insert_into_main")
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()
        
        # Load data from source table into a DataFrame
        start_time_main = time.time()
        inbound_spark_management["start_time"] = datetime.now()
        start_time = time.time()
        
        logger.info("Loading data from source table into a DataFrame...")
        sourceDF = spark.read.jdbc(db_url, table="coninv_insert_staging", properties=properties)
        log_time(start_time, "Time Taken To Load The data")
        source_count = sourceDF.count()
        logger.info(f"Total Rows Count = {source_count}")
        
        logger.info("Writing data into the target table...")
        columns_to_insert = [col for col in sourceDF.columns if col not in ['id', 'code']]
        sourceDF.select(columns_to_insert).write.jdbc(db_url, table="coninv", mode="append", properties=properties)
        
        # Insert data count into the logging dictionary
        inbound_spark_management["data_insert_count"] = sourceDF.count()
        
        # Truncate staging table
        cursor.execute("TRUNCATE TABLE coninv_insert_staging")
        connection.commit()
        logger.info("Staging table truncated successfully.")
        
        log_time(start_time_main, "-----------Step Two Loading data from insert Staging to main Table---------------------")
        
        # Log success message
        inbound_spark_management["status"] = "Success"
        inbound_spark_management["success_message"] = "invoice_table_step_2_insert_into_main executed successfully"
    except Exception as e:
        logger.error(f"Error in invoice_table_step_2_insert_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def invoice_table_step_3_update_into_main():
    connection = None
    try:
        logger.info("----------------Step 3 Started Method Name invoice_table_step_3_update_into_main ")
        # psycopg2 Property
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        start_time_main = time.time()

        sql_update_query  = """
            BEGIN;
            UPDATE coninv
            SET
                name = sourceTable.name,
                cpartno = sourceTable.cpartno,
                ctrck = sourceTable.ctrck,
                cprccd = sourceTable.cprccd,
                csolcd = sourceTable.csolcd,
                cfill1 = sourceTable.cfill1,
                cprbtg = sourceTable.cprbtg,
                crefcd = sourceTable.crefcd,
                cdirsh = sourceTable.cdirsh,
                cshpfr = sourceTable.cshpfr,
                cshpto = sourceTable.cshpto,
                cuntdn = sourceTable.cuntdn,
                cdistg = sourceTable.cdistg,
                ccuspo = sourceTable.ccuspo,
                cttm = sourceTable.cttm,
                cstate = sourceTable.cstate,
                crdeal = sourceTable.crdeal,
                creman = sourceTable.creman,
                crdlr = sourceTable.crdlr,
                comgen = sourceTable.comgen,
                crapid = sourceTable.crapid,
                cfill3 = sourceTable.cfill3,
                cintfl = sourceTable.cintfl,
                cintin = sourceTable.cintin,
                cfill2 = sourceTable.cfill2,
                cordno = sourceTable.cordno,
                cinvno = sourceTable.cinvno,
                cuntpr = sourceTable.cuntpr,
                cqtshp = sourceTable.cqtshp,
                csdcst = sourceTable.csdcst,
                cnslpr = sourceTable.cnslpr,
                cpbamt = sourceTable.cpbamt,
                ccordt = sourceTable.ccordt,
                crcore = sourceTable.crcore,
                cinvdt = sourceTable.cinvdt,
                cshpdt = sourceTable.cshpdt,
                rgcd = sourceTable.rgcd,
                updated_by = sourceTable.updated_by,
                updated_date = sourceTable.updated_date,
                validationstatus = sourceTable.validationstatus,
                is_valid = sourceTable.is_valid,
                created_date = sourceTable.created_date,
                created_by = sourceTable.created_by,
                comment = sourceTable.comment
            FROM 
                coninv_update_staging AS sourceTable
            WHERE
                coninv.cordno = sourceTable.cordno AND
                coninv.cinvno = sourceTable.cinvno AND
                coninv.cpartno = sourceTable.cpartno AND
                coninv.rgcd = sourceTable.rgcd;

            TRUNCATE TABLE coninv_update_staging;

            UPDATE coninv_hash  
            SET
                hash_value = temp.hash_value
            FROM
                coninv_hash_staging temp
            WHERE
                coninv_hash.code = temp.code;

            TRUNCATE TABLE coninv_hash_staging;

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
        logger.error(f"Error in invoice_table_step_3_update_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = f'Error in invoice_table_step_3_update_into_main: {e}'
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

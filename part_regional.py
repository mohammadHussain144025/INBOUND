from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, year, concat_ws, lit, substring, hash,when, to_timestamp
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
from setup import file_mapping_part_region, envFile

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
    "table_name": "part_regional",  # The name of the table being affected
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
    "brnd_cd": IntegerType(),
    "gbb": IntegerType(),
    "lfcycle": IntegerType(),
    "mktcd": StringType(),
    "mfgcc": IntegerType(),
    "prod_cd1": IntegerType(),
    "prod_cd3": IntegerType(),
    "mfgpdcd": IntegerType(),
    "prdmgr": IntegerType(),
    "part_num": StringType(),
    "code": StringType(),
    "long_ptno": StringType(),
    "projcst": DecimalType(16, 6),
    "prod_cd5": StringType(),
    "cr_dt": TimestampType(),
    "shrt_desc": StringType(),
    "long_desc": StringType(),
    "fran_code": StringType(),
    "prtcls": DecimalType(16, 6),
    "sup_prt": StringType(),
    "sup_dt": TimestampType(),
    "prtstat": StringType(),
    "popcdny": StringType(),
    "popcdcy": StringType(),
    "pllqty": IntegerType(),
    "drop_ship": StringType(),
    "cat_cd": StringType(),
    "cc1": StringType(),
    "cc2": StringType(),
    "cc3": StringType(),
    "cc4": StringType(),
    "cc5": StringType(),
    "sac": StringType(),
    "uac": StringType(),
    "stdcst": DecimalType(16, 6),
    "crcst": DecimalType(16, 6),
    "lprc": DecimalType(16, 6),
    "crprc": DecimalType(16, 6),
    "stdmcrcst": DecimalType(16, 6),
    "lpp_prc": DecimalType(16, 6),
    "lppqty": IntegerType(),
    "lppdt": TimestampType(),
    "lppvndr": StringType(),
    "postdt": TimestampType(),
    "poenddt": TimestampType(),
    "povndr": StringType(),
    "pofgt": StringType(),
    "popblp": DecimalType(16, 6),
    "popblq": IntegerType(),
    "popt": StringType(),
    "pocurr": StringType(),
    "pofdc": StringType(),
    "uytd": IntegerType(),
    "uy1qty": IntegerType(),
    "uy2qty": IntegerType(),
    "uy3qty": IntegerType(),
    "uy4qty": IntegerType(),
    "uy5qty": IntegerType(),
    "mfgnr": StringType(),
    "mfgcommcd": StringType(),
    "mfgfamcd": StringType(),
    "pnf_fmsi": StringType(),
    "mktcdl1": StringType(),
    "mktcdl2": IntegerType(),
    "mktcdl3": IntegerType(),
    "mktcdl4": IntegerType(),
    "mktcdl5": IntegerType(),
    "kit_flag": StringType(),
    "cr_gp": StringType(),
    "core_part": StringType(),
    "origin": StringType(),
    "nonrtn": StringType(),
    "harcode": IntegerType(),
    "ldtime": DecimalType(16, 6),
    "upc": IntegerType(),
    "vmrs": StringType(),
    "height": DecimalType(16, 6),
    "prtlen": DecimalType(16, 6),
    "prtwid": DecimalType(16, 6),
    "planner": StringType(),
    "buyer": StringType(),
    "cystdmtl": DecimalType(16, 6),
    "dutfrgexc": DecimalType(16, 6),
    "dutycst": DecimalType(16, 6),
    "frgtcst": DecimalType(16, 6),
    "mfglabcst": DecimalType(16, 6),
    "whburcst": DecimalType(16, 6),
    "mfgbcst": DecimalType(16, 6),
    "warbcst": DecimalType(16, 6),
    "pckcst": DecimalType(16, 6),
    "corqty": IntegerType(),
    "vndrnm": StringType(),
    "vndrcd": StringType(),
    "rgcd": StringType(),
    "sub_brand": IntegerType(),
    "updated_by": StringType(),
    "updated_date": TimestampType(),
    "validationstatus": StringType(),
    "is_valid": BooleanType(),
    "created_date": TimestampType(),
    "created_by": StringType(),
    "comment": StringType(),
    "part_uofm": StringType(),
    "rg_part": StringType()
}

def simplify_colum_with_table(df):
    try:
        if 'cat_cd' in df.columns:
            df = df.withColumn("cc1", when(df['cat_cd'].isNotNull(), substring('cat_cd', 1, 1)))
            df = df.withColumn("cc2", when(df['cat_cd'].isNotNull(), substring('cat_cd', 1, 2)))
            df = df.withColumn("cc3", when(df['cat_cd'].isNotNull(), substring('cat_cd', 1, 3)))
            df = df.withColumn("cc4", when(df['cat_cd'].isNotNull(), substring('cat_cd', 1, 4)))
            df = df.withColumn("cc5", when(df['cat_cd'].isNotNull(), substring('cat_cd', 1, 5)))
        
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

        for region, filename_prefix in file_mapping_part_region.items():
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
                                part_regional_table_step_2_insert_into_main(spark,properties,db_url)
                                part_regional_table_step_3_update_into_main()
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
        df = df.drop("lpp_part")
        df = df.withColumn("prdmgr",lit(1))
        df = df.withColumn("brnd_cd",lit(1))
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

def match_csv_and_x_product(csvdf, xproductdf, columnNameToAddWithMatchDf):
    # Combined common code dataframe
    try:
        match_df = xproductdf.join(csvdf, "code", "inner")
        match_df = match_df.filter(col("code").isNotNull())
        match_df = match_df.withColumn(columnNameToAddWithMatchDf, col("id"))
        # Drop unnecessary columns
        drop_columns = ['id', 'name', 'updated_by', 'updated_date', 'validationstatus', 'is_valid', 'created_date', 'created_by', 'comment', 'code']
        match_df = match_df.drop(*drop_columns)
        return match_df
    except Exception as e:
        logger.error(f"Error in match_csv_and_part_regional: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in match_csv_and_part_regional: {e}'

def x_product_code_1_digit(csvdf, x_product_code_1_df, spark, url, properties):
    try:
        # csvdf = csvdf.withColumn('code', col('part_num')).filter(col('part_num').isNotNull())

        csvdf = csvdf.withColumn('code', substring(col('prod_cd5'), 1, 1)).filter(col('prod_cd5').isNotNull())
        unmatched_csv = csvdf.join(x_product_code_1_df, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )

        logger.info(f"__________ Rows Count in Rule 1 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_product_code_1_digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.filter(col('code').isNotNull())
        logger.info(f"__________ rows to append on x_product_code_1_digit Without null: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="x_product_code_1_digit", mode="append", properties=properties)

        # Drop the 'code' column after writing to the database
        selected_cols_unmatched = selected_cols_unmatched.drop('code')

        new_x_product_code_1 = spark.read.jdbc(url=url, table="x_product_code_1_digit", properties=properties)
        match_df = match_csv_and_x_product(csvdf, new_x_product_code_1, columnNameToAddWithMatchDf="prod_cd1")
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_product_code_1_digit: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_product_code_1_digit: {e}'

def x_product_code_3_digit(csvdf, x_product_code_3_df, spark, url, properties):
   try: 
        csvdf = csvdf.withColumn('code', substring(col('prod_cd5'), 1, 3)).filter(col('prod_cd5').isNotNull())
        unmatched_csv = csvdf.join(x_product_code_3_df, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )

        # logger.info(f"__________ Rows Count in Rule 2 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.distinct()
        # logger.info(f"__________ rows to append on x_product_code_3_digit: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        # logger.info(f"__________ rows to append on x_product_code_3_digit Without null: {selected_cols_unmatched.count()}")
        
        selected_cols_unmatched.write.jdbc(url=url, table="x_product_code_3_digit", mode="append", properties=properties)

        # Drop the 'code' column after writing to the database
        selected_cols_unmatched = selected_cols_unmatched.drop('code')

        new_x_product_code_3 = spark.read.jdbc(url=url, table="x_product_code_3_digit", properties=properties)
        match_df = match_csv_and_x_product(csvdf, new_x_product_code_3,columnNameToAddWithMatchDf="prod_cd3")
        return match_df
   except Exception as e:
        logger.error(f"Error in join_operation_with_x_product_code_3_digit: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_product_code_3_digit: {e}'
   
def compare_code_csv_with_p_life_cycle_stage(csvdf, p_lifecycle_stage_df, spark, url, properties):
    try:
        # Find matched rows
        csvdf = csvdf.withColumn('code', col('part_num')).filter(col('part_num').isNotNull())
        matched_csv = csvdf.join(p_lifecycle_stage_df, "code", "inner")
        matched_csv = matched_csv.withColumn("lfcycle", col("id"))
        
        # Drop unnecessary columns
        drop_columns = ['id', 'name', 'updated_by', 'updated_date', 'validationstatus', 'is_valid', 'created_date', 'created_by', 'comment']
        matched_csv = matched_csv.drop(*drop_columns)
        
        # Find unmatched rows
        unmatched_csv = csvdf.join(p_lifecycle_stage_df, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn("lfcycle", lit(None))
        
        # Union matched and unmatched rows
        merged_csv_df = unmatched_csv.union(matched_csv)
        return merged_csv_df

    except Exception as e:
        logger.error(f"Error in join_operation_with_p_life_cycle_product: {e}")
        inbound_spark_management = {'status': 'Failed', 'error_message': f'Error in join_operation_with_p_life_cycle_product: {e}'}
        return None
    
def compare_code_csv_with_x_sub_brand(csvdf, x_sub_brand_df, spark, url, properties):
   try: 
        csvdf = csvdf.withColumn('code', col('part_num')).filter(col('part_num').isNotNull())
        unmatched_csv = csvdf.join(x_sub_brand_df, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )
        # logger.info(f"__________ Rows Count in Rule 4 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        # logger.info(f"__________ rows to append on x_sub_brand: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="x_sub_brand", mode="append", properties=properties)
        # logger.info(f"__________ rows to append on x_sub_brand Without null: {selected_cols_unmatched.count()}")
        
        new_x_sub_brand = spark.read.jdbc(url=url, table="x_sub_brand", properties=properties)
        match_df = match_csv_and_x_product(csvdf, new_x_sub_brand, columnNameToAddWithMatchDf="sub_brand")
        return match_df
   except Exception as e:
        logger.error(f"Error in join_operation_with_x_sub_brand: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_sub_brand: {e}'

def compare_code_csv_with_x_mfg_prod_code(csvdf, xproductdf, spark, url, properties):
    try:
        unmatched_csv = csvdf.join(xproductdf, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )
        logger.info(f"__________ Rows Count in Rule 5 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.distinct()
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_mfg_prod_code: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="x_mfg_prod_code", mode="append", properties=properties)
        logger.info(f"__________ rows to append on x_mfg_prod_code Without null: {selected_cols_unmatched.count()}")
        newxproductdf = spark.read.jdbc(url=url, table="x_mfg_prod_code", properties=properties)
        match_df = match_csv_and_x_product(csvdf, newxproductdf, columnNameToAddWithMatchDf="MFGPDCD")
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_mfg_prod_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_mfg_prod_code: {e}'

def compare_code_csv_with_x_mfg_prod_code_class(csvdf, xproductdf, spark, url, properties):
    try:
        unmatched_csv = csvdf.join(xproductdf, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )
        logger.info(f"__________ Rows Count in Rule 6 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.distinct()
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_mfg_prod_code_class: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="x_mfg_prod_code_class", mode="append", properties=properties)
        logger.info(f"__________ rows to append on x_mfg_prod_code_class Without null: {selected_cols_unmatched.count()}")
        newxproductdf = spark.read.jdbc(url=url, table="x_mfg_prod_code_class", properties=properties)
        match_df = match_csv_and_x_product(csvdf, newxproductdf, columnNameToAddWithMatchDf="mfgcc")
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_mfg_prod_code_clas: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_mfg_prod_code_clas: {e}'

def compare_code_csv_with_org_chart(csvdf, xproductdf, spark, url, properties):
    
    try:
        unmatched_csv = csvdf.join(xproductdf, "code", "left_anti")
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )
        logger.info(f"__________ Rows Count in Rule 7 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.distinct()
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on org_chart: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="org_chart", mode="append", properties=properties)
        logger.info(f"__________ rows to append on org_chart Without null: {selected_cols_unmatched.count()}")
        newxproductdf = spark.read.jdbc(url=url, table="org_chart", properties=properties)
        match_df = match_csv_and_x_product(csvdf, newxproductdf, columnNameToAddWithMatchDf="prdmgr")
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_org_chart: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_org_chart: {e}'

def compare_code_csv_with_x_market_code(csvdf, x_market_code_df):
    try:
        csvdf = csvdf.withColumn('code', col('mktcd')).filter(col('mktcd').isNotNull())
        x_market_code = x_market_code_df.select('code' , 'level1', 'level2', 'level3', 'level4', 'level5')
        # Find unmatched rows
        matched_csv = csvdf.join(x_market_code, "code", "inner")

        unmatched_csv = csvdf.join(x_market_code, "code", "left_anti")
        matched_csv = matched_csv.withColumn('mktcdl1', x_market_code['level1'].cast(IntegerType()))
        matched_csv = matched_csv.withColumn('mktcdl2', x_market_code['level2'].cast(IntegerType()))
        matched_csv = matched_csv.withColumn('mktcdl3', x_market_code['level3'].cast(IntegerType()))
        matched_csv = matched_csv.withColumn('mktcdl4', x_market_code['level4'].cast(IntegerType()))
        matched_csv = matched_csv.withColumn('mktcdl5', x_market_code['level5'].cast(IntegerType()))

        # logger.info(f"__________ Rows Count in Rule 8 Before drop Duplicate based on dw: {matched_csv.count()}")
        drop_cols = ['level1', 'level2', 'level3', 'level4', 'level5', 'code']
        matched_csv = matched_csv.drop(*drop_cols)

        unmatched_csv = unmatched_csv.withColumn('mktcdl1', lit(None))
        unmatched_csv = unmatched_csv.withColumn('mktcdl2', lit(None))
        unmatched_csv = unmatched_csv.withColumn('mktcdl3', lit(None))
        unmatched_csv = unmatched_csv.withColumn('mktcdl4', lit(None))
        unmatched_csv = unmatched_csv.withColumn('mktcdl5', lit(None))

        unmatched_csv = unmatched_csv.drop('code')

        # logger.info(f"__________ rows to append on x_market_code Without null: {unmatched_csv.count()}")
        merged_df = unmatched_csv.union(matched_csv)
        return merged_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_market_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_market_code: {e}'

def compare_code_csv_with_p_pricing_region(csvdf, p_pricing_region_df, spark, url, properties):
    try:
        csvdf = csvdf.withColumn('code', col('rgcd'))
        matchws_csv = csvdf.join(p_pricing_region_df, "code", "inner")
        csv = matchws_csv.withColumn("rgcd", col("id").cast("long"))

        drop_columns = ['id', 'code', 'name', 'updated_by', 'updated_date','curr' ,'code','validationstatus', 'is_valid', 'created_date', 'created_by', 'comment']
        csv = csv.drop(*drop_columns)
        csv = csv.withColumn('code', col('part_num'))
        return csv
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_market_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_p_pricing_region: {e}'

def extract_date_from_filename(filename):
    # Extract the date part from the filename
    date_str = filename.split('_')[-1].split('.')[0]  # Extracts "20240321"
    print("Extracted Date ==================",date_str)
    
    # Extract year, month, and day from the date string
    year = date_str[:4]
    month = date_str[4:6]
    day = date_str[6:]

    return f"{year}-{month}-{day}" 

def compare_code_csv_with_p_pricing_action_detail(csvdf, spark, url, properties):
    # Read p_pricing_action_detail table
    # List of columns to check for

    try:
        columns_to_check = ["Franc_code", "brand_code", "prod_code"]

        # Check if all columns are present in the DataFrame schema
        columns_present = all(col_name in csvdf.columns for col_name in columns_to_check)
        # Use columns_present in a conditional statement
        if columns_present:
            p_pricing_action_detail_df = spark.read.jdbc(url=url, table="p_pricing_action_detail", properties=properties)
            
            view_x_yes_no_flag = spark.read.jdbc(url=url, table="view_x_yes_no_flag", properties=properties)
            # Select required columns from p_pricing_action_detail_df
            p_pricing_action_detail_df = p_pricing_action_detail_df.select('code', 'attr', 'app', 'effdt')

            view_x_yes_no_flag = view_x_yes_no_flag.select('code', 'id')

            # Inner join CSV data with p_pricing_action_detail data on 'code' column
            common_csv_df = csvdf.join(p_pricing_action_detail_df, "code", "inner")

            # Identify rows in CSV data that do not match with p_pricing_action_detail data
            uncommon_csv_df = csvdf.join(p_pricing_action_detail_df, "code", "left_anti")
            
            # Set 'fran_code', 'prod_cd5', and 'brnd_cd' to None for unmatched rows
            uncommon_csv_df = uncommon_csv_df.withColumn("fran_code", lit(None))
            uncommon_csv_df = uncommon_csv_df.withColumn("prod_cd5", lit(None))
            uncommon_csv_df = uncommon_csv_df.withColumn("brnd_cd", lit(None))

            # Extract date from filename
            date_from_file = extract_date_from_filename(inbound_spark_management['file_name'])

            # Apply conditions to filter rows in common_csv_df
            # conditions = (col("attr") == "1") & \
            #             (col("app") == "yes") & \
            #             (col("effdt").cast("string").contains(date_from_file))
            conditions = (p_pricing_action_detail_df["attr"] == view_x_yes_no_flag["id"]) & \
                    (view_x_yes_no_flag["code"] == "True") & \
                    (p_pricing_action_detail_df["app"] == "yes") & \
                    (p_pricing_action_detail_df["effdt"].cast("string").contains(date_from_file))


            # Separate rows in common_csv_df based on conditions
            unmatched_condition_p_action_detail_df = common_csv_df.filter(~conditions)
            matched_condition_p_action_detail_df = common_csv_df.filter(conditions)

            # Set 'fran_code', 'prod_cd5', and 'brnd_cd' to None for unmatched rows in matched_condition_p_action_detail_df
            unmatched_condition_p_action_detail_df = unmatched_condition_p_action_detail_df \
                .withColumn("fran_code", lit(None)) \
                .withColumn("prod_cd5", lit(None)) \
                .withColumn("brnd_cd", lit(None))

            # Drop unnecessary columns
            cols_to_drop = ['attr', 'app', 'effdt']

            # Union all dataframes to merge them into one
            merged_with_check = matched_condition_p_action_detail_df.union(unmatched_condition_p_action_detail_df)
            merged_with_check = merged_with_check.drop(*cols_to_drop)

            merged_df = merged_with_check.union(uncommon_csv_df)
        else:
            print("Not all columns are present.")
            return csvdf
            
        return merged_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_market_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_p_pricing_action_detail: {e}'

def x_brand(csvdf, xproductdf, spark, url, properties):
    try:
        csvdf.drop('code')
        csvdf.withColumn('code', col('brnd_cd'))
        
        # Find unmatched rows
        unmatched_csv = csvdf.join(xproductdf, "code", "left_anti")

        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        selected_cols_unmatched = unmatched_csv.select(
            'name', 'code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid'
        )
        logger.info(f"__________ Rows Count in Rule 12 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        logger.info(f"__________ rows to append on x_brand: {selected_cols_unmatched.count()}")
        selected_cols_unmatched.write.jdbc(url=url, table="x_brand", mode="append", properties=properties)
        logger.info(f"__________ rows to append on x_brand Without null: {selected_cols_unmatched.count()}")
        newxproductdf = spark.read.jdbc(url=url, table="x_brand", properties=properties)
        match_df = match_csv_and_x_product(csvdf, newxproductdf, columnNameToAddWithMatchDf="brnd_cd")
        match_df.drop('code')
        match_df.withColumn('code', col('part_num'))
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_market_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_x_brand: {e}'

def org_chart(csvdf, xproductdf, spark, url, properties):
    try:
        csvdf.drop('code')
        csvdf.withColumn('code', col('prdmgr'))
        
        # Find unmatched rows
        unmatched_csv = csvdf.join(xproductdf, "code", "left_anti")

 
        unmatched_csv = unmatched_csv.withColumn('name', lit("to be filled"))\
                                     .withColumn('updated_by', lit("system@spriced.com"))\
                                     .withColumn('updated_date', lit(datetime.now()))\
                                     .withColumn('validationstatus', lit("true"))\
                                     .withColumn('is_valid', lit(False))\
                                     .withColumn('created_date', lit(datetime.now()))\
                                     .withColumn('created_by', lit("system@spriced.com"))\
                                     .withColumn('comment', lit(" "))
        
        logger.info(f"__________ Rows Count in Rule 12 Before drop Duplicate based on dw: {selected_cols_unmatched.count()}")

        selected_cols_unmatched = unmatched_csv.select('name','code', 'updated_by', 'updated_date', 'created_date', 'created_by', 'comment', 'validationstatus', 'is_valid', 'pa', 'pmmi', 'pami', 'pml', 'pmlmi', 'pd',  )
        selected_cols_unmatched = selected_cols_unmatched.dropDuplicates(['code'])
        selected_cols_unmatched.write.jdbc(url=url, table="x_brand", mode="append", properties=properties)

        newxproductdf = spark.read.jdbc(url=url, table="x_brand", properties=properties)
        
        match_df = match_csv_and_x_product(csvdf, newxproductdf, columnNameToAddWithMatchDf="brnd_cd")
        match_df.drop('code')
        match_df.withColumn('code', col('part_num'))
        return match_df
    except Exception as e:
        logger.error(f"Error in join_operation_with_x_market_code: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in join_operation_with_org_chart: {e}'

def data_to_insert_staging_table(csv ,hash_table_db, url, properties):
    try:
        if 'part_num' in csv.columns:
            csv = csv.withColumn('code', concat_ws("",col('part_num'), col('rgcd')))
        else:
            raise Exception("Column 'part_num' does not exist in the CSV DataFrame")

        start_time = time.time()

        # Ensure 'code' exists in hash_table_db DataFrame
        if 'code' not in hash_table_db.columns:
            raise Exception("Column 'code' does not exist in the hash_table_db DataFrame")
        
        missing_ids_csv = csv.join(hash_table_db, "code", "left_anti")
        if missing_ids_csv.count() == 0:
            return

        # insert staging table
        csv_to_insert_stage = missing_ids_csv.drop('hash_value')
        csv_to_insert_stage = csv_to_insert_stage.drop('check_sum')
        start_time = time.time()
        # csv_to_insert_stage.printSchema()
        csv_to_insert_stage = csv_to_insert_stage.drop('code').drop('id')
        csv_to_insert_stage.write.jdbc(url=url, table="part_regional_insert_staging", mode="append", properties=properties)
        #log_time(start_time=start_time,str="csv to insert stage")
        print("-----------------------insertIntoHash-Before------------------")
        # insert hash table
        missing_code_to_hash_table = missing_ids_csv.select('code','hash_value','check_sum')
        start_hash_time = time.time()
        missing_code_to_hash_table.write.jdbc(url=url, table="part_regional_hash", mode="append", properties=properties)
        log_time(start_time=start_hash_time, str="csv hash to hash stage")

    except Exception as e:
        logger.error(f"Error in data_to_insert_staging_table: {e}")
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'

def data_to_update_staging_table(csv, hash_table_db, url, properties):
    try:
        csv = csv.withColumn('code', concat_ws("",col('part_num'), col('rgcd')))
        code_matching = hash_table_db.join(csv, "code", 'inner')
        code_matching = code_matching.drop(hash_table_db['hash_value'])
        code_matching = code_matching.drop(hash_table_db['check_sum'])
        csv = code_matching.drop(hash_table_db['id'])

        # hash_table_db.printSchema()
        # csv.printSchema()
       
        updated_hash_csv = csv.join(hash_table_db, "hash_value", "left_anti" )
        logger.info(f"data to be updated: {updated_hash_csv.count()}")
        
        if updated_hash_csv.count() == 0:
            return

        # insert update staging table
        csv_to_update_stage = updated_hash_csv.drop('hash_value','check_sum')
        start_time = time.time()
        csv_to_update_stage = csv_to_update_stage.drop('code')
        csv_to_update_stage.write.jdbc(url=url, table="part_regional_update_staging", mode="append", properties=properties)    
        log_time(start_time=start_time,str="csv to update stage")
        print("----------------------------------Check Errrrrrrrrrrrrrrr----------------")
    
        mismatch_hash_to_hash_temp = updated_hash_csv.select("code", "hash_value","check_sum")
        start_hash_time = time.time()
        mismatch_hash_to_hash_temp.write.jdbc(url=url, table="part_regional_hash_staging", mode="append", properties=properties)    
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

        #Rule---1 It will Check the csv.part_num to the x_product_code_1_digit.code  if data resent take the id and put 
        #in csv.prod_cd1 if not insert and take id and put csv.prod_cd1
        
        #Rule---2 It will Check the csv.part_num to the x_product_code_3_digit.code  if data resent take the id and put 
        #in csv.prod_cd3 if not insert and take id and put csv.prod_cd3
        
        #Rule 3-----------   It will csv.prt_num with the x_martket_code.code and will find out the  
        #value from level1 to level 5  and will insert in csv.mktcdl1 to csv.mktcdl5  if not present leave as it is 

        #Rule 4 ------  Will Go To the p_lifecycle_stage  and check csv.part_num with p_lifecycle_stage.code and 
        #if row  exits  will take id and insert into csv.lycycle and if not will insert a row and take id and put csv.lycycle

        #Rule 5 ------  Will Go To the x_sub_brand  and check csv.part_num with x_sub_brand.code and 
        #if row  exits  will take id and insert into csv.sub_brand and if not will insert a row and take id and put csv.sub_brand
        
        #Rule 6 ----------- Will Go The p_pricing Region cmpare csv.rgcd with p_pricing Region.code and will take 
        #the id of p_pricing Region and put in the csv.rgcd 

        #Rule 7 --- will recieve Franc_code,brand_code,prod_code from csv  if theasse Values are Present in CSV then Only 
        # implement This rule  Now the Rule is  to check in p_pricing_action_detail that app =true ,and  take id of attr and join 
        #with  view view_x_yes_no_flag like  p_pricing_action_detail.attr= view_x_yes_no_flag.id and check view_x_yes_no_flag.code if code =Yes 
        #and Effective data of p_pricing_action_detail.eefct= date mention in the csv  then put all prod_coe,franc_code,brand_code
        #into the csv 

        #Rule 8 ------  Will Go To the x_brand  and check csv.brnd_cd with x_brand.code and 
        #if row  exits  will take id and insert into csv.brnd_cd and if not will insert a row and take id and put csv.brnd_cd

        # csv.drop(code)
        # csv.withC('code',col('brnd_cd'))

        # csv.drop(code)
        #  csv.withC('code',col('Part_Num '))

        #Rule 8---> will take csv.brnd_cd = x_brand.code will take ids of all matching rows and put in csv.brnd_cd= x_brnd.id
        #insert new rows and take id   csv.brnd_cd 


        #Rule1
        logger.info("----------------Starting Implementation of Rule 1  x_product_code_1_digit---------------")
        log_time_start = time.time()
        x_product_code_1_df = spark.read.jdbc(url=url, table="x_product_code_1_digit", properties=properties)
        csv = x_product_code_1_digit(csvdf=csv, x_product_code_1_df=x_product_code_1_df, spark=spark, properties=properties, url=url)
        log_time(log_time_start,"-------------Time Taken in Rule 1 Implementation-------------------")
        print('csv count after 1 rule ------------------> ',csv.count())

        #Rule2
        logger.info("----------------Starting Implementation of Rule 2  x_product_code_3_digit---------------")
        log_time_start = time.time()
        x_product_code_3_df = spark.read.jdbc(url=url, table="x_product_code_3_digit", properties=properties)
        csv = x_product_code_3_digit(csvdf=csv, x_product_code_3_df=x_product_code_3_df, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "-------------Time Taken in Rule 2 Implementation-------------------")
        print('csv count after 2 rule ------------------> ',csv.count())

        #Rule 3
        logger.info("----------------Starting Implementation of Rule 3  x_market_code---------------")
        log_time_start = time.time()
        x_market_code_df = spark.read.jdbc(url=url, table="x_market_code", properties=properties)
        csv = compare_code_csv_with_x_market_code(csvdf=csv, x_market_code_df=x_market_code_df)
        log_time(log_time_start, "-------------Time Taken in Rule 3 Implementation-------------------")
        print('csv count after 3 rule ------------------> ',csv.count())

        #Rule4
        log_time_start = time.time()
        logger.info("----------------Starting Implementation of Rule 4  p_lifecycle_stage---------------")
        p_lifecycle_stage_df = spark.read.jdbc(url=url, table="p_lifecycle_stage", properties=properties)
        csv = compare_code_csv_with_p_life_cycle_stage(csvdf=csv, p_lifecycle_stage_df=p_lifecycle_stage_df, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "-------------Time Taken in Rule 4 Implementation-------------------")
        print('csv count after 4 rule ------------------> ',csv.count())

        # #Rule5
        log_time_start = time.time()
        logger.info("----------------Starting Implementation of Rule 5 p_pricing_region---------------")
        p_pricing_region_df = spark.read.jdbc(url=url, table="p_pricing_region", properties=properties)
        csv = compare_code_csv_with_p_pricing_region(csvdf=csv, p_pricing_region_df=p_pricing_region_df, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "-------------Time Taken in Rule 5 Implementation-------------------")
        print('csv count after 5 rule ------------------> ',csv.count())

        #Rule6
        log_time_start = time.time()
        logger.info("----------------Starting Implementation of Rule 6  x_sub_brand---------------")
        x_sub_brand_df = spark.read.jdbc(url=url, table="x_sub_brand", properties=properties)
        csv = compare_code_csv_with_x_sub_brand(csvdf=csv, x_sub_brand_df=x_sub_brand_df, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "-------------Time Taken in Rule 6 Implementation-------------------")
        print('csv count after 6 rule ------------------> ',csv.count())

        # Rule7
        log_time_start = time.time()
        logger.info("----------------Starting Implementation of Rule 7  p_pricing_action_detail---------------")
        csv = compare_code_csv_with_p_pricing_action_detail(csvdf=csv, spark=spark, properties=properties, url=url)
        log_time(log_time_start, "-------------Time Taken in Rule 7 Implementation-------------------")
        print('csv count after 7 rule ------------------> ',csv.count())

        csv = csv.withColumn('updated_by', lit("system@spriced.com"))\
                 .withColumn('updated_date', lit(datetime.now()))\
                 .withColumn('validationstatus', lit(" "))\
                 .withColumn('is_valid', lit(False))\
                 .withColumn('created_date', lit(datetime.now()))\
                 .withColumn('created_by', lit("system@spriced.com"))\
                 .withColumn('comment', lit(" "))\
                 .filter(col('code').isNotNull())
        
        #csv = csv.withColumn('part_uofm', lit(" "))
        #we are filtering out the data becaouse lppdt is not correct
        # print("===============Before lppdt Count=================",csv.count())

        # # lppdt column not present ACAPARTREG_1240508_150657_55335.CSV
        # if 'lppdt' in csv.columns:
        #     csv = csv.filter(year('lppdt') >= 0) 
        # print("===============After lppdt Count=================",csv.count())

    # csv = csv.filter(col('code').isNotNull())
    # logger.info(f" -------------Total  csv rows count:------------- {csv.count()}")
        # these cols are the ones comming from extra reference tables and will be removed in future.
    # drop_cols = [ 'curr', 'pa', 'pmmi', 'pami', 'pml', 'pmlmi', 'pd', 'pdmi', 'pdl1', 'pdl1mi', 'pdl2', 'pdl2mi', 'ed', 'edmi', 'prcm', 'prcmmi' ]
        #csv = csv.drop(*drop_cols)
        logger.info(f" -------------Total  csv rows count:------------- {csv.count()}")
        # Hashing Logic 
        columns_to_concat = [col for col in csv.columns if col not in ['created_date', 'updated_date']]
        csv = csv.withColumn("concatenated", concat_ws("", *columns_to_concat))
        csv = csv.withColumn("concatenated", csv['concatenated'].cast("string"))
        csv = csv.withColumn("check_sum", hash(csv['concatenated']))
        csv = csv.withColumn("hash_value", hash(csv['concatenated']))
        csv = csv.drop("concatenated")

        # cr_dt column not present ACAPARTREG_1240508_150657_55335.CSV
        # if 'cr_dt' in csv.columns:
        #     csv = csv.withColumn("cr_dt", to_timestamp(col("cr_dt"), "yyyy-MM-dd"))

        log_time_start = time.time()
        logger.info("----------------Csv Data inserting into the  Staging table By performing Hashing ---------------")
        hash_table_db = spark.read.jdbc(url=url, table="part_regional_hash", properties=properties)
        data_to_insert_staging_table(csv,hash_table_db, url, properties)
        print("____ insertion process end ______")
        log_time(log_time_start, "Time Taken for inserting data into table")
        logger.info(f"CSV Count After insert Method: {csv.count()}")

        log_time_start = time.time()
        logger.info("Update Table process start")
        # hash_table_db = spark.read.jdbc(url=url, table="part_regional_hash", properties=properties)

        # code_matching = hash_table_db.join(csv, "code", 'inner')
        # code_matching = code_matching.drop(hash_table_db['hash_value'])
        # code_matching = code_matching.drop(hash_table_db['check_sum'])
        # csv = code_matching.drop(hash_table_db['id'])
        data_to_update_staging_table(csv,hash_table_db,url,properties)
        log_time(log_time_start, "Time Taken for Update Method")
    except Exception as e:
        inbound_spark_management['status'] = 'Failed'
        inbound_spark_management["error_message"] = f'Error in main method: {e}'
        logger.error(f"Error in main method: {e}")
    finally:
        pass

def part_regional_table_step_2_insert_into_main(spark, properties, db_url):
    connection = None
    try:
        # psycopg2 Property
        logger.info("Step 2 Started . method Name part_regional_table_step_2_insert_into_main")
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

         # Load data from source table into a DataFrame
        start_time_main = time.time()
        inbound_spark_management["start_time"] = datetime.now()
        start_time = time.time()

        logger.info("Loading data from source table into a DataFrame...")
        sourceDF = spark.read.jdbc(db_url, table="part_regional_insert_staging", properties=properties)
        log_time(start_time, "Time Taken To Load The data")
        logger.info(f"Total Rows Count = {sourceDF.count()}")
        
        sourceDF = sourceDF.withColumn("prtcls", sourceDF["prtcls"].cast("integer"))
        sourceDF = sourceDF.withColumn("crcst", sourceDF["crcst"].cast("integer"))
        sourceDF = sourceDF.withColumn("lprc", sourceDF["lprc"].cast("integer"))
        sourceDF = sourceDF.withColumn("crprc", sourceDF["crprc"].cast("integer"))

        # Write the DataFrame into the target table
        start_time = time.time()
        print("First STep Reading from CSV ",sourceDF.count());
        logger.info("Writing data into the target table...")

        logger.info("Writing data into the target table...")
        columns_to_insert = [col for col in sourceDF.columns if col not in ['id', 'code']]
        print("Second Step  Removing The ID ");
        sourceDF.select(columns_to_insert).write.jdbc(db_url, table="part_regional", mode="append", properties=properties)
        # Insert data count into the logging dictionary
        inbound_spark_management["data_insert_count"] = sourceDF.count()
         # Truncate staging table
        cursor.execute("TRUNCATE TABLE part_regional_insert_staging")
        connection.commit()
        logger.info("Staging table truncated successfully.")
        log_time(start_time_main, "-----------Step Two Loading data from insert Staging to main Table---------------------")

        # Log success message
        inbound_spark_management["status"] = "Success"
        inbound_spark_management["success_message"] = "part_regional_table_step_2_insert_into_main executed successfully"
    except Exception as e:
        logger.error(f"Error in part_regional_table_step_2_insert_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(e)
    finally:
        if connection:
            cursor.close()
            connection.close()

def part_regional_table_step_3_update_into_main():
    connection=None

    try:
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        cursor = connection.cursor()

        start_time_main = time.time()

        sql_update_query = """
                BEGIN;

                UPDATE part_regional 
                SET
                        name = source.name,
                        brnd_cd = source.brnd_cd,
                        gbb = source.gbb,
                        lfcycle = source.lfcycle,
                        mktcd = source.mktcd,
                        mfgcc = source.mfgcc,
                        prod_cd1 = source.prod_cd1,
                        prod_cd3 = source.prod_cd3,
                        mfgpdcd = source.mfgpdcd,
                        prdmgr = source.prdmgr,
                        part_num = source.part_num,
                        long_ptno = source.long_ptno,
                        projcst = source.projcst,
                        prod_cd5 = source.prod_cd5,
                        cr_dt = source.cr_dt,
                        shrt_desc = source.shrt_desc,
                        long_desc = source.long_desc,
                        fran_code = source.fran_code,
                        prtcls = source.prtcls,
                        sup_prt = source.sup_prt,
                        prtstat = source.prtstat,
                        popcdny = source.popcdny,
                        popcdcy = source.popcdcy,
                        pllqty = source.pllqty,
                        drop_ship = source.drop_ship,
                        cat_cd = source.cat_cd,
                        cc1 = source.cc1,
                        cc2 = source.cc2,
                        cc3 = source.cc3,
                        cc4 = source.cc4,
                        cc5 = source.cc5,
                        sac = source.sac,
                        uac = source.uac,
                        stdcst = source.stdcst,
                        crcst = source.crcst,
                        lprc = source.lprc,
                        crprc = source.crprc,
                        stdmcrcst = source.stdmcrcst,
                        lpp_prc = source.lpp_prc,
                        lppqty = source.lppqty,
                        lppdt = source.lppdt,
                        lppvndr = source.lppvndr,
                        postdt = source.postdt,
                        poenddt = source.poenddt,
                        povndr = source.povndr,
                        pofgt = source.pofgt,
                        popblp = source.popblp,
                        popblq = source.popblq,
                        popt = source.popt,
                        pocurr = source.pocurr,
                        pofdc = source.pofdc,
                        uytd = source.uytd,
                        uy1qty = source.uy1qty,
                        uy2qty = source.uy2qty,
                        uy3qty = source.uy3qty,
                        uy4qty = source.uy4qty,
                        uy5qty = source.uy5qty,
                        mfgnr = source.mfgnr,
                        mfgcommcd = source.mfgcommcd,
                        mfgfamcd = source.mfgfamcd,
                        pnf_fmsi = source.pnf_fmsi,
                        mktcdl1 = source.mktcdl1,
                        mktcdl2 = source.mktcdl2,
                        mktcdl3 = source.mktcdl3,
                        mktcdl4 = source.mktcdl4,
                        mktcdl5 = source.mktcdl5,
                        kit_flag = source.kit_flag,
                        cr_gp = source.cr_gp,
                        core_part = source.core_part,
                        origin = source.origin,
                        nonrtn = source.nonrtn,
                        harcode = source.harcode,
                        ldtime = source.ldtime,
                        upc = source.upc,
                        vmrs = source.vmrs,
                        height = source.height,
                        prtlen = source.prtlen,
                        prtwid = source.prtwid,
                        planner = source.planner,
                        buyer = source.buyer,
                        cystdmtl = source.cystdmtl,
                        dutfrgexc = source.dutfrgexc,
                        dutycst = source.dutycst,
                        frgtcst = source.frgtcst,
                        mfglabcst = source.mfglabcst,
                        whburcst = source.whburcst,
                        mfgbcst = source.mfgbcst,
                        warbcst = source.warbcst,
                        pckcst = source.pckcst,
                        corqty = source.corqty,
                        vndrnm = source.vndrnm,
                        vndrcd = source.vndrcd,
                        rgcd = source.rgcd,
                        sub_brand = source.sub_brand,
                        updated_by = source.updated_by,
                        updated_date = source.updated_date,
                        validationstatus = source.validationstatus,
                        is_valid = source.is_valid,
                        created_date = source.created_date,
                        created_by = source.created_by,
                        comment = source.comment,
                        part_uofm = source.part_uofm,
                        rg_part = source.rg_part,
                        mktcd_d = source.mktcd_d,
                        mfgpdd = source.mfgpdd,
                        mfgcd = source.mfgcd,
                        cbseg = source.cbseg,
                        sup_dt = source.sup_dt
                    FROM
                        part_regional_update_staging AS source
                    WHERE
                        part_regional.part_num = source.part_num AND
                        part_regional.rgcd = source.rgcd;

                    TRUNCATE TABLE part_regional_update_staging;

                    UPDATE part_regional_hash
                    SET
                        hash_value = temp.hash_value
                    FROM
                        part_regional_hash_staging temp
                    WHERE
                        part_regional_hash.code = temp.code;

                    TRUNCATE TABLE part_regional_hash_staging;

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
        logger.info("Update and truncation of staging tables completed successfully.====")
        log_time(start_time_main, "------------Step Thre Updating the Data From Staging Update to main Table Completed ----------")
    except Exception as e:
        logger.error(f"Error in part_regional_table_step_3_update_into_main: {e}")
        inbound_spark_management["status"] = "Failure"
        inbound_spark_management["error_message"] = str(f'Error in part_regional_table_step_3_update_into_main: {e}')
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
        try:
            logger.info("Executing insert query...================= ")
        # Execute the insert query (this is just a placeholder)
            cursor.execute(insert_query)
        except Exception as e:
            logger.error("Error inserting data: %s", e)
        # cursor.execute(insert_query)
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

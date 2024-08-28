# main has been called in 
def check_and_download_file(sftp):
-----------------------------------------------------------------------------------------------------------

# to add rgcd based on the processed directory e.g AUS or CA

 >>1) add region as param in the following method
        
        def read_csv_data(path_to_csv, spark, region):
         # and in this method add
            if 'rgcd' not in df.columns:
            logger.info("region found as ######################################################: %s",region)
            if region == 'AUS':
             df = df.withColumn("rgcd", lit(region))
            else: 
             df = df.withColumn("rgcd", lit("CA"))
            
 >>2) add region as param in the main method

       >>2.1) def main(local_file_path,spark,properties,url,region):
               #here  read_csv_data method has been called so add region there also
                 csv = read_csv_data(path_to_csv=local_file_path, spark=spark,region=region)

>>3) add region as param  where the main method has been called 

        def check_and_download_file(sftp):
            if output_csv_path:
             #add region as param
             main(output_csv_path, spark, properties, db_url,region)

------------------------------------------------------------------------------------------------------------------------
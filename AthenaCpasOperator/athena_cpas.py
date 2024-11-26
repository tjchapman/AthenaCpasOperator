import re
import time
import random
import string
import logging
from typing import Dict, List
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from AthenaCpasOperator import s3_client, s3_resource
from AthenaCpasOperator import glue_client, athena_client


logger = logging.getLogger(__name__)

class CpasOperator():
    """
    Refactored and tweaked from: https://github.com/civitaspo/airflow-plugin-glue_presto_apas

    This class will create a 'partition as select' for an Athena table in S3. It utilises boto3 to create temp tables for the partition, perform some checks and write the partition to the prod table. 
    
    Args: 
    - source: this is the data catalog 
    - db: the databse 
    - table name: the table you want to write the partitions to
    - sql string: the sql string that is being used as the select statement for the partition. e.g. 'Select * from [a table] where date = {v_date_variable}'
    - partition values: partitoin values in dict format 
    - save_mode: how do you want to deal with partitions if they already exists. Options are: skipifexists, errorifexists, ignoresavemode, overwrite. Default behaviour is to overwrite. 
    
    """

    def __init__(
            self,
            source: str,
            db: str,
            table: str,
            sql: str,
            partition_kv: Dict[str, str],
            location: str = None,
            fmt: str =  'parquet',
            additional_properties: Dict[str, str] = {},
            save_mode='overwrite',
            *args):
        
        self.source = source
        self.db = db
        self.table = table
        self.sql = sql
        self.partition_keys: List[str] = list(partition_kv.keys())
        self.partition_values: List[str] = list(partition_kv.values())
        self.location = location
        self.fmt = fmt
        self.additional_properties = additional_properties
        self.save_mode = save_mode

    @staticmethod
    def _random_str(size: int = 10, chars: str = string.ascii_uppercase + string.digits) -> str:
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def _extract_s3_uri(uri):
        m = re.search('^s3://([^/]+)/(.+)', uri)
        if not m:
            raise (f"URI[{uri}] is invalid for S3.")
        bucket = m.group(1)
        prefix = m.group(2)
        return bucket, prefix
    
    def query_athena(self, query_string, output_results: bool =False):
        ## will run in 'primary' athena workgroup
        logger.info(f'Running query string: {query_string}')

        query_response = athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={"Database": self.source},
            ResultConfiguration={
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
            },
        )
        while True:
            try:
                # This function only loads the first 1000 rows into the variable
                query_results = athena_client.get_query_results(
                    QueryExecutionId=query_response["QueryExecutionId"]
                )   

                if query_results:
                    runtime_stats = athena_client.get_query_runtime_statistics(QueryExecutionId=query_response["QueryExecutionId"])
                    exec_time = runtime_stats['QueryRuntimeStatistics']['Timeline']['TotalExecutionTimeInMillis']

                    if exec_time < 1000:
                        exec_time_fmt = str(exec_time) + ' milliseconds' #milliseconds
                    elif exec_time >=1000 and exec_time < 60000:
                        exec_time_fmt = str((exec_time/1000)%60) + ' seconds' #seconds
                    else:
                        exec_time_fmt = str((exec_time/1000*60)%60) + ' minutes' #minutes

                    logger.info(f'Query ran successfully in {exec_time_fmt}...')
                    
                    if output_results == True:
                        return query_results    
                        
                break
            except Exception as err:
                if "not yet finished" in str(err):
                    time.sleep(0.001)
                else:
                    raise err
                

    
    def _desc_columns_by_query(self, sql: str):
        tmp_table = f"operator_temp_apas_" \
            f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}" \
            f"_{self._random_str()}"
        columns: List[Dict[str, str]] = []
        try:
            create_stmt = f"CREATE TABLE {self.db}.{tmp_table} AS {sql}"
            logger.info(create_stmt)
            self.query_athena(query_string=create_stmt)
            describe_stmt = f"DESCRIBE {self.source}.{self.db}.{tmp_table}"
            records = self.query_athena(query_string=describe_stmt, output_results=True)
 
            listed_results = [[data.get('VarCharValue') for data in row['Data']]
            for row in records['ResultSet']['Rows']]
 
            logger.info(listed_results)
            for c in listed_results:
                replaced = c[0].replace('\t', ',')
                stripped =re.sub(r'[\s+]', '', replaced).rstrip(',').split(',')
                col_name = stripped[0]
                col_type = stripped[1]
                columns.append({
                    'name': col_name,
                    'type': col_type,
                })

        finally:
            drop_stmt = f"DROP TABLE {self.db}.{tmp_table}"
            logger.info(drop_stmt)
            self.query_athena(query_string=drop_stmt)
        logger.info(columns)
        return columns
    
    
    def _prepare_create_table_properties_stmt(self, prefix=None):
        props = self.additional_properties.copy()
        props['external_location'] = f"'{self.location}{prefix}'"
        props['format'] = f"'{self.fmt}'"
        props_stmts = []
        for k, v in props.items():
            props_stmts.append(f"{k} = {v}")
        return ','.join(props_stmts)
    

    def does_partition_exist(self, db: str, table: str, part_values):
        """Test if a specific partition exists in a database.table"""
        try:
            logger.info(f"Searching for {part_values} in {db}.{table}")
            glue_client.get_partition(DatabaseName=db, TableName=table, PartitionValues=part_values)
            logger.info('Partition does exist')
            return True
        
        except ClientError as e:
            if e.__class__.__name__ == 'EntityNotFoundException':
                logger.info('Partition does not exist')
                return False
            raise e
        
        
    def get_partition_keys(self, db: str, name: str) -> List[str]:
        table = self.get_table(db=db, name=name)
        partition_keys = []
        for p in table['PartitionKeys']:
            partition_keys.append(p['Name'])
        return partition_keys
        
        
    def _get_ordered_partition_kv(self):
        ordered_partition_values = []
        for pk in self.get_partition_keys(db=self.db, name=self.table):
            ordered_partition_values.append({
                'key': pk,
                'value': self.partition_values[self.partition_keys.index(pk)],
            })
        return ordered_partition_values
    

    def get_table(self, db: str, name: str) -> dict:
        args = {
            'DatabaseName': db,
            'Name': name,
        }
        return glue_client.get_table(**args)['Table']
    

    def delete_table(self, db: str, name: str) -> None:
        args = {
            'DatabaseName': db,
            'Name': name
        }
        glue_client.delete_table(**args)


    def delete_s3_folder(self, bucket: str, prefix: str):
        objects_to_delete = s3_resource.meta.client.list_objects(Bucket=bucket,
                                                         Prefix=prefix)

        delete_keys = {'Objects' : []}
        delete_keys['Objects'] = [{'Key' : k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]

        s3_resource.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)
        logger.info(f"Deleted {bucket}/{prefix} from S3")
    

    
    def delete_partition(self, db: str, table_name: str, partition_values: List[str]) -> None:
        args = {
                'DatabaseName': db,
                'TableName': table_name,
                'PartitionValues': partition_values
            }
        
        return glue_client.delete_partition(**args)
        


    def convert_table_to_partition(
                self,
                src_db: str, src_table: str,
                dst_db: str, dst_table: str,
                partition_values: List[str]):
            sd = self.get_table(db=src_db, name=src_table)['StorageDescriptor']
        
            args = {
                'DatabaseName': dst_db,
                'TableName': dst_table,
                'PartitionInput': {
                    'Values': partition_values,
                    'StorageDescriptor': sd,
                }
            }
            glue_client.create_partition(**args)
            self.delete_table(db=src_db, name=src_table)   


    def wait_until_objects_created(self, obj_filter=lambda obj: True):

        bucket, prefix = self._extract_s3_uri(self.location)
        created_objects = []
        bucket_resource = s3_resource.Bucket(bucket)

        for obj in bucket_resource.objects.filter(Prefix=prefix):
            if obj_filter(obj):
                logging.info(f"Found a created object[{obj}"
                             f", last_modified:{obj.last_modified}"
                             f", length:{obj.size}].")
                created_objects.append(obj)
            else:
                logging.info(f"Skip a Object[{obj}"
                             f", last_modified:{obj.last_modified}"
                             f", length:{obj.size}] "
                             f"as is not created in the current execution.")
        if not created_objects:
            assert 1==0, f"No objects are found in {self.location}."
        logging.info(f"Created objects are found in {self.location}.")

    def _gen_partition_location(self):
            
            table_location = self.get_table(db=self.db, name=self.table)['StorageDescriptor']['Location']
            if not table_location.endswith('/'):
                table_location = table_location + '/'

            partition_elems: List[str] = []
            for h in self._get_ordered_partition_kv():
                partition_elems.append(f"{h['key']}={h['value']}")

            return table_location + '/'.join(partition_elems)

    def pre_execute(self) -> None:
        if not self.location:
            self.location = self._gen_partition_location()
        if not self.location.endswith('/'):
            self.location = self.location + '/' 
        logger.info(f"self.location= {self.location}")


    def _processable_check_n_prepare_location(self) -> bool:

        bucket, prefix = self._extract_s3_uri(self.location)
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if resp['KeyCount'] > 0:
            if self.save_mode == 'skipifexists':
                logging.info(f"Skip this execution because location[{self.location}] exists"
                             f" and save_mode[{self.save_mode}] is defined.")
                return False
            elif self.save_mode == 'errorifexists':
                raise ValueError(f"Raise a exception because location[{self.location}] exists"
                                  f" and save_mode[{self.save_mode}] is defined.")
            elif self.save_mode == 'ignoresavemode':
                logging.info(f"Continue the execution regardless that location[{self.location}] exists"
                             f" because save_mode[{self.save_mode}] is defined.")
            if self.save_mode == 'overwrite':
                logging.info(f"Delete all objects in location[{self.location}]"
                             f" because save_mode[{self.save_mode}] is defined.")
                for obj in resp['Contents']:
                    logger.info(f"Deleting {obj['Key']}")
                    s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
            else:
                raise 
        else:
            logger.info("No old data to delete...")
        return True


    def execute(self) -> None:
        """
        Runs everything needed to create a CPAS... 
        """
     
        ## Run pre execute sequence to make pretty table location with partition values embedded
        self.pre_execute()
        if not self._processable_check_n_prepare_location():
            return
        
         
        ## Creating temp table
        col_stmts: List[str] = []
        for c in self._desc_columns_by_query(self.sql):
            col_stmts.append(f"{c['name']} {c['type']}")
        logging.info(f"Detect columns{col_stmts}")

        tmp_table = f"operator_temp_apas_" \
            f"_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}" \
            f"_{self._random_str()}"
        
        logger.info(tmp_table)

        bucket, prefix = self._extract_s3_uri(self.location)
        logger.info(f'Bucket={bucket}, Prefix={prefix}')

        ordered_partition_kv = self._get_ordered_partition_kv()
        ordered_partition_values = []
        for h in ordered_partition_kv:
            ordered_partition_values.append(h["value"])

        dummy_fname = '_DUMMY'
        try:
            logger.info(f"Upload '{dummy_fname}' -> s3://{bucket}/{prefix + dummy_fname}")
            s3_client.put_object(Bucket=bucket, Key=prefix+dummy_fname+'/') 
            prop_stmt = self._prepare_create_table_properties_stmt() 
            # create_sql = f"CREATE TABLE {self.db}.{tmp_table} ( {','.join(col_stmts)} )" \
            #         f" WITH ( {prop_stmt} )" 
            
            create_sql = f"CREATE EXTERNAL TABLE IF NOT EXISTS {self.db}.{tmp_table} ( {','.join(col_stmts)} )" \
                    f""" ROW FORMAT SERDE 
                        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
                        STORED AS INPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
                        OUTPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                        LOCATION 's3://{bucket}/{prefix}' """
            

                            
            results = self.query_athena(query_string=create_sql, output_results=True)
            logger.info(results)
            if not results:
                raise f'Failed to run sql: {create_sql}'
            
            query_start_at = datetime.now(timezone.utc)

            insert_sql = f"INSERT INTO {self.db}.{tmp_table} {self.sql}"
            results = self.query_athena(query_string=insert_sql, output_results=True)
            logger.info(results)
            update_count = results['UpdateCount']
            logger.info(f"Update count: {update_count}")
            if not results:
                raise f'Failed to run sql: {insert_sql}'
            
            affected_rows_sql =  f"SELECT COUNT(*) FROM {self.db}.{tmp_table}"
            affected_rows = self.query_athena(query_string=affected_rows_sql, output_results=True)
            listed_results = [[data.get('VarCharValue') for data in row['Data']]
            for row in affected_rows['ResultSet']['Rows']]

            affected_rows_num = int(listed_results[1][0])
            logger.info(f"Affected rows: {affected_rows_num}")
            assert affected_rows_num == update_count, "Mismatch between insert count and table row count"

        
            logger.info(f"query_start_at: {query_start_at}")
            if affected_rows_num > 0:
                self.wait_until_objects_created(
                        obj_filter=lambda obj: obj.last_modified > query_start_at and obj.size > 0
                    )
            
            ## check if partition exists and delete if so
            if self.does_partition_exist(db=self.db,
                                    table=self.table,
                                    part_values=ordered_partition_values):
    
                logger.info(f"Deleting old partition {ordered_partition_kv}")
                self.delete_partition(db=self.db,
                                  table_name=self.table,
                                  partition_values=ordered_partition_values)
            
            ## convert temp table to partition in prod table
            logger.info(f"Converting table {self.db}.{tmp_table}"
                    f" to partition {ordered_partition_kv}")
        
            self.convert_table_to_partition(src_db=self.db,
                                        src_table=tmp_table,
                                        dst_db=self.db,
                                        dst_table=self.table,
                                        partition_values=ordered_partition_values)
            
            ## tidy up after ourselves in s3
            self.delete_s3_folder(bucket=bucket, prefix=prefix+dummy_fname+'/')

        except Exception as e:
            logger.info(e)
            raise e
        
        

  
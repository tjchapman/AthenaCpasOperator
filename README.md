# Athena Create Partition As Select Helper

This package allows you to insert data and create partitions for an Athena table, using a select query. This improves on the 'Insert into' method as you can choose custom behaviour on how old partitions are dealt with, meaning data will not be duplicated and creates idempotency.

This is tweaked and refactored from an airflow plug-in that allows you to do the same thing. With this pacakge, we no longer need airflow and can have the same capabilites, minus the bloat.

You'll need an AWS account with provisioned keys for Glue, S3, Athena.

Link to open source Airflow plug-in: https://github.com/civitaspo/airflow-plugin-glue_presto_apas

## Example on how to use:

- Example found in 'main.py'

```
import logging
from AthenaCpasOperator.athena_cpas import CpasOperator

logger=logging.getLogger(__name__)

def main():
    logging.basicConfig(filename='log_file.log', level=logging.INFO)

    cpas_operator = CpasOperator(source="your-data-catalog",
                                  db="your-database",
                                  table="your-target-table",
                                  sql="select * from your-database.your-source-table",
                                  partition_kv={'Key1': 'Value1', 'Key2':'Value2'},
                                  save_mode='overwrite'
    )

    cpas_operator.execute()

if __name__ == "__main__":
    main()

```

### To run example found in main.py:

1. Enter own inputs (database, table, keys, sql string etc.)
2. `make run` -> to run it
3. `make clean` -> to tidy up after

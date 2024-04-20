# Athena Create Partition As Select Helper

This package allows you to insert data and create partitions for an Athena table, using a select query. This improves on the 'Insert into' method as you can choose custom behaviour on how old partitions are dealt with, meaning data will not be duplicated and creates idempotency.

This is tweaked and refactored from an airflow plug-in that allows you to do the same thing. With this pacakge, we no longer need airflow and can have the same capabilites, minus the bloat.

You'll need an AWS account with provisioned keys for Glue, S3, Athena.

Link to plug-in: https://github.com/civitaspo/airflow-plugin-glue_presto_apas

## Install requirements:

```
pip install -r requirements.txt
```

## Example on how to use:

- Example found in 'main.py'

```
from AthenaCpasOperator.athena_cpas import CpasOperator
import logging

logger=logging.getLogger(__name__)

def main():
    logging.basicConfig(filename='log_file.log', level=logging.INFO)

    cpas_operator = CpasOperator(source="your-data-catalog",
                                  db="your-database",
                                  table="your-target-table",
                                  sql="sql-select-string",
                                  partition_kv="{'Key1': 'Value1', 'Key2':'Value2'}"
    )

    cpas_operator.execute()

if __name__ == "__main__":
    main()

```

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
import shutil
import os
import pyspark
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType
import numpy as np
import time
import datetime

from upload import ftp_upload

FTP_HOST = 'ftp.ebi.ac.uk'
FTP_DIR = '/pub/databases/opentargets/platform/21.11/output/etl/json/'
DATA_DIR = os.path.join(os.path.abspath(""), 'data')
OUTP_DIR = os.path.join(os.path.abspath(""), 'outp')
KEY_DIRS = {'diseases', 'targets', 'evidence/sourceId=eva/'}


if __name__ == "__main__":
    ftp_upload(ftp_host=FTP_HOST, ftp_dir=FTP_DIR, outp_dir=DATA_DIR, key_dirs=KEY_DIRS)

    sqlcontext = pyspark.sql.SparkSession.builder.appName(
        "parser_app"
    ).config(
        'spark.debug.maxToStringFields', '100000'
    ).config(
        'spark.sql.debug.maxToStringFields', '100000'
    ).config(
        'spark.oap.cache.strategy', 'not_support_cache'
    ).getOrCreate()

    df_evidence = sqlcontext.read.format('json').load('./data/evidence/sourceId=eva/')
    print(f"Dataset-evidence size is: {df_evidence.count()}")
    df_evidence = df_evidence.na.drop(subset=['diseaseId', 'targetId'])
    print(f"Dataset-evidence size after 'na.drop' is: {df_evidence.count()}")

    df_evidence = df_evidence.groupBy(
        'diseaseId', 'targetId'
    ).agg(
        func.percentile_approx('score', 0.5).alias('median_score'),
        func.sort_array(func.collect_list('score'), asc=False).alias('score_lst')
    )

    df_evidence = df_evidence.withColumn("first_three", func.slice("score_lst", start=1, length=3))

    df_targets = sqlcontext.read.format('json').load('./data/targets')
    df_diseases = sqlcontext.read.format('json').load('./data/diseases')

    df_join = df_targets.join(df_evidence, df_targets.id == df_evidence.targetId).drop('id')
    df_join = df_join.join(df_diseases, df_join.diseaseId == df_diseases.id)
    df_join = df_join.select(
        'targetId', 'approvedSymbol', 'diseaseId', 'name', 'median_score', 'first_three'
    ).sort(
        func.asc('median_score')
    )
    print(f"Dataset joined-table size is: {df_join.count()}")

    if os.path.exists(OUTP_DIR):
        shutil.rmtree(OUTP_DIR)
    os.makedirs(OUTP_DIR, exist_ok=True)

    df_join.write.json(os.path.join(OUTP_DIR, 'outp_join'))

    df_join_diseases = df_join.groupBy('targetId').agg(
        func.sort_array(func.collect_list('diseaseId')).alias('diseaseId_lst')
    )
    flen = func.udf(lambda s: len(s), IntegerType())
    df_join_targets = df_join_diseases.withColumn(
        "diseases_count", flen(df_join_diseases.diseaseId_lst)
    ).sort(
        func.desc('diseases_count')
    )
    print(f"Dataset diseases-table size is: {df_join_targets.count()}")
    print(f"The count of targets with more than 2 diseases in the diseases-table is: "
          f"{df_join_targets.filter(df_join_targets.diseases_count >= 2).count()}")

    df_join_targets.write.json(os.path.join(OUTP_DIR, 'outp_diseases'))

    start = time.time()
    diseases_array = np.array(df_join_targets.select('diseaseId_lst').collect())

    targets_pairs_count = 0
    for i in range(len(diseases_array)):
        for j in range(i+1, len(diseases_array)):
            if len(set(diseases_array[i][0]) & set(diseases_array[j][0])) >= 2:
                targets_pairs_count += 1

    end = time.time()
    print(f"Targets pairs count (with more than 2 diseases in the intersection) is: {targets_pairs_count}" 
          f" / measured time is: {datetime.timedelta(seconds=end - start)}")

    start = time.time()
    df_join_targets_copy = df_join_targets.withColumnRenamed(
        "targetId", "targetId_copy"
    ).withColumnRenamed("diseaseId_lst", "diseaseId_lst_copy")

    df_result = df_join_targets.join(
        df_join_targets_copy,
        [func.col('targetId') != func.col('targetId_copy'),
         func.size(func.array_intersect(func.col('diseaseId_lst'), func.col('diseaseId_lst_copy'))) >= 2],
    ).select(
        func.sort_array(func.array(func.col('targetId'), func.col('targetId_copy')))
    ).alias('pair').distinct()

    end = time.time()
    print(f"Targets pairs count (with more than 2 diseases in the intersection) is: {df_result.count()}"
          f" / measured time is: {datetime.timedelta(seconds=end - start)}")

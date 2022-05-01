# dataset_parsing_via_apache_spark

## Packages for installation:

```
pip install pyspark
sudo apt-get update
sudo apt-get install openjdk-8-jdk
```

## How to use:

`ftp_upload` function from `upload.py` is uploading *KEY_DIRS* data folders from *FTP_HOST* into local *DATA_DIR*<br>
`main.py` script uploading data from FTP, filtering them and saving into the *OUTP_DIR*

## Output data:

`outp_join.json` from `./outp/outp_join` folder contains resulting joined-table table (saved in JSON) after following filtering:
1. Getting the `diseaseId`, `targetId`, and `score` fields from the evidence data (`evidence/sourceId=eva/` from FTP)
2. Calculation the median and 3 greatest `score` values for each `targetId` and `diseaseId` pair
3. Joining the targets and diseases datasets on the `targetId`=`target.id` and `diseaseId`=`disease.id` fields
4. Adding the `target.approvedSymbol` and `disease.name` fields into this result table

`outp_diseases.json` from `./outp/outp_diseases` folder contains target-diseases table received on joined-table<br>
An answer for the following question is in *Output log* chapter:
1. Count how many target-target pairs share a connection to at least two diseases

## Output log:

```
Count of /home/mi/PycharmProjects/dataset_parsing_via_apache_spark/data/targets files is: 200
Count of /home/mi/PycharmProjects/dataset_parsing_via_apache_spark/data/diseases files is: 16
Count of /home/mi/PycharmProjects/dataset_parsing_via_apache_spark/data/evidence/sourceId=eva/ files is: 200

Dataset-evidence size is: 1106942
Dataset-evidence size after 'na.drop' is: 1106942
Dataset joined-table size is: 25132
Dataset diseases-table size is: 5954
The count of targets with more than 2 diseases in the diseases-table is: 3731
Targets pairs count (with more than 2 diseases in the intersection) is: 350414 / measured time is: 0:02:21.479624
Targets pairs count (with more than 2 diseases in the intersection) is: 350414 / measured time is: 0:00:00.707054
```


The dataset is in ```.csv``` format and, although the official documentation specifies different data types for each column, in the raw dataset everything is being treated as text.

To solve this and optimize storage as recommended by AWS, we send the data to an AWS Glue Job in order to correct the data types and convert the format to ```.parquet```. This is the AWS Glue pipeline:

<img src="../images/dataCleaningPipeline.png" width="400" >

At this point, the **Double** and **Int** values were already correctly processed, so we could start modeling the information in HIVE. However, the date columns were still missing.

The easiest solution would have been to use PySpark directly, but since this process inherits the properties of the table created in HIVE, and to maintain an organized pipeline, we added a transformation step to the ETL process where the date format is corrected using a SQLQuery Transform with the following query: [Date-format-correction.sql](./dateFormatCorrection.sql)

<img src="../images/newDataCleaningPipeline.png" width="400">


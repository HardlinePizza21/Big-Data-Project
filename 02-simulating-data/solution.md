Using the ```LoadLocalDataIntoRDS.py``` script to load the 6.5 GB dataset from my local machine to the RDS MariaDB instance — a micro instance in the same availability zone as the EMR cluster (zone b).

<img src="../images/Screenshot from 2026-05-17 16-42-29.png" width="800">

Through this process I came to understand that processing a CSV with nearly 32.8 million entries to upload it to a relational database all the way up to AWS is highly inefficient. The goal of this step is to simulate the presence of data at that point (the RDS), but realistically all that information would have first arrived at the database through a more robust pipeline, one that required more time and stronger data quality assurance.

After securing a better connection I was able to upload the data to the RDS and migrate it to S3 using the AWS CLI through a script that sends a snapshot of the database to S3.

<img src="../images/Screenshot from 2026-05-17 17-40-21.png" width="2000" height="150">

And now into the S3 we got.

<img src="../images/Screenshot from 2026-05-17 17-45-05.png" width="1000">

A more automated way to migrate the data would be to use the AWS Database Migration Service(DMS), but for this workshop it is sufficient to use an EC2 instance that automatically migrates the data, since the database will not continue to grow.
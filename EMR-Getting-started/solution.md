first we should create a S3 bucket in the same availabilty region as the Cluster to procces the data

Then we upload the data set to the S3, and create a step in the cluster to procces the data.

In this case ```health_violations.py``` is an script that will be executed over the data, and this processes are calles *steps* in the cluster

<img src="../images/Screenshot from 2026-05-10 08-34-58.png">

The steps are also stored in the S3, and the arguments ```--data_source s3://tutorial-bucket-eafit/food_establishment_data.csv``` ```--output_uri s3://tutorial-bucket-eafit/myOutputFolder``` is to indicate where the data is and where the result should go, Is important to take in count that, there is not yet, an intelligent way to automatically aply those *steps* over the data
# spark-project

Master M2 – Université Grenoble Alpes & Grenoble INP  2020

This assignment is about analyzing a large dataset using Apache Spark. The dataset has been made available by Google. It includes data about a cluster of 12500 machines, and the activity
on this cluster during 29 days. This lab is an opportunity to process large amount of data and to implement complex data analyses using Spark. It is also an opportunity to better understand how
computing resources are used in a Cloud environment.


About the dataset
The data we will study in this lab have been released by Google in 2011. It represents 29 days of activity in a large scale Google machine (a cluster) featuring about 12.5k machines. It includes
information about the jobs executed on this cluster during that period as well as information about the corresponding resource usage.
The starting point to find information about the dataset is the following web page: https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md. A documentation including a detailed description of the dataset written by people from Google can be found in this document. We refer to this document as the Google Documentation in the following.
We give additional information about the dataset in Section Description of the dataset

Downloading the data
The data we are going to manipulate are publicly available on Google Cloud Storage. We have to use the tool GSUtil to download them:
• Information about GSUtil can be found here: https://cloud.google.com/storage/docs/gsutil
• We recommend you to install GSUtil by directly downloading the archive, as described here: https://cloud.google.com/storage/docs/gsutil_install#alt-install
• The Google Documentation includes a section that describes how to download the data (see Section "Downloading the data" at the end of the document). In a few words:
– gsutil ls gs://clusterdata-2011-2/ allows you to see the available files
– gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz ./
will copy the file part-00000-of-00001.csv.gz in the current directory.

Important comment: The total size of the dataset is huge (41 GB of data). Do not copy all the data at once. Large data tables have been divided into multiple files. It allows you to copy just one
file for one table and to test your programs on small sub-parts of the data.

Description of the dataset
As already mentioned, a detailed description of the data we are studying is available in the Google Documentation. Since this document can be difficult to read, we provide you with an overview of the data in the following. However, to decide which analyses you are going to conduct, we strongly encourage you to read the Google Documentation. Indeed, we do not discuss all available data in the following.
Important : The file schema.csv available on the data repository describes each of the field included in all CSV files comprised in the dataset. In case of doubt, always refer to this file.

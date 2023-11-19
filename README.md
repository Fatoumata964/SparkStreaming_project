# SparkStreaming_project
Here we are implementing a Fast Data solution to predict cancer disease risks in real time.
The data used during this project is available here: https://github.com/aaaaaa/etudidant/blob/main/cancer.csv

For each cancer observation, we have the following informations:
* Sample code number: id number
* Clump Thickness: 1 - 10
* Uniformity of Cell Size: 1 - 10
* Uniformity of Cell Shape: 1 - 10
* Marginal Adhesion: 1 - 10
* Single Epithelial Cell Size: 1 -10
* Bare Nuclei: 1 - 10
* Bland Chromatin: 1 - 10
* Normal Nucleoli: 1 -10
* Mitoses: 1 -10
* Class: (2 for benign, 4 for malignant)
  



We used the following architecture for this:

<img width="409" alt="Archi" src="https://github.com/Fatoumata964/SparkStreaming_project/assets/60388963/f5683efe-e8bb-4782-8e4e-bb3909742fcc">


* nifi-1.23.1
* kafka-3.6.0
* sparkVersion = "3.0.0"
* scalaVersion := "2.12.10"


We obtain the following results via OpenSearch:
<img width="950" alt="dash3" src="https://github.com/Fatoumata964/SparkStreaming_project/assets/60388963/dabe868b-7c8c-4c40-95eb-59248ecf1314">

<img width="562" alt="Dash2" src="https://github.com/Fatoumata964/SparkStreaming_project/assets/60388963/0e5bb1ea-2b35-44a5-9b3d-29d7e5375c27">


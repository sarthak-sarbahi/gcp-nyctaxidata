### Data engineering project with GCP

1. Getting the data
- Open source NYC tax data identified from [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Created a bucket in Google Cloud Storage (GCS) and uploaded parquet files in the newly created bucket.
2. Google Compute Engine
- Created a VM instance (e2-standard-4 type) with 4 vCPU and 16 GB memory.
- Installed Python3, [Mage](https://github.com/mage-ai/mage-ai) and other dependencies on the VM instance.
- Set up a firewall rule in the VM to access Mage UI.
3. Data pipeline
- The entire data pipeline was created using Python in Mage
- Two data loading scripts were prepared that pulled data from the bucket in GCS. Both these parquet files had a public URL through which they were read and stored in memory.
- A transformation script was prepared that built a fact-dimension data model using the NYC tax data.
- The transformed data was passed into the output script that wrote data into four tables in BigQuery. To sucessfully complete this step, a service account was created scoped with BigQuery data writing access. A key was generated using the service accont, the details of which were added in Mage so that the VM could communicate with BigQuery.
4. Google BigQuery
- A dataset within the project was created. All the four tables were written within this dataset.
- Ad hoc queries were performed to further analyze the newly written data in BigQuery.
- A transformed table was created using SQL query that served as the join between fact and dimension tables. The transformed table is readily available to be consumed for reporting needs. 
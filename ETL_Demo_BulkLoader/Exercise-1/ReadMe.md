Prerequisite for Exercise-1:

Step-1: Restored the AdventureWorks database backup to my local SQL Server instance. Link is provide below.
Step-2: Verified the SOurce table in the AdventureWorks database to ensure that the data is present and correct.
Step-3: Tested few table using select query to ensure that the data is accessible and can be queried without any issues.

AdventureWorks DB link:
https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks
 
 # Setting up SQL for ETL
 **Exercise-1: Setting up SQL for ETL**

 Step-1: Creating a StagingDB database

 Staging DB screenshot will be here

 Step-2: Creating a Staging table in StagingDB database

 I have created below mentioned table in the StagingDB, I thoght it will be good enough to perform the task further.

 stg_Customer
 stg_Person
 stg_SalesOrderHeader
 stg_SalesOrderDetail
 stg_SalesTerritory
 stg_Product
 stg_ProductCategory
 stg_SpecialOffer
 stg_SpecialOfferProduct
 stg_SalesReason
 stg_Store
 stg_EmailAddress


 After that cretad "ETL_Log" table - a table to log the ETL process.




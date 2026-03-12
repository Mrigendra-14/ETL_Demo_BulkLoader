# AdventureWorks-ETL-Pipeline
ETL Pipeline Implementation Using AdventureWorks Database

**Lets First understand the basic of Classic ETL**

ETL stands for Extract, Transform, and Load. It is used to move data from various source systems into a single, consistent destination, typically a data warehouse, for analysis and reporting. 
<img width="756" height="146" alt="image" src="https://github.com/user-attachments/assets/1fd39c32-4e87-4938-bba7-4026908d369f" />

**Classic ETL – The Concept**
There are 3 main phases for Classic ETL, which are described below:
**1.	Extract Phase
2.	Transform Phase
3.	Load Phase**

**1.	Extract Phase:** The goal is to pull raw data from multiple sources without impacting source system performance.

**Types of Extraction:**
**1.	Full Extraction** — Pull ALL data every time (simple but expensive).
**2.	Incremental Extraction** — Pull only NEW or CHANGED data since last run (efficient).
**3.	Delta Extraction**

**Resources from there we can extract: **
  SQL Server, Oracle, MySQL,CSV, Excel, XML, JSON,APIs, etc.

**2.	Transform Phase:** In this phase, the data is cleaned, validated and filtered to fit the target schema. This process ensures that the data is in a usable format for analysis. 

This process involves the following tasks; it can be expanded as per requirements, but I have mentioned these only.

<img width="819" height="252" alt="image" src="https://github.com/user-attachments/assets/d59d1259-0b5d-407b-9a6c-06340324bb2d" />

**3.	Load Phase: ** The transformed data is finally moved from the staging area into the target database or data warehouse.

**Loading Strategies:** The following are mentioned as load strategies.

**Full Load (Truncate & Load):** Delete all target data, reload everything. Simple but slow.
**Incremental Load ** — Insert/Update only changed records. Faster but complex.
**Upsert (MERGE)** — If record exists → Update, if not → Insert.

The primary tool provided by Microsoft for this purpose is SQL Server Integration Services (SSIS). 

**Role of SQL in ETL:**

**Extraction:** We use SQL queries in this phase to retrieve the data from the source relational database.

**Transformation:** Here we use T-SQL like Stored procedure, functions and complex queries. 

**Loading:** Here, we use Bulk Insert or Merge statements to load the processed data into the target table.










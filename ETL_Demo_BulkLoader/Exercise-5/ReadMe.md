# Exercise 5 — Transformation: Write the SQL transformer. Run it. Check the error log. How many rows were rejected? Why?
Also, attaching this sql query in the seprate .sql file in the Exercise-5 folder.

# In this project, two separate logging tables are used to capture different aspects of the ETL pipeline. This separation is intentional and helps distinguish between process-level logging and data-level validation.
**1.ETL_Log (Process-Level Logging) - to track the execution of ETL processes at a table level.**

**2.ETL_ErrorLog (Data-Level Logging) - to capture row-level data quality issues during transformation.**



--Step 1 — Create the Error Log Table

--Here,First we are creating a table that stores rejected records during transformation.

CREATE TABLE ETL_ErrorLog
(
    ErrorID INT IDENTITY(1,1),
    SourceTable VARCHAR(100),
    RecordID VARCHAR(100),
    ErrorReason VARCHAR(500),
    ErrorDate DATETIME DEFAULT GETDATE()
);

Purpose of this table is: 
- To Track rejected rows
- Track why transformation failed

**--Step 2 — Create the Final Transformed Table**

CREATE TABLE trn_MarketingCustomer
(
    CustomerID INT,
    CustomerName NVARCHAR(200),
    EmailAddress NVARCHAR(200),
    TerritoryName NVARCHAR(100),
    SalesOrderID INT,
    OrderDate DATETIME,
    TotalDue MONEY
);

**--Step 3 — Now here we are Creating the SQL Transformer Stored Procedure**
--This is the main transformation procedure.

--In this procedure, we have used 6 table only, i.e stg_Customer, stg_Person, stg_EmailAddress, stg_SalesOrderHeader, stg_SalesTerritory and trn_MarketingCustomer.

CREATE OR ALTER PROCEDURE [dbo].[usp_Transform_MarketingCustomer]
AS
BEGIN

SET NOCOUNT ON;

--Make process idempotent
TRUNCATE TABLE trn_MarketingCustomer;

TRUNCATE TABLE ETL_ErrorLog;

---- Log Rejected Rows based on business rule
---- 1. Store Customers (No Person ID )
---- 2. Missing EmailAddress

INSERT INTO ETL_ErrorLog (SourceTable, RecordID, ErrorReason)
 
SELECT
    'stg_Customer',
    CAST(c.CustomerID AS VARCHAR),
    CASE
        WHEN c.PersonID IS NULL THEN 'Store customer - No PersonID, no email possible'
        WHEN e.EmailAddress IS NULL THEN 'PersonID exists but no EmailAddress record'
    END
FROM stg_Customer c
LEFT JOIN stg_EmailAddress e 
    ON c.PersonID = e.BusinessEntityID
WHERE c.CustomerID IS NOT NULL
  AND (c.PersonID IS NULL OR e.EmailAddress IS NULL);


-- Insert valid rows into transformed table

INSERT INTO trn_MarketingCustomer
(
    CustomerID,
    CustomerName,
    EmailAddress,
    TerritoryName,
    SalesOrderID,
    OrderDate,
    TotalDue
)
SELECT
    c.CustomerID,
    CONCAT(p.FirstName,' ',p.LastName) AS CustomerName,
    e.EmailAddress,
    t.Name AS TerritoryName,
    s.SalesOrderID,
    s.OrderDate,
    s.TotalDue
 
FROM stg_Customer c
 
LEFT JOIN stg_Person p
    ON c.PersonID = p.BusinessEntityID
 
LEFT JOIN stg_EmailAddress e
    ON c.PersonID = e.BusinessEntityID
 
LEFT JOIN stg_SalesTerritory t
    ON c.TerritoryID = t.TerritoryID
 
LEFT JOIN stg_SalesOrderHeader s
    ON c.CustomerID = s.CustomerID
 
WHERE 
    c.CustomerID IS NOT NULL
    AND c.PersonID IS NOT NULL
    AND e.EmailAddress IS NOT NULL;

END;

--Step 4 — Run the Transformer
--Here, we will Execute the transformation procedure.

EXEC usp_Transform_MarketingCustomer;

--Step- 5: Check the error log
--Here we will verify rejected row using below queries:

SELECT * FROM ETL_ErrorLog;

--Result for above query:

--Currently, it is returning 701 rows with all the details like, ErrorID, SourceTable, RecordID, ErrorReason, ErrorDate.




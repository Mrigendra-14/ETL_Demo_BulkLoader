# Exercise 5 — Transformation: Write the SQL transformer. Run it. Check the error log. How many rows were rejected? Why?
Also, attaching this sql query in the seprate .sql file in the Exercise-4 folder.


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

--Step 2 — Create the Final Transformed Table

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

--Step 3 — Now here we are Creating the SQL Transformer Stored Procedure
--This is the main transformation procedure.

--In this procedure, we have used 6 table only, i.e stg_Customer, stg_Person, stg_EmailAddress, stg_SalesOrderHeader, stg_SalesTerritory and trn_MarketingCustomer.

CREATE PROCEDURE usp_Transform_MarketingCustomer
AS
BEGIN

SET NOCOUNT ON;


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

LEFT JOIN stg_SalesOrderHeader s
    ON c.CustomerID = s.CustomerID

LEFT JOIN stg_SalesTerritory t
    ON c.TerritoryID = t.TerritoryID

WHERE
    c.CustomerID IS NOT NULL
    AND e.EmailAddress IS NOT NULL;

END

--Step 4 — Log Rejected Records

-- in this step, we log records that fail transformation rules.
--Rule 1 — Missing CustomerID

INSERT INTO ETL_ErrorLog
(
    SourceTable,
    RecordID,
    ErrorReason
)

SELECT
    'stg_Customer',
    CAST(CustomerID AS VARCHAR),
    'CustomerID is NULL'

FROM stg_Customer
WHERE CustomerID IS NULL;

--When we have run this above Rule-1, then it shows that 0 rows affected.
--This means, There are NO records in stg_Customer where CustomerID is NULL

--Rule 2 — Missing EmailAddress

INSERT INTO ETL_ErrorLog
(
    SourceTable,
    RecordID,
    ErrorReason
)

SELECT
    'stg_EmailAddress',
    CAST(BusinessEntityID AS VARCHAR),
    'EmailAddress is NULL'

FROM stg_EmailAddress
WHERE EmailAddress IS NULL;

-- above query also shows that 0 rows affected.

--Step 5 — Run the Transformer
--Here, we will Execute the transformation procedure.

EXEC usp_Transform_MarketingCustomer;

--Step- 6: Check the error log
--Here we will verify rejected row using below queries:

SELECT * FROM ETL_ErrorLog;

--Result for above query:

--No records were inserted into the error log because all rows in the staging tables satisfied the defined transformation rules.

--Step 7 — Count Rejected Rows, How many rows were rejected?

SELECT COUNT(*) AS RejectedRows FROM ETL_ErrorLog;

--Result for above query:
--This is also showing 0 "RejectedRows".

--Why were there no rejected rows?

--There were no rejected rows because all records in the staging tables met the transformation criteria defined in the SQL transformer. Specifically, there were no records with NULL CustomerID in stg_Customer and no records with NULL EmailAddress in stg_EmailAddress, which were the conditions we set for logging errors. This indicates that the data quality in the staging tables is good and that all necessary fields for transformation are present.



  




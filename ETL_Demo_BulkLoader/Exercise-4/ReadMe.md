**--Exercise 4 — Profiling: Write SQL queries against stg. dataset to discover the problems. How many nulls in customer_id? How many distinct date formats exist? What's the distribution of regions? What are the scenarios where Email reports show nulls?**

Also, Attaching a seprate sql file which contains all the queries which I have written to perform the above task. File name is "Exercise-4.sql" and it is present in this structure. Below is the code snippet for reference.

**--To perform this task I have written the following SQL quieries identify potential issues:

--1.How many nulls in customer_id?**
SELECT COUNT(*) AS NullCustomerID
FROM stg_Customer
WHERE CustomerID IS NULL;
-- In above CustomerID is a primary identifier, so it should not be NULL.


**--2.How many distinct date formats exist? **

**--Since the ModifiedDate was stored as DATETIME, the original date formats were lost. So, I used a VARCHAR column in staging to keep the original formats and check for inconsistencies.

-- Creating a new raw staging table-> ModifiedDate type is storing as VARCHAR- then doing the Profiling**

**--Step 1 — Create a new staging version**

CREATE TABLE stg_Customer_Raw
(
    CustomerID INT,
    PersonID INT,
    TerritoryID INT,
    ModifiedDate VARCHAR(50)
);

**--Step 2 — Insert sample mixed-format data**


INSERT INTO stg_Customer_Raw
(CustomerID, PersonID, TerritoryID, ModifiedDate) VALUES
(1, 100, 1, '2023/01/15'),
(2, 101, 2, '15-01-2023'),
(3, 102, 3, 'Jan 15 2023'),
(4, 103, 4, '2023-01-15');

**--Step 3 — Now Run profiling**


SELECT ModifiedDate,COUNT(*) AS Occurrences FROM stg_Customer_Raw
GROUP BY ModifiedDate
ORDER BY Occurrences DESC;

<img width="262" height="162" alt="image" src="https://github.com/user-attachments/assets/cbbdce97-bf53-4124-997c-4a665c69903d" />


**--Step 4 detect non-standard patterns:**


SELECT COUNT(*) AS NonISODates
FROM stg_Customer_Raw
WHERE ModifiedDate NOT LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]%';

<img width="274" height="97" alt="image" src="https://github.com/user-attachments/assets/1845597f-9e8c-4b5c-827c-edc83f790cbb" />






--Below query is the previous one and this query always returns 1 because DATETIME removes original date format differences.

SELECT 
    COUNT(DISTINCT FORMAT(ModifiedDate, 'yyyy-MM-dd HH:mm:ss')) AS DistinctDateFormats
FROM stg_Customer;
--Only one date pattern exists in the datset.
--SQL Server stores DATETIME values internally without a specific format.



**--3.What's the distribution of regions? (Sales Terrotory)**
SELECT 
    t.Name AS TerritoryName,
    COUNT(c.CustomerID) AS CustomerCount
FROM stg_Customer c
LEFT JOIN stg_SalesTerritory t
    ON c.TerritoryID = t.TerritoryID
GROUP BY t.Name
ORDER BY CustomerCount DESC;

--Explanation
--This query analyzes the distribution of customers across different sales territories.

--Customers are distributed across several territories with varying counts.

--This information can help understand geographic customer distribution and potential regional market strength.


**--4.What are the scenarios where Email reports show nulls?**



--Scenario 1: When Customer is a store not PersonID(PersonID is Null)

SELECT 
    c.CustomerID,
    c.PersonID,
    e.EmailAddress
FROM stg_Customer c
LEFT JOIN stg_EmailAddress e
    ON c.PersonID = e.BusinessEntityID
WHERE c.PersonID IS NULL;

--Result: above query will return "EmailAddress=NULL" and also PersonID=NULL

--Explanation: 

--Some customers represent stores instead of individual persons.
--Since email addresses are linked through PersonID, customers with PersonID = NULL cannot be matched with the EmailAddress table, therefore email to appear NULL in reports


--Customers representing stores have PersonID = NULL.

--resulting in NULL email values in reports.



--Scenario 2: Customer has PersonID but no EmailAddress record exist
SELECT 
    c.CustomerID,
    c.PersonID,
    e.EmailAddress
FROM stg_Customer c
LEFT JOIN stg_EmailAddress e
    ON c.PersonID = e.BusinessEntityID
WHERE c.PersonID IS NOT NULL
AND e.BusinessEntityID IS NULL;

--Result: No rows returned

--Explanation:

--The customer has a valid PersonID, but no corresponding record exists in the EmailAddress table.
--When the report joins these tables, the email column returns NULL.

--Scenario 3: Email record exists but EmailAddress value is NULL
SELECT *
FROM stg_EmailAddress
WHERE EmailAddress IS NULL;

--Result: No rows returned

--Explanation:
--All records in the EmailAddress table contain valid email values.
--There are no cases where the EmailAddress field itself is NULL.

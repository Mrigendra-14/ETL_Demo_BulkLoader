--Step-1: First we will create DimCustomer table

CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,  
    CustomerID INT,                             
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    EmailAddress NVARCHAR(100),
    TerritoryID INT
);

--Step-2: Here, we will create DimChannel table

CREATE TABLE DimChannel (
    ChannelKey INT IDENTITY(1,1) PRIMARY KEY,
    ChannelName NVARCHAR(50)
);

--Step-3: Here, we will create DimBrand table

CREATE TABLE DimBrand (
    BrandKey INT IDENTITY(1,1) PRIMARY KEY,
    BrandName NVARCHAR(50)
);

--Step-4: Now Loading DimCustomer - Creating stored procedure - usp_Load_DimCustomer

CREATE OR ALTER PROCEDURE usp_Load_DimCustomer
AS
BEGIN
    SET NOCOUNT ON;
 
    MERGE DimCustomer AS target
    USING (
        SELECT 
            c.CustomerID,
            p.FirstName,
            p.LastName,
            e.EmailAddress,
            c.TerritoryID
        FROM AdventureWorks2019.Sales.Customer c
        LEFT JOIN AdventureWorks2019.Person.Person p 
            ON c.PersonID = p.BusinessEntityID
        LEFT JOIN AdventureWorks2019.Person.EmailAddress e 
            ON p.BusinessEntityID = e.BusinessEntityID
    ) AS source
    ON target.CustomerID = source.CustomerID
 
    WHEN MATCHED THEN
        UPDATE SET 
            FirstName = source.FirstName,
            LastName = source.LastName,
            EmailAddress = source.EmailAddress,
            TerritoryID = source.TerritoryID
 
    WHEN NOT MATCHED THEN
        INSERT (CustomerID, FirstName, LastName, EmailAddress, TerritoryID)
        VALUES (source.CustomerID, source.FirstName, source.LastName, source.EmailAddress, source.TerritoryID);
END;

--Step-5: Now loading DimChannel - Creating stored procedure - usp_Load_DimChannel


CREATE OR ALTER PROCEDURE usp_Load_DimChannel
AS
BEGIN
    SET NOCOUNT ON;
 
    MERGE DimChannel AS target
    USING (
        SELECT DISTINCT 
            CASE 
                WHEN OnlineOrderFlag = 1 THEN 'Online'
                ELSE 'Offline'
            END AS ChannelName
        FROM AdventureWorks2019.Sales.SalesOrderHeader
    ) AS source
    ON target.ChannelName = source.ChannelName
 
    WHEN NOT MATCHED THEN
        INSERT (ChannelName)
        VALUES (source.ChannelName);
END;

--Step-6: Now Loading DimBrand - Creating stored procedure - usp_Load_DimBrand

CREATE OR ALTER PROCEDURE usp_Load_DimBrand
AS
BEGIN
    SET NOCOUNT ON;
 
    MERGE DimBrand AS target
    USING (
        SELECT DISTINCT pc.Name AS BrandName
        FROM AdventureWorks2019.Production.Product p
        LEFT JOIN AdventureWorks2019.Production.ProductSubcategory ps 
            ON p.ProductSubcategoryID = ps.ProductSubcategoryID
        LEFT JOIN AdventureWorks2019.Production.ProductCategory pc
            ON ps.ProductCategoryID = pc.ProductCategoryID
        WHERE pc.Name IS NOT NULL
    ) AS source
    ON target.BrandName = source.BrandName
 
    WHEN NOT MATCHED THEN
        INSERT (BrandName)
        VALUES (source.BrandName);
END;

--Step- 7 : Create a Master SP to execute the above 3 SPs

CREATE OR ALTER PROCEDURE usp_Load_Dimensions
AS
BEGIN
    EXEC usp_Load_DimCustomer;
    EXEC usp_Load_DimChannel;
    EXEC usp_Load_DimBrand;
END;

--Step- 8: Execute the Master SP

EXEC usp_Load_Dimensions


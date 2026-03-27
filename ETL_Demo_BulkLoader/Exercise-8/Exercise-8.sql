--Step-1: Creating Fact Sales table
CREATE TABLE FactSales (
    FactKey INT IDENTITY(1,1) PRIMARY KEY,

    SalesOrderID INT,
    SalesOrderDetailID INT,

    CustomerKey INT,
    BrandKey INT,
    ChannelKey INT,

    OrderDate DATETIME,
    OrderQty INT,
    UnitPrice MONEY,
    LineTotal MONEY
);


--Step-2: Make the table Idempotent

ALTER TABLE FactSales ADD CONSTRAINT UQ_FactSales UNIQUE (SalesOrderID, SalesOrderDetailID); -- It will Prevent duplicate transactions

--Step-3: Now, we will create stored procedure to load data into FactSales table

CREATE OR ALTER PROCEDURE usp_Load_FactSales
AS
BEGIN
    SET NOCOUNT ON;

    MERGE FactSales AS target
    USING (
        SELECT
            soh.SalesOrderID,
            sod.SalesOrderDetailID,

            dc.CustomerKey,
            db.BrandKey,
            dch.ChannelKey,

            soh.OrderDate,
            sod.OrderQty,
            sod.UnitPrice,
            sod.LineTotal

        FROM AdventureWorks2019.Sales.SalesOrderHeader soh

        JOIN AdventureWorks2019.Sales.SalesOrderDetail sod
            ON soh.SalesOrderID = sod.SalesOrderID

        -- Customer Mapping
        LEFT JOIN DimCustomer dc
            ON dc.CustomerID = soh.CustomerID

        -- Channel Mapping
        LEFT JOIN DimChannel dch
            ON dch.ChannelName =
                CASE
                    WHEN soh.OnlineOrderFlag = 1 THEN 'Online'
                    ELSE 'Offline'
                END

        -- Brand Mapping
        LEFT JOIN AdventureWorks2019.Production.Product p
            ON sod.ProductID = p.ProductID

        LEFT JOIN AdventureWorks2019.Production.ProductSubcategory ps
            ON p.ProductSubcategoryID = ps.ProductSubcategoryID

        LEFT JOIN AdventureWorks2019.Production.ProductCategory pc
            ON ps.ProductCategoryID = pc.ProductCategoryID

        LEFT JOIN DimBrand db
            ON db.BrandName = pc.Name

    ) AS source

    ON target.SalesOrderID = source.SalesOrderID
       AND target.SalesOrderDetailID = source.SalesOrderDetailID

    WHEN NOT MATCHED THEN
        INSERT (
            SalesOrderID,
            SalesOrderDetailID,
            CustomerKey,
            BrandKey,
            ChannelKey,
            OrderDate,
            OrderQty,
            UnitPrice,
            LineTotal
        )
        VALUES (
            source.SalesOrderID,
            source.SalesOrderDetailID,
            source.CustomerKey,
            source.BrandKey,
            source.ChannelKey,
            source.OrderDate,
            source.OrderQty,
            source.UnitPrice,
            source.LineTotal
        );

END;

--Step-4: Step 4 — Create Master SP

CREATE OR ALTER PROCEDURE usp_Load_All
AS
BEGIN
    SET NOCOUNT ON;

    EXEC usp_Load_Dimensions;
    EXEC usp_Load_FactSales;
END;

--Step 5 — Execute the Master SP

EXEC usp_Load_All

--Step-6: To test the Idempotency, we will run the SP twice
EXEC usp_Load_FactSales;
EXEC usp_Load_FactSales;








**Exercise 2 — Perform Data staging**

In Exercise-1, we have created the staging tables in the StagingDB database. Now, in Exercise-2, we will perform data staging by loading data from the source tables in the AdventureWorks database into the staging tables in the StagingDB database.

To perform this task, I have created a Stored procedure which is described below:

Once Stored is completed then we can execute the stored procedure to load data into the staging tables. Below is the command to execute the stored procedure: 

EXEC StagingDB.dbo.usp_LoadStaging;

Also, attaching .sql file which contains the code for the stored procedure.

USE [StagingDB]
GO
/****** Object:  StoredProcedure [dbo].[usp_LoadStaging]    Script Date: 12-03-2026 18:12:46 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER   Procedure [dbo].[usp_LoadStaging]
AS
Begin
	SET NOCOUNT ON;

	DECLARE @StartTime DATETIME = GETDATE();
	DECLARE @RowCount Int;
	DECLARE @TableName NVARCHAR(100);

	BEGIN TRY

	-------------------------
	--First truncate all table
	--------------------------

	    TRUNCATE TABLE stg_Customer;
        TRUNCATE TABLE stg_EmailAddress;
        TRUNCATE TABLE stg_Person;
        TRUNCATE TABLE stg_Product;
        TRUNCATE TABLE stg_ProductCategory;
        TRUNCATE TABLE stg_SalesOrderDetail;
        TRUNCATE TABLE stg_SalesOrderHeader;
        TRUNCATE TABLE stg_SalesReason;
        TRUNCATE TABLE stg_SalesTerritory;
        TRUNCATE TABLE stg_SpecialOffer;
        TRUNCATE TABLE stg_SpecialOfferProduct;
        TRUNCATE TABLE stg_Store;

		-------------------
		--Now load stg_Customer
		----------------------

		SET @TableName = 'stg_Customer';
		SET @StartTime = GETDATE();

		INSERT INTO stg_Customer
		(
			CustomerID, PersonID, StoreID, TerritoryID, rowguid, AccountNumber, ModifiedDate, ETL_LoadDate
		)
		SELECT
            CustomerID, PersonID, StoreID, TerritoryID, rowguid, AccountNumber, ModifiedDate, GETDATE()
			FROM AdventureWorks2019.Sales.Customer;

           SET @RowCount = @@ROWCOUNT;

		INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');
        
		-----------------------------
		--Now Loading stg_EmailAddress
		------------------------------

		SET @TableName = 'stg_EmailAddress';
		SET @StartTime = GETDATE();

		INSERT INTO stg_EmailAddress
		(
			BusinessEntityID, EmailAddressID, EmailAddress, rowguid, ModifiedDate, ETL_LoadDate
        )
		SELECT
            BusinessEntityID, EmailAddressID, EmailAddress, rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Person.EmailAddress;
		SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		-----------------------------
		--Now loading stg_Person
		-----------------------------

		SET @TableName = 'stg_Person';
		SET @StartTime = GETDATE();

		INSERT INTO stg_Person
		(
			BusinessEntityID, PersonType, NameStyle, Title,
            FirstName, MiddleName, LastName, Suffix,
            EmailPromotion, AdditionalContactInfo, Demographics,
            rowguid, ModifiedDate, ETL_LoadDate
		)
		SELECT
            BusinessEntityID, PersonType, NameStyle, Title,
            FirstName, MiddleName, LastName, Suffix,
            EmailPromotion,
            CAST(AdditionalContactInfo AS NVARCHAR(MAX)),
            CAST(Demographics AS NVARCHAR(MAX)),
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Person.Person;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		-----------------------
		--Loading stg_Product
		-----------------------

		SET @TableName = 'stg_Product';
		SET @StartTime = GETDATE();

		INSERT INTO stg_Product
		(
			ProductID, Name, ProductNumber, MakeFlag, FinishedGoodsFlag,
            Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice,
            Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight,
            DaysToManufacture, ProductLine, Class, Style,
            ProductSubcategoryID, ProductModelID,
            SellStartDate, SellEndDate, DiscontinuedDate,
            rowguid, ModifiedDate, ETL_LoadDate
		)

		SELECT
            ProductID, Name, ProductNumber, MakeFlag, FinishedGoodsFlag,
            Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice,
            Size, SizeUnitMeasureCode, WeightUnitMeasureCode, Weight,
            DaysToManufacture, ProductLine, Class, Style,
            ProductSubcategoryID, ProductModelID,
            SellStartDate, SellEndDate, DiscontinuedDate,
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Production.Product;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		-----------------------------
		--Loading stg_ProductCategory
		-----------------------------

		SET @TableName = 'stg_ProductCategory';
		SET @StartTime  = GETDATE();

		INSERT INTO stg_ProductCategory
		(
			ProductCategoryID, Name, rowguid, ModifiedDate, ETL_LoadDate
            
		)
		SELECT
            ProductCategoryID, Name, rowguid, ModifiedDate, GETDATE()
            
        FROM AdventureWorks2019.Production.ProductCategory;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		----------------------------------
		--Loading stg_SalesOrderHeader
		----------------------------------

		SET @TableName  = 'stg_SalesOrderHeader';
        SET @StartTime  = GETDATE();

		INSERT INTO stg_SalesOrderHeader
        (
            SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
            Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber,
            AccountNumber, CustomerID, SalesPersonID, TerritoryID,
            BillToAddressID, ShipToAddressID, ShipMethodID,
            CreditCardID, CreditCardApprovalCode, CurrencyRateID,
            SubTotal, TaxAmt, Freight, TotalDue, Comment,
            rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate,
            Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber,
            AccountNumber, CustomerID, SalesPersonID, TerritoryID,
            BillToAddressID, ShipToAddressID, ShipMethodID,
            CreditCardID, CreditCardApprovalCode, CurrencyRateID,
            SubTotal, TaxAmt, Freight, TotalDue, Comment,
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SalesOrderHeader;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		---------------------------------
		--Loading stg_SalesOrderDetail
		---------------------------------

		SET @TableName  = 'stg_SalesOrderDetail';
        SET @StartTime  = GETDATE();

		INSERT INTO stg_SalesOrderDetail
        (
            SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber,
            OrderQty, ProductID, SpecialOfferID,
            UnitPrice, UnitPriceDiscount, LineTotal,
            rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            SalesOrderID, SalesOrderDetailID, CarrierTrackingNumber,
            OrderQty, ProductID, SpecialOfferID,
            UnitPrice, UnitPriceDiscount, LineTotal,
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SalesOrderDetail;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		----------------------------------------
		--Loading stg_SalesReason
		----------------------------------------

		SET @TableName  = 'stg_SalesReason';
        SET @StartTime  = GETDATE();

		INSERT INTO stg_SalesReason
        (
            SalesReasonID, Name,
            ReasonType, ModifiedDate, ETL_LoadDate
        )
        SELECT
            SalesReasonID, Name,
            ReasonType, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SalesReason;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		--------------------------------
		--Load stg_SalesTerritory
		--------------------------------

		SET @TableName  = 'stg_SalesTerritory';
        SET @StartTime  = GETDATE();

        INSERT INTO stg_SalesTerritory
        (
            TerritoryID, Name, CountryRegionCode, TerritoryGroup,
            SalesYTD, SalesLastYear, CostYTD, CostLastYear,
            rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            TerritoryID, Name, CountryRegionCode,
            [Group],        -- renamed TerritoryGroup in staging
            SalesYTD, SalesLastYear, CostYTD, CostLastYear,
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SalesTerritory;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		----------------------------
		--Loading stg_SpecialOffer
		----------------------------

		SET @TableName  = 'stg_SpecialOffer';
        SET @StartTime  = GETDATE();

        INSERT INTO stg_SpecialOffer
        (
            SpecialOfferID, Description, DiscountPct,
            Type, Category, StartDate, EndDate,
            MinQty, MaxQty, rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            SpecialOfferID, Description, DiscountPct,
            Type, Category, StartDate, EndDate,
            MinQty, MaxQty, rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SpecialOffer;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		--------------------------------------
		-- Loading stg_SpecialOfferProduct
		--------------------------------------

		SET @TableName  = 'stg_SpecialOfferProduct';
        SET @StartTime  = GETDATE();

        INSERT INTO stg_SpecialOfferProduct
        (
            SpecialOfferID, ProductID,
            rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            SpecialOfferID, ProductID,
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.SpecialOfferProduct;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		----------------------------
		--stg_Store
		----------------------------

		SET @TableName  = 'stg_Store';
        SET @StartTime  = GETDATE();

        INSERT INTO stg_Store
        (
            BusinessEntityID, Name, SalesPersonID,
            Demographics, rowguid, ModifiedDate, ETL_LoadDate
        )
        SELECT
            BusinessEntityID, Name, SalesPersonID,
            CAST(Demographics AS NVARCHAR(MAX)),
            rowguid, ModifiedDate, GETDATE()
        FROM AdventureWorks2019.Sales.Store;

        SET @RowCount = @@ROWCOUNT;
        INSERT INTO ETL_Log(TableName, StartTime, EndTime, RowsExtracted, Status)
        VALUES (@TableName, @StartTime, GETDATE(), @RowCount, 'Success');

		END TRY
        BEGIN CATCH

		INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        PRINT 'ERROR in: '  + @TableName;
        PRINT 'Message:  '  + ERROR_MESSAGE();
    END CATCH
END;


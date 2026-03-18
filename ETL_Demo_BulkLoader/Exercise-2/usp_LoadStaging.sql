--usp_LoadStaging
--Load data from AdventureWorks2019 to StagingDB. 
--Each table is handled in a separate TRY-CATCH block to ensure that if one table fails, the others can still load.

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

	-------------------------
	--First truncate all table
	--------------------------

	    TRUNCATE TABLE stg_Customer;
        TRUNCATE TABLE stg_EmailAddress;
        TRUNCATE TABLE stg_Person;
        TRUNCATE TABLE stg_SalesOrderDetail;
        TRUNCATE TABLE stg_SalesOrderHeader;
        TRUNCATE TABLE stg_SalesTerritory;

        --NOTE
        --Below tables are not used in the current ETL Exercise, keeping for future use.
        --TRUNCATE TABLE stg_Product;
        --TRUNCATE TABLE stg_ProductCategory;
        --TRUNCATE TABLE stg_SalesReason;
        --TRUNCATE TABLE stg_SpecialOffer;
        --TRUNCATE TABLE stg_SpecialOfferProduct;
        --TRUNCATE TABLE stg_Store;

		----------------------
		--Now load stg_Customer
		----------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;


        
		-----------------------------
		--Now Loading stg_EmailAddress
		------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		-----------------------------
		--Now loading stg_Person
		-----------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;
		

		---------------------------------
		--Loading stg_SalesOrderDetail
		---------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

        
		----------------------------------
		--Loading stg_SalesOrderHeader
		----------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

        --------------------------------
		--Load stg_SalesTerritory
		--------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

        /*
        -----------------------
		--Loading stg_Product
		-----------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		-----------------------------
		--Loading stg_ProductCategory
		-----------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		----------------------------------------
		--Loading stg_SalesReason
		----------------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		

		----------------------------
		--Loading stg_SpecialOffer
		----------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		--------------------------------------
		-- Loading stg_SpecialOfferProduct
		--------------------------------------

        BEGIN TRY

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

        END TRY

        BEGIN CATCH

        INSERT INTO ETL_Log
        (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
        VALUES
        (@TableName, @StartTime, GETDATE(), 0, 'Failed', ERROR_MESSAGE());

        END CATCH;

		----------------------------
		--stg_Store
		----------------------------

        BEGIN TRY

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

        END CATCH;
        */
    
END;

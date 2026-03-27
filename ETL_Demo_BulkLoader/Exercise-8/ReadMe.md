--Exercise 8 — Fact Load: Load the fact table. Make it idempotent — running it twice should not create duplicates.

--First we will create FactSales table


-- Then we will load data into it using MERGE statement to ensure idempotency.


--Then we will create a master SP (usp_Load_All) to execute usp_Load_Dimensions and usp_Load_FactSales in sequence.

-- Then, we will execute usp_Load_All to run the complete ETL pipeline.

--Finally,To test the Idempotency, we will run the SP twice.

EXEC usp_Load_FactSales;
EXEC usp_Load_FactSales;


    


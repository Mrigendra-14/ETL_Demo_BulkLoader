--Exercise 8 — Fact Load: Load the fact table. Make it idempotent — running it twice should not create duplicates.

--First we will create FactSales table


-- Then we will load data into it using MERGE statement to ensure idempotency.


--Then we will create a master SP (usp_Load_All) to execute usp_Load_Dimensions and usp_Load_FactSales in sequence.

-- Finally, we will execute usp_Load_All to perform the entire load process.


    


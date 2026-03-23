# Exercise 6 — Transformation: Write the C# transformer. Run it. Check the error log. How many rows were rejected? Why?

In the Exercise-5, we had done this using SQL, now here in Exercise-6, we will do the same transformation using C#.

C# program will read staging tables-> apply transformation rules-> insert into transformed table -> Log errors.

-- ETL Flow:
Staging Tables-> C# Transformer-> trn_MarketingCustomer (Transformed Table)-> ETL_ErrorLog (Error Log Table)


# C# Transformer code is writen inside the "ETL_Transformer" project.






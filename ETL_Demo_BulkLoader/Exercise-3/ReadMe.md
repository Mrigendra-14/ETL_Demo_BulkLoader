**Exercise-3 : Loading data into StagingDB using Bulk Insert**

In Exercise-3, we will load data from a CSV file into staging using C# SqlBulkCopy.

CSV file--> C# SQLBulkCopy --> StagingDB

C# SQLBulkCopy is the fast way to load data into SQL Server.

 **To create CSV file, I have used AdventureWorks datbase as a source and created a CSV file.**

**Steps to create CSV file:**

Right click on AdventureWorks database in SQL Server Management Studio (SSMS) and select "Tasks" > "Export Data". This will open the SQL Server Import and Export Wizard.
In the wizard, select the source database (SQL Server native or OLEDB) and the destination as "Flat File Destination". Click "Next".
On the next screen, specify the file name and location for the CSV file. 
On the next screen, select the table that you want to export data from. You can also write a query to specify the data you want to export. Click "Next".
On the next screen, review the settings and click "Finish" to start the export process. Once the export is complete, you will have a CSV file with the data from the selected table.

We need to repeat the above steps for all the tables that we want to load into staging.
**As of now I have done it for below tables:**

Customer
EmailAddress
Person
SalesOrderDetail
SalesOrderHeader
SalesTerritory

 **Once we will done with these steps then we can get above csv file in our specified folder with the loaded data.**
We can use this csv file to load data into staging using C# SQLBulkCopy in the next step.

I have written a C# code to load data from CSV file into staging using SQLBulkCopy. Code file name is "Program.cs". which is present in this structure. Below is the code snippet for reference.

Note: We need to truncate the table before executing this code to avoid duplicate data.
Once we execute the code our data will be loaded into staging and we can verify it by running select query on the staging table.


--Code is written inside the Program.cs file of the "ETL_Demo_BulkLoader" project."

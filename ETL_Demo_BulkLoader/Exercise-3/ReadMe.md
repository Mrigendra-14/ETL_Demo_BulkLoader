Exercise-3 : Loading data into StagingDB using Bulk Insert

In Exercise-3, we will load data from a CSV file into staging using C# SqlBulkCopy.

CSV file--> C# SQLBulkCopy --> StagingDB

C# SQLBulkCopy is the fast way to load data into SQL Server.

# To create CSV file, I have used AdventureWorks datbase as a source and created a CSV file.

# Steps to create CSV file:

Right click on AdventureWorks database in SQL Server Management Studio (SSMS) and select "Tasks" > "Export Data". This will open the SQL Server Import and Export Wizard.
In the wizard, select the source database (SQL Server native or OLEDB) and the destination as "Flat File Destination". Click "Next".
On the next screen, specify the file name and location for the CSV file. 
On the next screen, select the table that you want to export data from. You can also write a query to specify the data you want to export. Click "Next".
On the next screen, review the settings and click "Finish" to start the export process. Once the export is complete, you will have a CSV file with the data from the selected table.

We need to repeat the above steps for all the tables that we want to load into staging.
# As of now I have done it for below tables:

Customer
EmailAddress
Person
SalesOrderDetail
SalesOrderHeader
SalesTerritory

# Once we will done with these steps then we can get above csv file in our specified folder with the loaded data.
We can use this csv file to load data into staging using C# SQLBulkCopy in the next step.

I have written a C# code to load data from CSV file into staging using SQLBulkCopy. Code file name is "Program.cs". which is present in this structure. Below is the code snippet for reference.

Note: We need to truncate the table before executing this code to avoid duplicate data.
Once we execute the code our data will be loaded into staging and we can verify it by running select query on the staging table.


using System.Data;
using System.Data.SqlClient;
using System.IO;



namespace ETL_Demo_BulkLoader
{
    internal class Program
    {
       static string connectionString = "Server=ASEN-PL--MDWIVE\\MSSQLSERVERNEW;Database=StagingDB;Trusted_Connection=True;";

        
        static DataTable ReadCsv(string filePath)
        {
            DataTable dt = new DataTable();
            int rowGuidIndex = -1;

            using (var reader = new StreamReader(filePath))
            {
                bool isFirstRow = true;

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    if (isFirstRow)
                    {
                        for (int i = 0; i < values.Length; i++)
                        {
                            string col = values[i].Trim();

                            if (col.Equals("rowguid", StringComparison.OrdinalIgnoreCase))
                            {
                                dt.Columns.Add(col, typeof(Guid));
                                rowGuidIndex = i;
                            }
                            else
                            {
                                dt.Columns.Add(col, typeof(string));
                            }
                        }

                        isFirstRow = false;
                    }
                    else
                    {
                        object[] row = new object[values.Length];

                        for (int i = 0; i < values.Length; i++)
                        {
                            string value = values[i].Trim().Trim('"').Trim('{', '}').Trim('(', ')');

                            if (string.IsNullOrWhiteSpace(value))
                            {
                                row[i] = DBNull.Value;
                            }
                            else if (i == rowGuidIndex)
                            {
                                if (Guid.TryParse(value, out Guid guidVal))
                                    row[i] = guidVal;
                                else
                                    row[i] = DBNull.Value;
                            }
                            else
                            {
                                row[i] = value;
                            }
                        }

                        dt.Rows.Add(row);
                    }
                }
            }

            return dt;
        }

        static void BulkInsert(string filePath, string tableName)
        {
            DataTable dt = ReadCsv(filePath);

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = tableName;
                    foreach (DataColumn column in dt.Columns)
                    {
                        if (tableName == "stg_SalesTerritory" && column.ColumnName == "Group")
                            bulkCopy.ColumnMappings.Add("Group", "TerritoryGroup");
                        else
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                    }
                    try
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error loading {filePath} into {tableName}: {ex.Message}");
                    }
                }
            }

            Console.WriteLine($"Loaded {filePath} into {tableName}");
           
        }

        static void Main(string[] args)
        {
            string basePath = @"C:\ETLDemo\InputFiles\";

            BulkInsert(basePath + "Customer.csv", "stg_Customer");
            BulkInsert(basePath + "Person.csv", "stg_Person");
            BulkInsert(basePath + "EmailAddress.csv", "stg_EmailAddress");
            BulkInsert(basePath + "SalesOrderHeader.csv", "stg_SalesOrderHeader");
            BulkInsert(basePath + "SalesOrderDetail.csv", "stg_SalesOrderDetail");
            BulkInsert(basePath + "SalesTerritory.csv", "stg_SalesTerritory");
            Console.WriteLine("All files loaded successfully.");
            Console.ReadLine();
        }
    }
}

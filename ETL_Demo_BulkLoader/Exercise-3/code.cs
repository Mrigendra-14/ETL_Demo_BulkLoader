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

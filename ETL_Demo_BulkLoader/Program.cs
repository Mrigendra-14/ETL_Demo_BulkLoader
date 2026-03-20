using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Globalization;
using System.IO;
using CsvHelper;



namespace ETL_Demo_BulkLoader
{
    internal class Program
    {
        static string connectionString = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;
        static DataTable ReadCsv(string filePath)
        {
            DataTable dt = new DataTable();

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                // Read header first
                csv.Read();
                csv.ReadHeader();
                string[] headers = csv.HeaderRecord;

                
                foreach (var header in headers)
                {
                    if (header.Equals("rowguid", StringComparison.OrdinalIgnoreCase))
                        dt.Columns.Add(header, typeof(Guid));
                    else
                        dt.Columns.Add(header, typeof(string));
                }

                
                while (csv.Read())
                {
                    DataRow row = dt.NewRow();
                    foreach (var header in headers)
                    {
                        string val = csv.GetField(header);
                        string cleaned = val?.Trim().Trim('"').Trim('{', '}').Trim('(', ')');

                        
                        if (string.IsNullOrWhiteSpace(cleaned))
                        {
                            row[header] = DBNull.Value;
                        }
                        
                        else if (header.Equals("rowguid", StringComparison.OrdinalIgnoreCase))
                        {
                            row[header] = Guid.TryParse(cleaned, out Guid g)
                                ? g
                                : DBNull.Value;
                        }
                        else
                        {
                            row[header] = cleaned;
                        }
                    }
                    dt.Rows.Add(row);
                }
            }

            return dt;
        }

        static void BulkInsert(string filePath, string tableName)
        {
            DataTable dt = ReadCsv(filePath);
            DateTime startTime = DateTime.Now;
            bool isSuccess = true;


            using (SqlConnection conn = new SqlConnection(connectionString))
            {

                conn.Open();
                try
                {

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



                        bulkCopy.BatchSize = 1000;
                        bulkCopy.BulkCopyTimeout = 120;
                        bulkCopy.WriteToServer(dt);
                        DateTime endTime = DateTime.Now;
                        LogToDatabase(conn, tableName, dt.Rows.Count, startTime, endTime);
                    }
                }



                catch (Exception ex)
                {
                    isSuccess = false;
                    DateTime endTime = DateTime.Now;
                    LogErrorToDatabase(conn, tableName, ex.Message, startTime, endTime);
                    Console.WriteLine($"Error loading {filePath} into {tableName}: {ex.Message}");
                }
            }


            if (isSuccess)
            {
                Console.WriteLine($"Loaded {dt.Rows.Count} rows from {filePath} into {tableName}");
            }


        }

        static void LogToDatabase(SqlConnection conn, string tableName, int rowCount, DateTime start, DateTime end)
        {

            string query = @"
                INSERT INTO ETL_Log (TableName, StartTime, EndTime, RowsExtracted, Status)
                VALUES (@TableName, @StartTime, @EndTime, @RowCount, 'Success')";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@TableName", tableName);
                cmd.Parameters.AddWithValue("@RowCount", rowCount);
                cmd.Parameters.AddWithValue("@StartTime", start);
                cmd.Parameters.AddWithValue("@EndTime", end);
                cmd.ExecuteNonQuery();
            }

        }
        static void LogErrorToDatabase(SqlConnection conn, string tableName, string error, DateTime start, DateTime end)
        {

            string query = @"
                INSERT INTO ETL_Log (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
                VALUES (@TableName, @StartTime, @EndTime, 0, 'Failed', @Error)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@TableName", tableName);
                cmd.Parameters.AddWithValue("@Error", error);
                cmd.Parameters.AddWithValue("@StartTime", start);
                cmd.Parameters.AddWithValue("@EndTime", end);
                cmd.ExecuteNonQuery();
            }

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

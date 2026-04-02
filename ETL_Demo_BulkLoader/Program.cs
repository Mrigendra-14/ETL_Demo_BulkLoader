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

        static async Task <(bool success, int rowCount)> BulkInsertAsync(string filePath, string tableName)
        {
            Log($"[START] Loading {tableName} from {Path.GetFileName(filePath)}");

            if (!File.Exists(filePath))
            {
                Log($"[SKIP] File not found: {filePath}");
                return (false, 0);
            }
            if (new FileInfo(filePath).Length == 0)
            {
                Log($"[SKIP] File is empty: {filePath}");
                return (false, 0);
            }
            try
            {
                using (var stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None)) { }
            }
            catch (Exception ex)
            {
                Log($"[SKIP] File cannot be accessed: {ex.Message}");
                return (false, 0);
            }
            if (Path.GetExtension(filePath).ToLower() != ".csv")
            {
                Log($"[SKIP] Invalid file type: {filePath}");
                return (false, 0);
            }


            DateTime startTime = DateTime.Now;
            int rowCount = 0;


            await using (SqlConnection conn = new SqlConnection(connectionString))
            {

               
                try
                {
                    await conn.OpenAsync();
                    DataTable dt = ReadCsv(filePath);
                    rowCount = dt.Rows.Count;

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
                        await bulkCopy.WriteToServerAsync(dt);
                        DateTime endTime = DateTime.Now;
                        await LogToDatabaseAsync(conn, tableName, rowCount, startTime, endTime);
                        Log($"[SUCCESS] {tableName} — {rowCount} rows loaded in {(endTime - startTime).TotalSeconds:F2}s");
                        return (true, rowCount);
                        

                    }
                }



                catch (Exception ex)
                {
                    
                    DateTime endTime = DateTime.Now;
                    await LogErrorToDatabaseAsync(conn, tableName, ex.Message, startTime, endTime);
                    Log($"[FAILED] {tableName} — {ex.Message}");
                    return (false, 0);
                }
            }


        }

        static async Task LogToDatabaseAsync(SqlConnection conn, string tableName, int rowCount, DateTime start, DateTime end)
        {

            string query = @"
                INSERT INTO ETL_Log (TableName, StartTime, EndTime, RowsExtracted, Status)
                VALUES (@TableName, @StartTime, @EndTime, @RowCount, 'Success')";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.Add("@TableName", SqlDbType.NVarChar, 100).Value = tableName;
                cmd.Parameters.Add("@RowCount", SqlDbType.Int).Value = rowCount;
                cmd.Parameters.Add("@StartTime", SqlDbType.DateTime).Value = start;
                cmd.Parameters.Add("@EndTime", SqlDbType.DateTime).Value = end;
                await cmd.ExecuteNonQueryAsync();
            }

        }
        static async Task LogErrorToDatabaseAsync(SqlConnection conn, string tableName, string error, DateTime start, DateTime end)
        {

            string query = @"
                INSERT INTO ETL_Log (TableName, StartTime, EndTime, RowsExtracted, Status, ErrorMessage)
                VALUES (@TableName, @StartTime, @EndTime, 0, 'Failed', @Error)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.Add("@TableName", SqlDbType.NVarChar, 100).Value = tableName;
                cmd.Parameters.Add("@Error", SqlDbType.NVarChar, 500).Value = error;
                cmd.Parameters.Add("@StartTime", SqlDbType.DateTime).Value = start;
                cmd.Parameters.Add("@EndTime", SqlDbType.DateTime).Value = end;
                await cmd.ExecuteNonQueryAsync();
            }

        }

        private static string GetBasePath()
        {
            string path = ConfigurationManager.AppSettings["InputFilesBasePath"]
                 ?? throw new InvalidOperationException("Missing required config key: 'InputFilesBasePath' in AppSettings.");

            if (!Directory.Exists(path))
                throw new DirectoryNotFoundException(
                    $"Input files directory not found: '{path}'");

            return path;
        }

        static async Task Main(string[] args)

        {
            Log("=== ETL BULK LOADER STARTED ===");
            string basePath = GetBasePath();

            int successCount = 0;
            int failedCount = 0;
            int totalRows = 0;

            
            (bool success, int rows) = await BulkInsertAsync(Path.Combine(basePath, "Customer.csv"), "stg_Customer");
            if (success) { successCount++; totalRows += rows; } else failedCount++;

            (success, rows) = await BulkInsertAsync(Path.Combine(basePath, "Person.csv"), "stg_Person");
            if (success) { successCount++; totalRows += rows; } else failedCount++;

            (success, rows) = await BulkInsertAsync(Path.Combine(basePath, "EmailAddress.csv"), "stg_EmailAddress");
            if (success) { successCount++; totalRows += rows; } else failedCount++;

            (success, rows) = await BulkInsertAsync(Path.Combine(basePath, "SalesOrderHeader.csv"), "stg_SalesOrderHeader");
            if (success) { successCount++; totalRows += rows; } else failedCount++;

            (success, rows) = await BulkInsertAsync(Path.Combine(basePath, "SalesOrderDetail.csv"), "stg_SalesOrderDetail");
            if (success) { successCount++; totalRows += rows; } else failedCount++;

            (success, rows) = await BulkInsertAsync(Path.Combine(basePath, "SalesTerritory.csv"), "stg_SalesTerritory");
            if (success) { successCount++; totalRows += rows; } else failedCount++;


            Log($"[SUMMARY] Tables={successCount + failedCount} | Success={successCount} | Failed={failedCount} | TotalRows={totalRows}");
            Log("=== ETL BULK LOADER COMPLETED ===");
          
            Console.ReadLine();
        }
        static void Log(string message)
        {
            Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}");
        }
    }
}

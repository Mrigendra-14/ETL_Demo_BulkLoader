using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;


namespace ETL_Transformer
{
    internal class Program
    {
        static string connectionString = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;


        //This method retrieves data from the staging tables, joining them together to create a comprehensive dataset for transformation.
        static async Task<DataTable> GetStagingDataAsync(SqlConnection conn)
        {
            string query = @"
            SELECT
                c.CustomerID,
                p.FirstName,
                p.LastName,
                e.EmailAddress,
                t.Name AS TerritoryName,
                s.SalesOrderID,
                s.OrderDate,
                s.TotalDue,
                c.PersonID
            FROM stg_Customer c
            LEFT JOIN stg_Person p ON c.PersonID = p.BusinessEntityID
            LEFT JOIN stg_EmailAddress e ON c.PersonID = e.BusinessEntityID
            LEFT JOIN stg_SalesTerritory t ON c.TerritoryID = t.TerritoryID
            LEFT JOIN stg_SalesOrderHeader s ON c.CustomerID = s.CustomerID";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.CommandTimeout = 300;

                using (SqlDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    return dt;
                }
            }
        }
        //Now create Transformation method
        //This method will validate the data, Insert valid rows and log rejected rows.

        static async Task TransformDataAsync(SqlConnection conn)
        {

                DataTable stagingData = await GetStagingDataAsync(conn);

                Log($"[INFO] Staging data fetched — {stagingData.Rows.Count} rows");

                int validCount = 0;
                int rejectedCount = 0;

                foreach (DataRow row in stagingData.Rows)
                {
                    int? customerId = null;
                    try
                    {

                        customerId = row["CustomerID"] == DBNull.Value
                            ? null
                            : Convert.ToInt32(row["CustomerID"]);

                        int? personId = row["PersonID"] == DBNull.Value
                            ? null
                            : Convert.ToInt32(row["PersonID"]);

                        string email = row["EmailAddress"] == DBNull.Value ? null : row["EmailAddress"].ToString().Trim();

                        // Rejected case
                        if (customerId == null)
                        {
                            await LogErrorAsync(conn, customerId, "CustomerID is NULL");
                            Log($"[REJECTED] CustomerID=NULL — CustomerID is NULL");
                            rejectedCount++;
                            continue;
                        }
                        else if (personId == null)
                        {
                            await LogErrorAsync(conn, customerId, "Store customer - No PersonID, no email possible");
                            Log($"[REJECTED] CustomerID={customerId} — No PersonID");
                            rejectedCount++;
                            continue;
                        }
                        else if (string.IsNullOrWhiteSpace(email))
                        {
                            await LogErrorAsync(conn, customerId, "PersonID exists but no EmailAddress record");
                            Log($"[REJECTED] CustomerID={customerId} — No EmailAddress");
                            rejectedCount++;
                            continue;
                        }
                        else
                        {
                            // Valid data will insert
                            await InsertValidRecordAsync(conn, row);
                            Log($"[VALID] CustomerID={customerId} inserted successfully");
                            validCount++;
                        }

                    }
                    catch (Exception ex)
                    {
                        await LogErrorAsync(conn, customerId, $"Unexpected error: {ex.Message}");
                        Log($"[ERROR] CustomerID={customerId} — {ex.Message}");
                        rejectedCount++;
                    }
                }
                
                Log($"[SUMMARY] Total={stagingData.Rows.Count} | Valid={validCount} | Rejected={rejectedCount}");
            
        }
        //Now Insert Valid record
        static async Task InsertValidRecordAsync(SqlConnection conn, DataRow row)
        {
            string query = @"
                INSERT INTO trn_MarketingCustomer
                (CustomerID, CustomerName, EmailAddress, TerritoryName, SalesOrderID, OrderDate, TotalDue)
                VALUES
                (@CustomerID, @CustomerName, @EmailAddress, @TerritoryName, @SalesOrderID, @OrderDate, @TotalDue)";

            string firstName = row["FirstName"] == DBNull.Value ? null : row["FirstName"].ToString().Trim();
            string lastName = row["LastName"] == DBNull.Value ? null : row["LastName"].ToString().Trim();
            string email = row["EmailAddress"] == DBNull.Value ? null : row["EmailAddress"].ToString().Trim();
            string territory = row["TerritoryName"] == DBNull.Value ? null : row["TerritoryName"].ToString().Trim();

            string customerName = (firstName == null && lastName == null) ? null : $"{firstName} {lastName}".Trim();



            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.CommandTimeout = 300;
                cmd.Parameters.Add("@CustomerID", SqlDbType.Int).Value = row["CustomerID"];
                cmd.Parameters.Add("@CustomerName", SqlDbType.NVarChar, 200).Value = (object)customerName ?? DBNull.Value;
                cmd.Parameters.Add("@EmailAddress", SqlDbType.NVarChar, 200).Value = (object)email ?? DBNull.Value;
                cmd.Parameters.Add("@TerritoryName", SqlDbType.NVarChar, 100).Value = (object)territory ?? DBNull.Value;
                cmd.Parameters.Add("@SalesOrderID", SqlDbType.Int).Value = row["SalesOrderID"] == DBNull.Value ? DBNull.Value : row["SalesOrderID"];
                cmd.Parameters.Add("@OrderDate", SqlDbType.DateTime).Value = row["OrderDate"] == DBNull.Value ? DBNull.Value : Convert.ToDateTime(row["OrderDate"]);
                cmd.Parameters.Add("@TotalDue", SqlDbType.Money).Value = row["TotalDue"] == DBNull.Value ? DBNull.Value : Convert.ToDecimal(row["TotalDue"]);

                await cmd.ExecuteNonQueryAsync();
            }
        }

        //Now Log Error

        static async Task LogErrorAsync(SqlConnection conn, int? customerId, string reason)
        {
            string query = @"
                INSERT INTO ETL_ErrorLog (SourceTable, RecordID, ErrorReason)
                VALUES ('stg_Customer', @RecordID, @ErrorReason)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.Add("@RecordID", SqlDbType.VarChar, 100).Value = (object)customerId?.ToString() ?? DBNull.Value;
                cmd.Parameters.Add("@ErrorReason", SqlDbType.VarChar, 500).Value = (object)reason ?? DBNull.Value;

                await cmd.ExecuteNonQueryAsync();
            }
        }
        //make Idempotent before transform
        static async Task PrepareTablesAsync(SqlConnection conn)
        {
            using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE trn_MarketingCustomer; TRUNCATE TABLE ETL_ErrorLog;", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }
        static async Task Main(string[] args)
        {
            Log("=== ETL TRANSFORMER STARTED ===");

            await using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();
                await PrepareTablesAsync(conn);
                Log("[INFO] Tables truncated successfully");

                await TransformDataAsync(conn);
            }

            Log("=== ETL TRANSFORMER COMPLETED ===");

            Console.WriteLine("Transformation completed.");
            Console.ReadLine();
        }
        static void Log(string message)
        {
            Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}");
        }
    }
}


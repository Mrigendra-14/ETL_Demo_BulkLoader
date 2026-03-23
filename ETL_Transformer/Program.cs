using System.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;


namespace ETL_Transformer
{
    internal class Program
    {
        static string connectionString = ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString;


        //This method retrieves data from the staging tables, joining them together to create a comprehensive dataset for transformation.
        static DataTable GetStagingData(SqlConnection conn)
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
            using (SqlDataAdapter da = new SqlDataAdapter(cmd))
            {
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
        }
        //Now create Transformation method
        //This method will validate the data, Insert valid rows and log rejected rows.

        static void TransformData()
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                DataTable stagingData = GetStagingData(conn);

                foreach (DataRow row in stagingData.Rows)
                {
                    int? customerId = row["CustomerID"] == DBNull.Value
                        ? null
                        : Convert.ToInt32(row["CustomerID"]);

                    int? personId = row["PersonID"] == DBNull.Value
                        ? null
                        : Convert.ToInt32(row["PersonID"]);
                    string email = row["EmailAddress"]?.ToString();

                    // Rejected case
                    if (customerId == null)
                    {
                        LogError(conn, customerId, "CustomerID is NULL");
                        continue;
                    }
                    else if (personId == null)
                    {
                        LogError(conn, customerId, "Store customer - No PersonID, no email possible");
                        continue;
                    }
                    else if (string.IsNullOrWhiteSpace(email))
                    {
                        LogError(conn, customerId, "PersonID exists but no EmailAddress record");
                        continue;
                    }
                    else
                    {
                        // Valid data will insert
                        InsertValidRecord(conn, row);
                    }
                }
            }
        }
        //Now Insert Valid record
        static void InsertValidRecord(SqlConnection conn, DataRow row)
        {
            string query = @"
            INSERT INTO trn_MarketingCustomer
            (CustomerID, CustomerName, EmailAddress, TerritoryName, SalesOrderID, OrderDate, TotalDue)
            VALUES
            (@CustomerID, @CustomerName, @EmailAddress, @TerritoryName, @SalesOrderID, @OrderDate, @TotalDue)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@CustomerID", row["CustomerID"]);
                cmd.Parameters.AddWithValue("@CustomerName",
                    $"{row["FirstName"]} {row["LastName"]}");
                cmd.Parameters.AddWithValue("@EmailAddress", row["EmailAddress"]);
                cmd.Parameters.AddWithValue("@TerritoryName", row["TerritoryName"]);
                cmd.Parameters.AddWithValue("@SalesOrderID", row["SalesOrderID"] ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@OrderDate", row["OrderDate"] ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@TotalDue", row["TotalDue"] ?? DBNull.Value);

                cmd.ExecuteNonQuery();
            }
        }

        //Now Log Error

        static void LogError(SqlConnection conn, int? customerId, string reason)
        {
            string query = @"
            INSERT INTO ETL_ErrorLog (SourceTable, RecordID, ErrorReason)
            VALUES ('stg_Customer', @RecordID, @ErrorReason)";

            using (SqlCommand cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@RecordID", customerId?.ToString() ?? "NULL");
                cmd.Parameters.AddWithValue("@ErrorReason", reason);

                cmd.ExecuteNonQuery();
            }
        }
        //make Idempotent before transform
        static void PrepareTables(SqlConnection conn)
        {
            using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE trn_MarketingCustomer; TRUNCATE TABLE ETL_ErrorLog;", conn))
            {
                cmd.ExecuteNonQuery();
            }
        }
        static void Main(string[] args)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                PrepareTables(conn);
            }

            TransformData();

            Console.WriteLine("Transformation completed.");
            Console.ReadLine();
        }
    }
}

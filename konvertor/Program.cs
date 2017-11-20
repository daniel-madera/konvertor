using System;
using System.Linq;
using System.Configuration;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.IO;
using System.Text;

namespace konvertor
{
    class Program
    {
        static int Main(string[] args)
        {
            using (OdbcConnection dbf = new OdbcConnection())
            {
                using (SqlConnection sql = new SqlConnection())
                {
                    using (StreamWriter log = new StreamWriter(Console.OpenStandardOutput(), Console.OutputEncoding))
                    {
                        try
                        {
                            log.AutoFlush = true;
                            Console.SetOut(log);

                            checkDbfSettings();
                            checkSqlSettings();

                            dbf.ConnectionString = buildDbfConnectionString();
                            dbf.Open();

                            sql.ConnectionString = buildSqlConnectionString();
                            sql.Open();

                            DataConvertor convertor = new DataConvertor(dbf, sql, log, Boolean.Parse(ConfigurationManager.AppSettings["IsTest"]));
                            log.WriteLine("Probíhá konverze dat, prosím čekejte...");
                            convertor.Execute();
                            return 0;
                        }

                        catch (OdbcException exception)
                        {
                            string message = "Nepodařilo se navázat spojení s DBF databází! " + exception.Message;
                            log.WriteLine(message);
                            return 1;
                        }
                        catch (SqlException exception)
                        {
                            string message = "Nepodařilo se navázat spojení s SQL databází! " + exception.Message;
                            log.WriteLine(message);
                            return 1;
                        }
                        catch (Exception exception)
                        {
                            string message = "Chyba programu! " + exception.Message;
                            log.WriteLine(message);
                            return 1;
                        }
                    }
                }
            }
        }

        private static void checkDbfSettings()
        {
            string dbfPath = ConfigurationManager.AppSettings["DbfPath"];
            string[] dbfFilesInDirectory = Directory.GetFiles(dbfPath, "*.DBF");
            foreach (string requiredFile in DataConvertor.requiredDbfFiles)
            {
                if (!dbfFilesInDirectory.Contains(dbfPath + "\\" + requiredFile))
                {
                    throw new Exception(String.Format("Nebyly nalezeny všechny DBF soubory. Chybí {0} soubor!", requiredFile));
                }
            }
        }

        private static void checkSqlSettings()
        {
            SqlConnection sql = null;
            try
            {
                sql = new SqlConnection();
                sql.ConnectionString = buildSqlConnectionString();
                sql.Open();
            }   
            catch (SqlException e)
            {
                throw new Exception("Zkontrolujte údaje o připojení k SQL databázi! " + e.Message);
            }
            finally
            {
                try { sql.Close(); } catch (Exception) {}
            }
        }

        private static string buildDbfConnectionString()
        {
            string path = ConfigurationManager.AppSettings["DbfPath"];
            string dbfConnectionString = "Driver={Microsoft dBase Driver (*.dbf)};SourceType=DBF;"
                + "SourceDB=" + path + ";Exclusive=No;NULL=NO;DELETED=NO;BACKGROUNDFETCH=NO;";
            return dbfConnectionString;
        }

        private static string buildSqlConnectionString()
        {
            string host = ConfigurationManager.AppSettings["SqlHost"];
            string db = ConfigurationManager.AppSettings["SqlDbName"];
            string user = ConfigurationManager.AppSettings["SqlUser"];
            string password = ConfigurationManager.AppSettings["SqlPassword"];
            string sqlConnectionString = String.Format("Data Source={0};Initial Catalog={1};Connection Timeout=8;"
                + "User ID={2};Password={3}", host, db, user, password);
            return sqlConnectionString;
        }
        
    }
}

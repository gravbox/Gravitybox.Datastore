using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Install
{
    public class CustomMethods
    {
        private static int[] LANGUAGE_CODES = new int[] { 0, 1028, 1030, 1031, 1033, 1036, 1040,
                                                             1041, 1043, 1045, 1049, 1053, 1054,
                                                             1055, 2052, 2057, 2070, 3082 };

        private static string[] STOP_WORDS_TO_REMOVE = new string[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "0" };

        public static void BuildDatastoreStopList(SqlConnection connection, SqlTransaction transaction, string connectionString)
        {
            SqlConnection newConnection = null;

            try
            {
                newConnection = new SqlConnection(connectionString);
                newConnection.Open();

                var ifExistsCommand = newConnection.CreateCommand();
                ifExistsCommand.CommandText = "SELECT COUNT(*) FROM sys.fulltext_stoplists WHERE name = 'DatastoreStopList';";
                ifExistsCommand.CommandType = CommandType.Text;
                var result = (int) ifExistsCommand.ExecuteScalar();

                if (result <= 0)
                {
                    var stopListCommand = newConnection.CreateCommand();
                    stopListCommand.CommandText = "CREATE FULLTEXT STOPLIST DatastoreStopList FROM SYSTEM STOPLIST;";
                    stopListCommand.CommandType = CommandType.Text;
                    stopListCommand.ExecuteNonQuery();
                }

                // If this comes up in a security scan
                // I am looping over two staticly defined arrays during an upgrade script that only fires one-time
                // if this is going to be a problem, we will just unroll the loop
                foreach (var langCode in LANGUAGE_CODES)
                {
                    foreach (var stopword in STOP_WORDS_TO_REMOVE)
                    {
                        var ifStopwordExistsCommand = newConnection.CreateCommand();
                        ifStopwordExistsCommand.CommandText = "SELECT count(*) FROM sys.fulltext_stopwords sw JOIN sys.fulltext_stoplists sl on sw.stoplist_id = sl.stoplist_id "
                                                              + $"WHERE sl.name = 'DatastoreStopList' AND sw.language_id = {langCode} AND sw.stopword = '{stopword}'";

                        var stopwordExistsResult = (int)ifStopwordExistsCommand.ExecuteScalar();

                        if (stopwordExistsResult > 0)
                        {
                            var alterCommand = newConnection.CreateCommand();
                            alterCommand.CommandText = $"ALTER FULLTEXT STOPLIST DatastoreStopList DROP '{stopword}' LANGUAGE {langCode};";
                            alterCommand.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
                if (newConnection != null)
                {
                    try
                    {
                        newConnection.Close();
                    }
                    catch (Exception) { }
                }
            }

        }
    }
}

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Common;

namespace Gravitybox.Datastore.Server.Interfaces
{
    public static class ConfigHelper
    {
        private static DateTime _lastUpdate = DateTime.MinValue;
        private static Dictionary<string, string> _settings = new Dictionary<string, string>();
        private static readonly object _syncObject = new object();
        public static string _connectionString = string.Empty;

        public static string ConnectionString
        {
            get { return _connectionString; }
            set
            {
                _connectionString = value;
                Refresh();
            }
        }

        public enum ServerVersionConstants
        {
            SQLInvalid,
            SQL2008,
            SQL2012,
            SQL2014,
            SQLOther,
        }

        #region Constructors

        static ConfigHelper()
        {
            SqlVersion = ServerVersionConstants.SQLInvalid;
        }

        private static void Refresh()
        {
            //Do not rethrow error. If error just skip this and do NOT reset the settings object to null
            try
            {
                lock (_syncObject)
                {
                    var v = GetSettings(ConnectionString);
                    if (v != null)
                        _settings = v;
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        #endregion

        #region Sql

        private static Dictionary<string, string> GetSettings(string connectionString)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = "select * from [ConfigurationSetting]";
                        command.CommandType = CommandType.Text;
                        var adapter = new SqlDataAdapter(command);
                        var ds = new DataSet();
                        adapter.Fill(ds, "Q");

                        var retval = new Dictionary<string, string>();
                        foreach (DataRow r in ds.Tables[0].Rows)
                        {
                            retval.Add(r["Name"].ToString().ToLower(), (string)r["Value"]);
                        }
                        return retval;
                    }
                }
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Timeout expired"))
                    LoggerCQ.LogWarning("ConfigHelper.GetSettings timeout expired");
                else
                    LoggerCQ.LogError(ex);
                return null;
            }
        }

        private static void SaveSetting(string connectionString, string name, string value)
        {
            try
            {
                lock (_syncObject)
                {
                    using (var connection = new SqlConnection(connectionString))
                    {
                        connection.Open();
                        using (var command = connection.CreateCommand())
                        {
                            var sb = new StringBuilder();
                            sb.AppendLine("if exists (select * from [ConfigurationSetting] where name=@name)");
                            sb.AppendLine("BEGIN");
                            sb.AppendLine("update [ConfigurationSetting] set value=@value where name=@name");
                            sb.AppendLine("END");
                            sb.AppendLine("ELSE");
                            sb.AppendLine("BEGIN");
                            sb.AppendLine("insert into [ConfigurationSetting] (name, value) values (@name, @value)");
                            sb.AppendLine("END");

                            command.CommandText = sb.ToString();
                            command.CommandType = CommandType.Text;
                            command.Parameters.Add(new SqlParameter { DbType = DbType.String, ParameterName = "name", Value = name });
                            command.Parameters.Add(new SqlParameter { DbType = DbType.String, ParameterName = "value", Value = value });
                            command.ExecuteNonQuery();
                        }
                    }
                    Refresh();
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        #endregion

        #region Setting Methods

        public static Dictionary<string, string> AllSettings
        {
            get
            {
                if (DateTime.Now.Subtract(_lastUpdate).TotalSeconds > 60)
                {
                    _lastUpdate = DateTime.Now;
                    Refresh();
                }
                return _settings;
            }
        }

        private static string GetValue(string name)
        {
            return GetValue(name, string.Empty);
        }

        private static string GetValue(string name, string defaultValue)
        {
            if (AllSettings.ContainsKey(name.ToLower()))
                return AllSettings[name.ToLower()];
            return defaultValue;
        }

        private static int GetValue(string name, int defaultValue)
        {
            int retVal;
            if (int.TryParse(GetValue(name, string.Empty), out retVal))
                return retVal;
            return defaultValue;
        }

        private static bool GetValue(string name, bool defaultValue)
        {
            bool retVal;
            if (bool.TryParse(GetValue(name, string.Empty), out retVal))
                return retVal;
            return defaultValue;
        }

        private static DateTime GetValue(string name, DateTime defaultValue)
        {
            DateTime retVal;
            if (DateTime.TryParse(GetValue(name, string.Empty), out retVal))
                return retVal;
            return defaultValue;
        }

        private static void SetValue(string name, string value)
        {
            SaveSetting(ConnectionString, name, value);
        }

        private static void SetValue(string name, long value)
        {
            SetValue(name, value.ToString());
        }

        private static void SetValue(string name, bool value)
        {
            SetValue(name, value.ToString().ToLower());
        }

        private static void SetValue(string name, DateTime value)
        {
            SetValue(name, value.ToString("yyyy-MM-dd HH:mm:ss").ToLower());
        }

        #endregion

        #region Properties

        public static string PublicKey
        {
            get { return GetValue("PublicKey", string.Empty); }
            set { SetValue("PublicKey", value); }
        }

        public static string PrivateKey
        {
            get { return GetValue("PrivateKey", string.Empty); }
            set { SetValue("PrivateKey", value); }
        }

        public static KeyPair MasterKeys
        {
            get { return new KeyPair { PublicKey = ConfigHelper.PublicKey, PrivateKey = ConfigHelper.PrivateKey }; }
        }

        public static bool AllowCaching
        {
            get { return GetValue("AllowCaching", true); }
            set { SetValue("AllowCaching", value); }
        }

        //public static bool AllowStatistics
        //{
        //    get { return GetValue("AllowStatistics", true); }
        //    set { SetValue("AllowStatistics", value); }
        //}

        public static int Port
        {
            get { return GetValue("Port", 1973); }
            set { SetValue("Port", value); }
        }

        public static string MailServer
        {
            get { return GetValue("MailServer", string.Empty); }
            set { SetValue("MailServer", value); }
        }

        public static int MailServerPort
        {
            get { return GetValue("MailServerPort", 25); }
            set { SetValue("MailServerPort", value); }
        }

        public static string MailServerUsername
        {
            get { return GetValue("MailServerUsername", string.Empty); }
            set { SetValue("MailServerUsername", value); }
        }

        public static string MailServerPassword
        {
            get { return GetValue("MailServerPassword", string.Empty); }
            set { SetValue("MailServerPassword", value); }
        }

        //public static string NotifyEmail
        //{
        //    get { return GetValue("NotifyEmail", string.Empty); }
        //    set { SetValue("NotifyEmail", value); }
        //}

        public static string DebugEmail
        {
            get { return GetValue("DebugEmail", string.Empty); }
            set { SetValue("DebugEmail", value); }
        }

        public static string FromEmail
        {
            get { return GetValue("FromEmail", string.Empty); }
            set { SetValue("FromEmail", value); }
        }

        public static bool SupportsCompression { get; set; }

        public static ServerVersionConstants SqlVersion { get; set; }

        public static bool SupportsRowsFetch
        {
            get
            {
                return ConfigHelper.SqlVersion == Gravitybox.Datastore.Server.Interfaces.ConfigHelper.ServerVersionConstants.SQL2014 ||
                    ConfigHelper.SqlVersion == Gravitybox.Datastore.Server.Interfaces.ConfigHelper.ServerVersionConstants.SQL2012 ||
                    ConfigHelper.SqlVersion == Gravitybox.Datastore.Server.Interfaces.ConfigHelper.ServerVersionConstants.SQLOther;
            }
        }

        public static DateTime LastDefrag
        {
            get { return GetValue("LastDefrag", DateTime.MinValue); }
            set { SetValue("LastDefrag", value); }
        }

        public static bool DefragIndexes
        {
            get { return GetValue("DefragIndexes", false); }
            set { SetValue("DefragIndexes", value); }
        }

        public static int QueryCacheCount
        {
            get { return GetValue("QueryCacheCount", 20000); }
            set { SetValue("QueryCacheCount", value); }
        }

        public static bool AllowLocking
        {
            get { return GetValue("AllowLocking", true); }
            set { SetValue("AllowLocking", value); }
        }

        public static string NotifyEmail
        {
            get { return GetValue("NotifyEmail", string.Empty); }
            set { SetValue("NotifyEmail", value); }
        }

        public static string AsyncCachePath
        {
            get { return GetValue("AsyncCachePath", string.Empty); }
            set { SetValue("AsyncCachePath", value); }
        }

        public static bool AllowLockStats
        {
            get { return GetValue("AllowLockStats", false); }
            set { SetValue("AllowLockStats", value); }
        }

        public static bool AllowCoreCache
        {
            get { return GetValue("AllowCoreCache", true); }
            set { SetValue("AllowCoreCache", value); }
        }

        public static bool AllowQueryCacheClearing => GetValue("AllowQueryCacheClearing", true);

        public static bool MemOpt => GetValue("MemOpt", false);

        #endregion

    }
}
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.Entity;

namespace Gravitybox.Datastore.Server.Core
{
    public static class ConfigHelper
    {
        private static DateTime _lastUpdate = DateTime.MinValue;
        private static Dictionary<string, string> _settings = new Dictionary<string, string>();
        private static readonly object _syncObject = new object();
        public static string _connectionString = string.Empty;
        private static System.Timers.Timer _timerHeartBeat = null;

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
                    var v = GetSettings();
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

        private static Dictionary<string, string> GetSettings()
        {
            try
            {
                using (var context = new DatastoreEntities(ConnectionString))
                {
                    return context.ConfigurationSetting.ToList().ToDictionary(x => x.Name.ToLower(), x => x.Value);
                }
            }
            catch (Exception ex)
            {
                if (ex.ToString().Contains("Timeout expired"))
                    LoggerCQ.LogWarning(ex, "ConfigHelper.GetSettings timeout expired");
                else
                    LoggerCQ.LogError(ex);
                return null;
            }
        }

        private static void SaveSetting(string connectionString, string name, string value)
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

        #region GetValue

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

        private static Guid GetValue(string name, Guid defaultValue)
        {
            if (Guid.TryParse(GetValue(name, string.Empty), out Guid g))
                return g;
            return defaultValue;
        }

        #endregion

        #region SetValue

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

        #endregion

        #region Properties

        public static bool AllowCaching
        {
            get { return GetValue("AllowCaching", true); }
            set { SetValue("AllowCaching", value); }
        }

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
        }

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
                return ConfigHelper.SqlVersion == ConfigHelper.ServerVersionConstants.SQL2014 ||
                    ConfigHelper.SqlVersion == ConfigHelper.ServerVersionConstants.SQL2012 ||
                    ConfigHelper.SqlVersion == ConfigHelper.ServerVersionConstants.SQLOther;
            }
        }

        public static DateTime LastDefrag
        {
            get { return GetValue("LastDefrag", DateTime.MinValue); }
            set { SetValue("LastDefrag", value); }
        }

        public static bool DefragIndexes
        {
            get { return GetValue("DefragIndexes", true); }
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

        public static string AsyncCachePath
        {
            get { return GetValue("AsyncCachePath", string.Empty); }
            set { SetValue("AsyncCachePath", value); }
        }

        public static bool AllowLockStats => GetValue("AllowLockStats", false);

        public static bool AllowCoreCache => GetValue("AllowCoreCache", true);

        public static bool AllowQueryCacheClearing => GetValue("AllowQueryCacheClearing", true);

        public static bool MemOpt => false;

        public static bool AllowCacheWithKeyword => GetValue("AllowCacheWithKeyword", false);

        #endregion

        public static Guid CurrentMaster { get; private set; }

        private static double _serverTimeSkew = 0;
        public static void StartUp()
        {
            try
            {
                using (var context = new DatastoreEntities())
                {
                    var item = context.ServiceInstance.FirstOrDefault();
                    if (item == null)
                        PromoteMaster();
                    else if (DateTime.UtcNow.AddSeconds(-_serverTimeSkew).Subtract(item.LastCommunication).TotalSeconds > 10)
                        PromoteMaster();
                }

                //Get the lastest master instance
                using (var context = new DatastoreEntities())
                {
                    var item = context.ServiceInstance.FirstOrDefault();
                    if (item != null)
                        CurrentMaster = item.InstanceId;
                }

                //Subtract My Time from database time
                //If (+) then db server is behind me, if (-) db server is ahead
                //Save the difference and (-) from all queries to normalize all service servers to db server
                _serverTimeSkew = DateTime.UtcNow.Subtract(GetDatabaseTime()).TotalSeconds;

                //This will send the heartbeat to the DB
                _timerHeartBeat = new System.Timers.Timer(5000);
                _timerHeartBeat.Elapsed += TimerHeartBeatElapsed;
                _timerHeartBeat.Start();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                throw;
            }
        }

        public static void ShutDown()
        {
            try
            {
                //When shutting down, reset the DB keep alive settings so this server is NOT the master
                //Only do this if this instance is currently the master
                using (var context = new DatastoreEntities())
                {
                    var item = context.ServiceInstance.FirstOrDefault();
                    if (item != null && item.InstanceId == RepositoryManager.InstanceId)
                    {
                        item.InstanceId = Guid.Empty;
                        item.LastCommunication = new DateTime(2000, 1, 1);
                        item.FirstCommunication = item.LastCommunication;
                        context.SaveChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
        }

        public static bool PromoteMaster()
        {
            var tryCount = 0;
            do
            {
                try
                {
                    using (var context = new DatastoreEntities())
                    {
                        var item = context.ServiceInstance.FirstOrDefault();
                        if (item == null)
                        {
                            //There is no item in the table so create one
                            context.AddItem(new ServiceInstance
                            {
                                FirstCommunication = DateTime.UtcNow.AddSeconds(-_serverTimeSkew),
                                LastCommunication = DateTime.UtcNow.AddSeconds(-_serverTimeSkew),
                                InstanceId = RepositoryManager.InstanceId,
                            });
                            context.SaveChanges();
                            return true;
                        }
                        else if (item.InstanceId == RepositoryManager.InstanceId)
                        {
                            //Nothing to do. This service is already the master
                            return true;
                        }
                        else
                        {
                            //The instance was successfully change to this service instance
                            item.InstanceId = RepositoryManager.InstanceId;
                            item.FirstCommunication = DateTime.UtcNow.AddSeconds(-_serverTimeSkew);
                            item.LastCommunication = DateTime.UtcNow.AddSeconds(-_serverTimeSkew);
                            context.SaveChanges();
                            return true;
                        }
                    }
                }
                catch (Exception)
                {
                    //If the record could not be created OR not updated try again
                    tryCount++;
                }
            } while (tryCount < 3);
            return false;
        }

        private static DateTime GetDatabaseTime()
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "select GETUTCDATE() as [Time]";
                        command.CommandType = CommandType.Text;
                        var da = new SqlDataAdapter { SelectCommand = command };
                        var ds = new DataSet();
                        da.Fill(ds);
                        if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1)
                            return (DateTime)ds.Tables[0].Rows[0][0];
                    }
                }
                return DateTime.MinValue;
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return DateTime.MinValue;
            }
        }

        private static void TimerHeartBeatElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //Send the heart beat to the DB to coordinate all data store instances
            try
            {
                _timerHeartBeat.Stop();
                using (var context = new DatastoreEntities())
                {
                    var item = context.ServiceInstance.FirstOrDefault();
                    if (item != null && item.InstanceId == RepositoryManager.InstanceId)
                    {
                        item.LastCommunication = DateTime.UtcNow.AddSeconds(-_serverTimeSkew);
                        context.SaveChanges();
                        CurrentMaster = item.InstanceId;
                    }
                    else if (item != null)
                    {
                        CurrentMaster = item.InstanceId;
                    }
                    else
                    {
                        CurrentMaster = Guid.Empty;
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
            }
            finally
            {
                _timerHeartBeat.Start();
            }
        }

    }
}
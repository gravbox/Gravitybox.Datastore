#pragma warning disable 0168
using System;
using System.Linq;
using System.IO;
using Celeriq.Utilities;

namespace Celeriq.Server.Interfaces
{
    /// <summary />
    public static class ConfigurationSettings
    {
        private const string DataPathKey = "DataPath";
        private const string MaxRunningRepositoriesKey = "MaxRunningRepositories";
        private const string MaxMemoryKey = "MaxMemory";
        private const string AutoDataUnloadTimeKey = "AutoDataUnloadTime";

        private static void Initialize()
        {
            try
            {
                var config = System.Configuration.ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetEntryAssembly().Location);
                var setting = config.AppSettings.Settings[DataPathKey];
                var pathExists = false;
                if (setting != null)
                {
                    var path = setting.Value;
                    if (!string.IsNullOrEmpty(path) && !Directory.Exists(path))
                    {
                        try
                        {
                            Directory.CreateDirectory(path);
                            pathExists = true;
                        }
                        catch (Exception ex)
                        {
                            //Do Nothing
                        }
                    }
                    else if (Directory.Exists(path))
                    {
                        pathExists = true;
                    }
                }

                //Set the data path
                if (pathExists)
                {
                    DataPath = setting.Value;
                }
                else
                {
                    DataPath = Path.Combine(ReflectionHelper.GetEntryAssemblyPath(), "Data");
                    config.AppSettings.Settings.Add(DataPathKey, DataPath);
                    config.Save();
                }

                //Load a key pair (or create if need be)
                if (config.AppSettings.Settings["PublicKey"] == null || config.AppSettings.Settings["PrivateKey"] == null)
                {
                    MasterKeys = SecurityHelper.GenerateSymmetricKeys();
                    config.AppSettings.Settings.Add("PublicKey", MasterKeys.PublicKey);
                    config.AppSettings.Settings.Add("PrivateKey", MasterKeys.PrivateKey);
                    config.Save();
                }
                else
                {
                    MasterKeys = new KeyPair();
                    MasterKeys.PublicKey = config.AppSettings.Settings["PublicKey"].Value;
                    MasterKeys.PrivateKey = config.AppSettings.Settings["PrivateKey"].Value;
                }

                //Load other settings
                if (config.AppSettings.Settings[MaxRunningRepositoriesKey] == null)
                    config.AppSettings.Settings.Add(MaxRunningRepositoriesKey, MaxRunningRepositories.ToString());
                else
                    MaxRunningRepositories = Convert.ToInt32(config.AppSettings.Settings[MaxRunningRepositoriesKey].Value);

                if (config.AppSettings.Settings[MaxMemoryKey] == null)
                    config.AppSettings.Settings.Add(MaxMemoryKey, MaxMemory.ToString());
                else
                    MaxMemory = Convert.ToInt64(config.AppSettings.Settings[MaxMemoryKey].Value);

                if (config.AppSettings.Settings[AutoDataUnloadTimeKey] == null)
                    config.AppSettings.Settings.Add(AutoDataUnloadTimeKey, AutoDataUnloadTime.ToString());
                else
                    AutoDataUnloadTime = Convert.ToInt32(config.AppSettings.Settings[AutoDataUnloadTimeKey].Value);

            }
            catch (Exception ex)
            {
                Logger.LogError(ex);
                throw new Exception("Celeriq has not been properly installed on this machine. Error 0x1981");
            }
        }

        static ConfigurationSettings()
        {
            Initialize();
        }

        public static void Save()
        {
            try
            {
                var config = System.Configuration.ConfigurationManager.OpenExeConfiguration(System.Reflection.Assembly.GetEntryAssembly().Location);

                var setting = config.AppSettings.Settings[DataPathKey];
                if (setting == null) config.AppSettings.Settings.Add(DataPathKey, DataPath);
                else config.AppSettings.Settings[DataPathKey].Value = DataPath;

                setting = config.AppSettings.Settings[MaxRunningRepositoriesKey];
                if (setting == null) config.AppSettings.Settings.Add(MaxRunningRepositoriesKey, MaxRunningRepositories.ToString());
                else config.AppSettings.Settings[MaxRunningRepositoriesKey].Value = MaxRunningRepositories.ToString();

                setting = config.AppSettings.Settings[MaxMemoryKey];
                if (setting == null) config.AppSettings.Settings.Add(MaxMemoryKey, MaxMemory.ToString());
                else config.AppSettings.Settings[MaxMemoryKey].Value = MaxMemory.ToString();

                setting = config.AppSettings.Settings[AutoDataUnloadTimeKey];
                if (setting == null) config.AppSettings.Settings.Add(AutoDataUnloadTimeKey, AutoDataUnloadTime.ToString());
                else config.AppSettings.Settings[AutoDataUnloadTimeKey].Value = AutoDataUnloadTime.ToString();

                config.Save();

            }
            catch (Exception ex)
            {
                Logger.LogError(ex);
                throw;
            }
        }

        private static KeyPair _masterKeys;
        private static string _dataPath;
        private static int _maxRunningRepositories;
        private static long _maxMemory;
        private static int _autoDataUnloadTime;

        public static KeyPair MasterKeys
        {
            get { return _masterKeys; }
            set { _masterKeys = value; }
        }

        public static string DataPath
        {
            get { return _dataPath; }
            set { _dataPath = value; }
        }

        public static int MaxRunningRepositories
        {
            get { return _maxRunningRepositories; }
            set { _maxRunningRepositories = value; }
        }

        public static long MaxMemory
        {
            get { return _maxMemory; }
            set { _maxMemory = value; }
        }

        public static int AutoDataUnloadTime
        {
            get { return _autoDataUnloadTime; }
            set { _autoDataUnloadTime = value; }
        }

    }
}
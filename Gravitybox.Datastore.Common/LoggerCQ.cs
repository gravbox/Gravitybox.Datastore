#pragma warning disable 0168
using System;
using System.Linq;
using NLog;
using NLog.Config;
using NLog.Targets;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class LoggerCQ
    {
        #region Class Members

        private static Logger _logger = null;

        #endregion

        #region Constructor

        static LoggerCQ()
        {
            _logger = NLog.LogManager.GetLogger(string.Empty);
        }

        #endregion

        #region Logging

        /// <summary />
        public static void LogError(string message)
        {
            try
            {
                if (_logger != null)
                {
                    _logger.Error(message);
                }
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        /// <summary />
        public static void LogError(Exception exception, string message)
        {
            try
            {
                if (_logger != null)
                    _logger.Error(message + "\n" + exception.ToString());
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        /// <summary />
        public static void LogError(Exception exception)
        {
            try
            {
                if (_logger != null)
                    _logger.Error(exception.ToString());
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        /// <summary />
        public static void LogDebug(string message)
        {
            try
            {
                if (_logger != null)
                    _logger.Debug(message);
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        /// <summary />
        public static void LogInfo(string message)
        {
            try
            {
                if (_logger != null)
                    _logger.Info(message);
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        /// <summary />
        public static void LogWarning(string message)
        {
            try
            {
                if (_logger != null)
                    _logger.Warn(message);
            }
            catch (Exception)
            {
                //Do Nothing
            }
        }

        #endregion
    }
}
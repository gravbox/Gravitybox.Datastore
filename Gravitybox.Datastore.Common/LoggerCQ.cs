using System;
using NLog;

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
            _logger = LogManager.GetLogger("Gravitybox.Datastore");
        }

        #endregion

        #region Logging

        /// <summary />
        public static void LogError(string message)
        {
            _logger.Error(message);
        }

        /// <summary />
        public static void LogError(Exception exception, string message)
        {
            _logger.Error(exception, message);
        }

        /// <summary />
        public static void LogError(Exception exception)
        {
            _logger.Error(exception);
        }

        /// <summary />
        public static void LogDebug(string message)
        {
            _logger.Debug(message);
        }

        /// <summary />
        public static void LogInfo(string message)
        {
            _logger.Info(message);
        }

        /// <summary />
        public static void LogWarning(string message)
        {
            _logger.Warn(message);
        }

        /// <summary />
        public static void LogWarning(Exception exception, string message)
        {
            _logger.Warn(exception, message);
        }

        /// <summary />
        public static void LogWarning(Exception exception)
        {
            _logger.Warn(exception);
        }

        #endregion
    }
}

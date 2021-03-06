using System;
using NLog;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class LoggerCQ
    {
        #region Class Members

        private static readonly Logger _logger;

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
            _logger.Error(message.Replace('\n','_').Replace('\r','_'));
        }

        /// <summary />
        public static void LogError(Exception exception, string message)
        {
            _logger.Error(exception, message.Replace('\n', '_').Replace('\r', '_'));
        }

        /// <summary />
        public static void LogError(Exception exception)
        {
            _logger.Error(exception);
        }

        /// <summary />
        public static void LogDebug(string message)
        {
            _logger.Debug(message.Replace('\n', '_').Replace('\r', '_'));
        }

        /// <summary />
        public static void LogInfo(string message)
        {
            _logger.Info(message.Replace('\n', '_').Replace('\r', '_'));
        }

        /// <summary />
        public static void LogWarning(string message)
        {
            _logger.Warn(message.Replace('\n', '_').Replace('\r', '_'));
        }

        /// <summary />
        public static void LogWarning(Exception exception, string message)
        {
            _logger.Warn(exception, message.Replace('\n', '_').Replace('\r', '_'));
        }

        /// <summary />
        public static void LogWarning(Exception exception)
        {
            _logger.Warn(exception);
        }

        public static void LogTrace(string message)
        {
            _logger.Trace(message.Replace('\n', '_').Replace('\r', '_'));
        }

        #endregion
    }
}

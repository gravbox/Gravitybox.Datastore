using System;
using System.Collections.Generic;
using NLog;
using NLog.Targets;
using StackExchange.Exceptional;

namespace Gravitybox.Datastore.WinService.Logging
{
    public class ExceptionalErrorStoreTarget : Target
    {
        protected override void Write(LogEventInfo logEvent)
        {
            var exception = logEvent.Exception;
            // Only capture log events that include an exception.
            if (exception == null) return;

            var logMessage = logEvent.FormattedMessage;
            var logLevel = logEvent.Level.ToString().ToUpperInvariant();
            var loggerName = !string.IsNullOrEmpty(logEvent.LoggerName) ? logEvent.LoggerName : "<empty>";

            var logData = new Dictionary<string, string>
            {
                { "NLog-Level", logLevel },
                { "NLog-LoggerName", loggerName }
            };

            if (!string.IsNullOrEmpty(logMessage))
            {
                logData.Add("NLog-Message", logMessage);
            }

            ErrorStore.LogExceptionWithoutContext(exception, customData: logData);
        }
    }
}

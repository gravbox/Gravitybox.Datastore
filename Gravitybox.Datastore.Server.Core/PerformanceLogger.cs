using System;
using System.Diagnostics;
using Gravitybox.Datastore.Common;

namespace Gravitybox.Datastore.Server.Core
{
    internal class PerformanceLogger : IDisposable
    {
        private readonly string _message;
        private readonly Stopwatch _timer;
        
        public PerformanceLogger(string message)
        {
            _message = message;
            LoggerCQ.LogTrace($"BEGIN {_message}");

            _timer = Stopwatch.StartNew();
        }
        
        public void Dispose()
        {
            if (_timer.IsRunning)
            {
                _timer.Stop();
                LoggerCQ.LogTrace($"END {_message}: Elapsed={_timer.ElapsedMilliseconds}");
            }
        }
    }
}
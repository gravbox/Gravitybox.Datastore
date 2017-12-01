using System;
using Polly;

namespace Gravitybox.Datastore.Common
{
    /// <summary>
    /// A mechanism to perform some action N times when encountering an error
    /// </summary>
    internal static class RetryHelper
    {
        /// <summary>
        /// Waits 1s before retrying.
        /// </summary>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        public static Policy DefaultRetryPolicy(int retryCount)
            => DefaultRetryPolicy<Exception>(retryCount);

        /// <summary>
        /// Waits 1s before retrying.
        /// </summary>
        /// <typeparam name="TException"></typeparam>
        /// <param name="retryCount"></param>
        /// <returns></returns>
        public static Policy DefaultRetryPolicy<TException>(int retryCount)
            where TException : Exception
        {
            return Policy.Handle<TException>()
                .WaitAndRetry(retryCount,
                    _ => TimeSpan.FromMilliseconds(1000),
                    (exception, waitDuration, tryCount, retryContext) => { });
        }
    }
}

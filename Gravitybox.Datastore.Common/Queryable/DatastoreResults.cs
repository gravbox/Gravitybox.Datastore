using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

namespace Gravitybox.Datastore.Common.Queryable
{
    /// <summary />
    public interface IDatastoreItems
    {
        /// <summary />
        List<IDatastoreItem> Items { get; }
    }

    /// <summary />
    [Serializable]
    public class DatastoreItems<TResultType>
    {
        /// <summary />
        public List<TResultType> Items { get; set; }

        /// <summary />
        public int TotalPageCount { get; set; }

        /// <summary />
        public int TotalRecordCount { get; set; }

        /// <summary>
        /// The query parameters used to generate this result set
        /// </summary>
        public DataQuery Query { get; set; }
    }

    /// <summary />
    [Serializable]
    public class DatastoreResults<TResultType> : DatastoreItems<TResultType>, IDatastoreItems
        where TResultType : IDatastoreItem
    {
        /// <summary>
        /// The remaining dimensions that can be used to further filter this result set
        /// </summary>
        public DimensionStore<TResultType> DimensionStore { get; set; }

        /// <summary>
        /// The full set of dimensions over the total set of data with no filtering applied
        /// </summary>
        public DimensionStore<TResultType> AllDimensions { get; set; }

        /// <summary>
        /// The dimensions used to filter the executed query
        /// </summary>
        public DimensionStore<TResultType> AppliedDimensionList { get; set; }

        /// <summary>
        /// All of the field filters used in the query
        /// </summary>
        public List<IFieldFilter> AppliedFieldFilterList { get; set; }

        /// <summary>
        /// Diagnostic information about the query
        /// </summary>
        public ResultDiagnostics Diagnostics { get; set; } = new ResultDiagnostics();

        ///// <summary>
        ///// The query parameters used to generate this result set
        ///// </summary>
        //public DataQuery Query { get; set; } = new DataQuery();

        /// <summary />
        public List<DerivedFieldValue> DerivedFieldList { get; set; } = new List<DerivedFieldValue>();

        List<IDatastoreItem> IDatastoreItems.Items
        {
            get { return this.Items.Cast<IDatastoreItem>().ToList(); }
        }
    }

    /// <summary />
    [Serializable]
    public class ActionDiagnostics
    {
        /// <summary>
        /// This is the total time in milliseconds to execute the query
        /// </summary>
        public long ComputeTime { get; set; }

        /// <summary>
        /// This is the total time in milliseconds to get a lock before processing
        /// </summary>
        public long LockTime { get; set; }

        /// <summary>
        /// The repository that this dataset came from
        /// </summary>
        public Guid RepositoryId { get; set; }

        /// <summary>
        /// Determines if the action was successful
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// A list of errors that occured during the operation
        /// </summary>
        public string[] Errors { get; set; }
    }

    /// <summary />
    [Serializable]
    public class ResultDiagnostics : ActionDiagnostics
    {
        /// <summary>
        /// This is the current version of the data on the server. 
        /// When data changes in a repository this number also changes.
        /// </summary>
        public int DataVersion { get; set; }

        /// <summary>
        /// A unique hash for this snapshot data
        /// </summary>
        public long VersionHash { get; set; }

        /// <summary>
        /// Determines if this data was pulled from server cache
        /// </summary>
        public bool CacheHit { get; set; }
    }

    /// <summary />
    [Serializable]
    public class DimensionStore<TSourceType> : List<DimensionItem>
        where TSourceType : IDatastoreItem
    {
        private NamedItemList nonParsedFieldList;

        /// <summary />
        public DimensionStore()
        {
        }

        /// <summary />
        public DimensionStore(IEnumerable<DimensionItem> items)
            : this()
        {
            if (items != null)
                this.AddRange(items);
        }

        /// <summary />
        public DimensionStore(IEnumerable<DimensionItem> items, NamedItemList nonParsedFieldList)
            : this(items)
        {
            this.nonParsedFieldList = nonParsedFieldList;
        }

        /// <summary>
        /// Gets a single dimension. If it does not exist an empty object is returned
        /// </summary>
        public DimensionItem Where<T>(Expression<Func<TSourceType, T>> member)
        {
            var memberName = ExpressionHelper.GetMemberName(member);
            if (memberName == null)
                return null;

            return Where(memberName);
        }

        /// <summary />
        public DimensionItem Where<T>(Expression<Func<T, object>> member)
        {
            var memberName = ExpressionHelper.GetMemberName(member);
            if (memberName == null)
                return null;

            return Where(memberName);
        }

        /// <summary />
        public DimensionItem Where(string name)
        {
            var retval = this.FirstOrDefault(x => x.Name == name);
            if (retval == null)
            {
                retval = new DimensionItem
                {
                    Name = name,
                };
            }
            return retval;
        }

    }

    /// <summary />
    public class DatastoreResultsAsync
    {
        /// <summary />
        private DatastoreService _dsService;
        /// <summary />
        protected Guid _hookId;
        /// <summary />
        protected DataQuery _query;
        /// <summary />
        protected string _dataFile = null;

        /// <summary />
        internal DatastoreResultsAsync(DatastoreService dsService, DataQuery query)
        {
            try
            {
                _dsService = dsService;
                _query = query;
                _hookId = _dsService.QueryAsync(query);
            }
            catch
            {
                this.IsComplete = true;
                throw;
            }
        }

        /// <summary />
        public bool IsComplete { get; private set; }

        /// <summary />
        public void WaitUntilReady()
        {
            try
            {
                if (this.IsComplete) return;
                while (!_dsService.QueryAsyncReady(_hookId))
                {
                    System.Threading.Thread.Sleep(1000);
                }

                //Get chunks and append to file
                var chunk = 0;
                byte[] chunkData = null;
                var tempFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
                _dataFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
                do
                {
                    chunkData = _dsService.QueryAsyncDownload(_hookId, chunk);
                    if (chunkData != null)
                    {
                        using (var stream = new FileStream(tempFile, FileMode.Append))
                        {
                            stream.Write(chunkData, 0, chunkData.Length);
                        }
                        chunk++;
                    }
                } while (chunkData != null);

                Extensions.UnzipFile(tempFile, _dataFile);
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
            catch
            {
                _dataFile = null;
                throw;
            }
            finally
            {
                this.IsComplete = true;
            }
        }

        /// <summary />
        public string OutputFile
        {
            get
            {
                if (!this.IsComplete) return null;
                return _dataFile;
            }
        }
    }

}

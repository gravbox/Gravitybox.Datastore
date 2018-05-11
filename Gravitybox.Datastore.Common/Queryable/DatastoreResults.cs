using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Xml;
using System.Xml.Linq;

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
    public class DatastoreResultsAsync<T> : IDisposable
        where T : IDatastoreItem, new()
    {
        /// <summary />
        private DatastoreService _dsService;
        /// <summary />
        protected Guid _hookId;
        /// <summary />
        protected DataQuery _query;
        /// <summary />
        protected string _dataFile = null;
        protected RepositorySchema _schema = null;
        private List<DimensionItem> _headers = new List<DimensionItem>();
        private List<DimensionItem> _dimensions = new List<DimensionItem>();
        private XmlReader _reader = null;

        /// <summary />
        internal DatastoreResultsAsync(DatastoreService dsService, DataQuery query)
        {
            try
            {
                _dsService = dsService;
                _query = query;
                _hookId = _dsService.QueryAsync(query);
                _schema = _dsService.Schema;
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
        public void WaitUntilReady(long timeout = 0)
        {
            if (timeout < 0) timeout = 0;
            try
            {
                if (this.IsComplete) return;

                #region Wait for file to be ready
                var startTime = DateTime.Now;
                while (!_dsService.QueryAsyncReady(_hookId))
                {
                    //If there is a timeout and it is exceeded then throw exception
                    if (timeout > 0 && DateTime.Now.Subtract(startTime).TotalMilliseconds > timeout)
                        throw new TimeoutException($"Time exceeded {timeout}ms");

                    System.Threading.Thread.Sleep(5000);
                }
                #endregion

                #region Get chunks and append to file
                var chunk = 0;
                byte[] chunkData = null;
                var tempFile = Path.Combine(this.GetTempPath(), Guid.NewGuid().ToString());

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
                #endregion

                #region Unzip the file in temp folder
                _dataFile = Path.Combine(this.GetTempPath(), Guid.NewGuid().ToString());
                Extensions.UnzipFile(tempFile, _dataFile);
                if (File.Exists(tempFile)) File.Delete(tempFile);
                #endregion

                #region Load file headers and dimensions
                //This will dispose in the dispose event
                _reader = XmlReader.Create(_dataFile);

                var inHeaders = false;
                while (_reader.Read())
                {
                    if (inHeaders && _reader.Name == "h" && _reader.NodeType == XmlNodeType.Element)
                    {
                        string tt = null;
                        var didx = Convert.ToInt64("0" + _reader.GetAttribute("didx"));
                        _reader.Read();

                        if (_reader.NodeType == XmlNodeType.Text)
                            tt = _reader.Value;

                        _headers.Add(new DimensionItem
                        {
                            DIdx = didx,
                            Name = tt,
                        });
                    }
                    if (_reader.Name == "headers") inHeaders = true;
                    if (_reader.Name == "dimensions") break;
                }

                DimensionItem currentD = null;
                while (_reader.Read())
                {
                    if (_reader.Name == "d" && _reader.NodeType == XmlNodeType.Element)
                    {
                        var didx = Convert.ToInt64("0" + _reader.GetAttribute("didx"));
                        var tt = _reader.GetAttribute("name");
                        currentD = new DimensionItem
                        {
                            Name = tt,
                            DIdx = didx,
                        };
                        _dimensions.Add(currentD);
                    }
                    else if (_reader.Name == "r" && _reader.NodeType == XmlNodeType.Element)
                    {
                        string tt = null;
                        var dvidx = Convert.ToInt64("0" + _reader.GetAttribute("dvidx"));
                        _reader.Read();

                        if (_reader.NodeType == XmlNodeType.Text)
                            tt = _reader.Value;

                        currentD.RefinementList.Add(new RefinementItem
                        {
                            DIdx = currentD.DIdx,
                            DVIdx = dvidx,
                            FieldValue = tt,
                        });
                    }
                    if (_reader.Name == "items") break;
                }

                _dimensions.RemoveAll(x => x.DIdx == 0);

                #endregion

                this.IsComplete = true;
            }
            catch (Exception ex)
            {
                this.IsComplete = true;
                _dataFile = null;
                throw;
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

        /// <summary />
        public bool EOF { get; private set; }

        /// <summary />
        public int CurrentIndex { get; private set; } = 0;

        /// <summary />
        public List<T> GetItems(int count)
        {
            //All records processed
            if (this.EOF) return new List<T>();

            try
            {
                //When we come to this method we are already queued up to the items array in file
                var retval = new List<T>();
                var ordinalPosition = 0;
                var localIndex = 0;
                //if (!_reader.ReadToFollowing("i"))
                //{
                //    this.EOF = true;
                //    return retval;
                //}

                while (!_reader.EOF)
                {
                    if (_reader.Name == "i" && _reader.NodeType == XmlNodeType.Element)
                    {
                        long.TryParse(_reader.GetAttribute("ri"), out long ri);
                        int.TryParse(_reader.GetAttribute("ts"), out int ts);

                        //Setup static values
                        var newItem = new T();
                        newItem.__RecordIndex = ri;
                        newItem.__Timestamp = ts;
                        newItem.__OrdinalPosition = ordinalPosition++;
                        newItem.ExtraValues = new Dictionary<string, string>();

                        //Loop through all properties for this new item
                        var elementXml = _reader.ReadOuterXml();
                        var doc = XDocument.Parse(elementXml);
                        var fieldIndex = 0;
                        foreach (var n in doc.Descendants().Where(x => x.Name == "v"))
                        {
                            var prop = newItem.GetType().GetProperty(_headers[fieldIndex].Name);
                            if (prop != null && prop.CanWrite && prop.GetSetMethod(true) != null)
                            {
                                var isNull = n.Value == "~■!N";
                                if (isNull)
                                {
                                    prop.SetValue(newItem, null, null);
                                }
                                else if (prop.PropertyType == typeof(int?) || prop.PropertyType == typeof(int))
                                {
                                    prop.SetValue(newItem, int.Parse(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(byte?) || prop.PropertyType == typeof(byte))
                                {
                                    prop.SetValue(newItem, Convert.ToByte(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(short?) || prop.PropertyType == typeof(short))
                                {
                                    prop.SetValue(newItem, Convert.ToInt16(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(DateTime?) || prop.PropertyType == typeof(DateTime))
                                {
                                    var dt = new DateTime(Convert.ToInt64(n.Value));
                                    prop.SetValue(newItem, dt, null);
                                }
                                else if (prop.PropertyType == typeof(bool?) || prop.PropertyType == typeof(bool))
                                {
                                    prop.SetValue(newItem, n.Value == "1", null);
                                }
                                else if (prop.PropertyType == typeof(Single?) || prop.PropertyType == typeof(Single))
                                {
                                    prop.SetValue(newItem, Convert.ToSingle(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(double?) || prop.PropertyType == typeof(double))
                                {
                                    prop.SetValue(newItem, Convert.ToDouble(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(GeoCode))
                                {
                                    var geoArr = n.Value.Split('|');
                                    var geo = new GeoCode { Latitude = Convert.ToDouble(geoArr[0]), Longitude = Convert.ToDouble(geoArr[1]) };
                                    prop.SetValue(newItem, geo, null);
                                }
                                else if (prop.PropertyType == typeof(string))
                                {
                                    prop.SetValue(newItem, n.Value, null);
                                }
                                else if (prop.PropertyType == typeof(string[]))
                                {
                                    //Get real values
                                    var d = _dimensions.FirstOrDefault(x => x.DIdx == _headers[fieldIndex].DIdx);
                                    if (d != null)
                                    {
                                        var varr = n.Value.Split('|').Select(x => Convert.ToInt64(x)).ToList();
                                        var v = d.RefinementList.Where(x => varr.Contains(x.DVIdx)).Select(x => x.FieldValue).ToArray();
                                        prop.SetValue(newItem, v, null);
                                    }
                                }
                                else
                                {
                                    //Should never hit this if all types are handled
                                }
                            } //property exists
                            if (prop != null && (!prop.CanWrite || prop.GetSetMethod(true) == null))
                            {
                                //Do Nothing : Found property but it is not writable so there is nothing that we can do
                            }
                            else
                            {
                                //Metadata
                                var isNull = n.Value == "~■!N";
                                if (isNull)
                                    newItem.ExtraValues.Add(_headers[fieldIndex].Name, null);
                                else
                                    newItem.ExtraValues.Add(_headers[fieldIndex].Name, n.Value);
                            }

                            fieldIndex++;
                        }
                        localIndex++;
                        this.CurrentIndex++;

                        //Add the item and break out if hit max count
                        retval.Add(newItem);
                        if (localIndex >= count) break;
                    }
                    else
                    {
                        _reader.ReadToFollowing("i");
                    }
                }
                this.EOF = _reader.EOF;
                return retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// The folder to use for temp files. If the folder does not exist, the default temp path is used.
        /// </summary>
        public string TempFolder { get; set; } = null;

        private string GetTempPath()
        {
            if (!string.IsNullOrEmpty(this.TempFolder) && Directory.Exists(this.TempFolder))
                return this.TempFolder;
            return Path.GetTempPath();
        }

        void IDisposable.Dispose()
        {
            //Try to close the reader
            try
            {
                if (_reader != null)
                    _reader.Close();
                _reader = null;
            }
            catch { }

            //Try to delete the temp file
            try
            {
                if (File.Exists(_dataFile))
                    File.Delete(_dataFile);
            }
            catch { }
        }
    }

}

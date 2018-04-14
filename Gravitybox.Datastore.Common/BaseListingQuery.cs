#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.ComponentModel;
using System.Web;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    [Serializable]
    [DataContract]
    [KnownType(typeof (NamedItem))]
    [KnownType(typeof (NamedItemList))]
    [KnownType(typeof (UserCredentials))]
    public partial class BaseListingQuery : Gravitybox.Datastore.Common.IListingQuery
    {
        /// <summary />
        public BaseListingQuery()
        {
            this.Reset();
        }

        /// <summary />
        public BaseListingQuery(string url)
            : this()
        {
            if (string.IsNullOrEmpty(url)) return;
            if (url.Contains("%")) url = System.Web.HttpUtility.UrlDecode(url);
            var originalUrl = url;

            var pageBreak = url.IndexOf('?');
            if (pageBreak != -1 && pageBreak < url.Length - 1)
            {
                this.PageName = url.Substring(0, pageBreak);
                url = url.Substring(pageBreak + 1, url.Length - pageBreak - 1);
            }
            else
            {
                this.PageName = url;
                return;
            }

            #region Parse Query String
            var tuplets = url.Split('&');
            foreach (var gset in tuplets)
            {
                var values = gset.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                if (values.Length == 2)
                {
                    switch (values[0])
                    {
                        case "_ied":
                            if (values[1] == "1" || values[1] == "true")
                                this.IncludeEmptyDimensions = true;
                            break;
                        case "_ec":
                            if (values[1] == "1" || values[1] == "true")
                                this.ExcludeCount = true;
                            break;
                        case "_id":
                            if (values[1] == "0" || values[1] == "false")
                                this.IncludeDimensions = false;
                            break;
                        case "_ir":
                            if (values[1] == "0" || values[1] == "false")
                                this.IncludeRecords = false;
                            break;
                        case "d":
                            {
                                var dValues = values[1].Split(new char[] { '+', ' ' }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (var dvidxV in dValues)
                                {
                                    long dvidx;
                                    if (long.TryParse(dvidxV, out dvidx))
                                        this.DimensionValueList.Add(dvidx);
                                }
                            }
                            break;
                        case "po":
                            {
                                if (int.TryParse(values[1], out int po))
                                    this.PageOffset = po;
                            }
                            break;
                        case "rpp":
                            {
                                if (int.TryParse(values[1], out int rpp))
                                    this.RecordsPerPage = rpp;
                            }
                            break;
                        case "srch":
                            this.Keyword = HttpUtility.UrlDecode(values[1]);
                            break;
                        case "ff":
                            {
                                var filters = values[1].Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (var s in filters)
                                {
                                    var svalues = s.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                                    if (svalues.Length == 5)
                                    {
                                        IFieldFilter ff = new GeoCodeFieldFilter();
                                        if (ff.FromUrl(s))
                                            this.FieldFilters.Add(ff);
                                    }
                                    else
                                    {
                                        IFieldFilter ff = new FieldFilter();
                                        if (ff.FromUrl(s))
                                            this.FieldFilters.Add(ff);
                                    }
                                }
                            }
                            break;
                        case "fs":
                            {
                                var sorts = values[1].Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (var s in sorts)
                                {
                                    var svalues = s.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                                    if (svalues.Length > 0)
                                    {
                                        this.FieldSorts.Add(new FieldSort() { Name = svalues[0], SortDirection = (svalues.Length == 1 || svalues[1] != "0" ? Gravitybox.Datastore.Common.SortDirectionConstants.Asc : Gravitybox.Datastore.Common.SortDirectionConstants.Desc) });
                                    }
                                }
                            }
                            break;
                        case "fsel":
                            this.FieldSelects = values[1].Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries).ToList();
                            break;
                        case "gf":
                            this.GroupFields = values[1].Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries).ToList();
                            break;
                        case "ul":
                            {
                                if (this.UserList == null) this.UserList = new List<int>();
                                else this.UserList.Clear();
                                var lsv = values[1].Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (var lv in lsv)
                                {
                                    if (int.TryParse(lv, out int v))
                                        this.UserList.Add(v);
                                }
                            }
                            break;
                        case "skipd":
                            {
                                if (this.SkipDimensions == null) this.SkipDimensions = new List<long>();
                                else this.SkipDimensions.Clear();
                                var lsv = values[1].Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                                foreach (var lv in lsv)
                                {
                                    if (long.TryParse(lv, out long v))
                                        this.SkipDimensions.Add(v);
                                }
                            }
                            break;
                        default:
                            if (values.Length >= 2)
                            {
                                if (this.NonParsedFieldList.Count(x => x.Key == values[0]) > 0)
                                    this.NonParsedFieldList.First(x => x.Key == values[0]).Value = values[1];
                                else
                                    this.NonParsedFieldList.Add(new NamedItem() { Key = values[0], Value = values[1] });
                            }
                            break;
                    }
                }
            }
            #endregion

        }

        /// <summary />
        public virtual void Reset()
        {
            this.PageName = null;
            this.DimensionValueList = new List<long>();
            this.FieldFilters = new List<IFieldFilter>();
            this.FieldSorts = new List<IFieldSort>();
            this.FieldSelects = null;
            this.GroupFields = null;
            this.PageOffset = 1;
            this.RecordsPerPage = 10;
            this.Keyword = null;
            this.NonParsedFieldList = new NamedItemList();
            this.QueryID = Guid.NewGuid().ToString();
            this.IncludeDimensions = true;
            this.IncludeEmptyDimensions = false;
            this.IncludeRecords = true;
            this.SkipDimensions = new List<long>();
        }

        #region Events

        /// <summary />
        [field: NonSerialized]
        public event EventHandler<Gravitybox.Datastore.Common.EventArguments.BeforeValueEventArgs<string>> AfterLoadFromUrlComplete;

        /// <summary />
        [field: NonSerialized]
        public event EventHandler<Gravitybox.Datastore.Common.EventArguments.BeforeValueEventArgs<string>> AfterPostToString;

        /// <summary />
        protected virtual void OnAfterLoadFromUrlComplete(Gravitybox.Datastore.Common.EventArguments.BeforeValueEventArgs<string> e)
        {
            if (this.AfterLoadFromUrlComplete != null)
                this.AfterLoadFromUrlComplete(this, e);
        }

        /// <summary />
        protected virtual void OnAfterPostToString(Gravitybox.Datastore.Common.EventArguments.BeforeValueEventArgs<string> e)
        {
            if (this.AfterPostToString != null)
                this.AfterPostToString(this, e);
        }

        #endregion

        /// <summary />
        [DataMember]
        public List<IDerivedField> DerivedFieldList { get; set; }

        /// <summary />
        [DataMember]
        public bool IncludeEmptyDimensions { get; set; }

        /// <summary />
        [DataMember]
        public bool UseDefaults { get; set; }

        /// <summary />
        [DataMember]
        public List<long> DimensionValueList { get; set; }

        /// <summary />
        [DataMember]
        public int PageOffset { get; set; }

        /// <summary />
        [DataMember]
        public int RecordsPerPage { get; set; }

        /// <summary />
        [DataMember]
        public string Keyword { get; set; }

        /// <summary />
        [DataMember]
        public List<Gravitybox.Datastore.Common.IFieldFilter> FieldFilters { get; set; } = new List<IFieldFilter>();

        /// <summary />
        [DataMember]
        public List<Gravitybox.Datastore.Common.IFieldSort> FieldSorts { get; set; } = new List<IFieldSort>();

        /// <summary>
        /// This is null except when a specific list of fields is requested
        /// </summary>
        [DataMember]
        public List<string> FieldSelects { get; set; }

        /// <summary />
        [DataMember]
        public List<string> GroupFields { get; set; }

        /// <summary />
        [DataMember]
        public NamedItemList NonParsedFieldList { get; set; }

        /// <summary />
        [DataMember]
        public string PageName { get; set; }

        /// <summary />
        [DataMember]
        public string QueryID { get; set; }

        /// <summary />
        [DataMember]
        public bool IncludeRecords { get; set; }

        /// <summary>
        /// If this is false, dimension data is not returned
        /// </summary>
        [DataMember]
        public bool IncludeDimensions { get; set; }

        /// <summary />
        [DataMember]
        [DefaultValue(true)]
        public bool ExcludeCount { get; set; }

        /// <summary>
        /// Given a set of DIdx values, the associated dimensions will not be calculated with the returned results
        /// </summary>
        [DataMember]
        public List<long> SkipDimensions { get; set; } = new List<long>();

        /// <summary>
        /// The IP to use as the source for logging. If missing the requesting IP will be logged
        /// </summary>
        [DataMember]
        public string IPMask { get; set; }

        /// <summary />
        [DataMember]
        public List<int> UserList { get; set; }

        /// <summary />
        public override int GetHashCode()
        {
            var hash = new StringBuilder();
            if (this.DimensionValueList == null || this.DimensionValueList.Count == 0)
                hash.Append("NULL|");
            else
                hash.Append(string.Join("-", this.DimensionValueList.OrderBy(x => x).ToArray()) + "|");

            hash.Append(this.PageOffset + "|" + this.RecordsPerPage + "|" + this.Keyword + "|");

            if (this.FieldFilters == null || this.FieldFilters.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldFilters.OrderBy(x => x.Name))
                    hash.Append(o.GetHashCode() + "~|");
            }

            if (this.FieldSorts == null || this.FieldSorts.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldSorts)
                    hash.Append(o.GetHashCode() + "!|");
            }

            if (this.FieldSelects == null || this.FieldSelects.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldSelects)
                    hash.Append(o.GetHashCode() + "@|");
            }

            if (this.GroupFields == null || this.GroupFields.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.GroupFields)
                    hash.Append(o.GetHashCode() + "#|");
            }

            if (this.IncludeEmptyDimensions)
                hash.Append("_ied|");

            if ((this.NonParsedFieldList == null) || (this.NonParsedFieldList.Count == 0))
                hash.Append("NULL|");
            else
            {
                foreach (var item in this.NonParsedFieldList.OrderBy(x => x.Key))
                    hash.Append(item.Key + "|" + item.Value + "|");
            }

            hash.Append((this.IncludeDimensions ? "1" : "0") + "|");
            hash.Append((this.IncludeRecords ? "1" : "0") + "|");
            hash.Append((this.IncludeEmptyDimensions ? "1" : "0") + "|");
            hash.Append((this.ExcludeCount ? "1" : "0") + "|");

            if (this.UserList != null)
            {
                hash.Append(string.Join("-", this.UserList.OrderBy(x => x)) + "|");
            }

            if (this.SkipDimensions != null)
                hash.Append(string.Join("-", this.SkipDimensions.OrderBy(x => x).Select(x => x.ToString())));
            hash.Append("|");

            if (this.DerivedFieldList != null)
                this.DerivedFieldList.OrderBy(x => x.Field).ToList().ForEach(x => hash.Append(x.Field + "|" + x.Action + "|"));

            return EncryptionDomain.Hash(hash.ToString());
        }

        /// <summary />
        public virtual int CoreHashCode()
        {
            var hash = new StringBuilder();
            if (this.DimensionValueList == null || this.DimensionValueList.Count == 0)
                hash.Append("NULL|");
            else
                hash.Append(string.Join("-", this.DimensionValueList.OrderBy(x => x).ToArray()) + "|");

            hash.Append(this.Keyword + "|");

            if (this.FieldFilters == null || this.FieldFilters.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldFilters.OrderBy(x => x.Name))
                    hash.Append(o.GetHashCode() + "~|");
            }

            if (this.FieldSelects == null || this.FieldSelects.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldSelects)
                    hash.Append(o.GetHashCode() + "@|");
            }

            if (this.GroupFields == null || this.GroupFields.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.GroupFields)
                    hash.Append(o.GetHashCode() + "#|");
            }

            if (this.IncludeEmptyDimensions)
                hash.Append("_ied|");

            hash.Append((this.IncludeDimensions ? "1" : "0") + "|");
            hash.Append((this.IncludeRecords ? "1" : "0") + "|");
            hash.Append((this.IncludeEmptyDimensions ? "1" : "0") + "|");
            hash.Append((this.ExcludeCount ? "1" : "0") + "|");

            if (this.UserList != null)
            {
                hash.Append(string.Join("-", this.UserList.OrderBy(x => x)) + "|");
            }

            if (this.SkipDimensions != null)
                hash.Append(string.Join("-", this.SkipDimensions.OrderBy(x => x).Select(x => x.ToString())));
            hash.Append("|");

            if (this.DerivedFieldList != null)
                this.DerivedFieldList.OrderBy(x => x.Field).ToList().ForEach(x => hash.Append(x.Field + "|" + x.Action + "|"));

            return EncryptionDomain.Hash(hash.ToString());
        }

        /// <summary />
        public virtual int CoreWhereHashCode()
        {
            var hash = new StringBuilder();
            if (this.DimensionValueList == null || this.DimensionValueList.Count == 0)
                hash.Append("NULL|");
            else
                hash.Append(string.Join("-", this.DimensionValueList.OrderBy(x => x).ToArray()) + "|");

            hash.Append(this.Keyword + "|");

            if (this.FieldFilters == null || this.FieldFilters.Count == 0)
                hash.Append("NULL|");
            else
            {
                foreach (var o in this.FieldFilters.OrderBy(x => x.Name))
                    hash.Append(o.GetHashCode() + "~|");
            }

            if (this.UserList != null)
            {
                hash.Append(string.Join("-", this.UserList.OrderBy(x => x)) + "|");
            }

            return EncryptionDomain.Hash(hash.ToString());
        }

        /// <summary />
        public override string ToString()
        {
            var retval = new StringBuilder();

            #region Dimensions

            if (this.DimensionValueList != null && this.DimensionValueList.Count > 0)
            {
                retval.Append("d=" + string.Join("+", this.DimensionValueList.Select(x => x.ToString())));
            }

            #endregion

            #region Field Filters

            if (this.FieldFilters != null && this.FieldFilters.Count > 0)
            {
                retval.Append("&ff=" + string.Join("|", this.FieldFilters.Select(x => x.ToString())));
            }

            #endregion

            #region Field Sorts

            if (this.FieldSorts != null && this.FieldSorts.Count > 0)
            {
                retval.Append("&fs=" + string.Join("|", this.FieldSorts.Select(x => x.ToString())));
            }

            #endregion

            #region Field Selects

            if (this.FieldSelects != null && this.FieldSelects.Count > 0)
            {
                retval.Append("&fsel=" + string.Join("|", this.FieldSelects.Select(x => x.ToString())));
            }

            #endregion

            #region GroupBy

            if (this.GroupFields != null && this.GroupFields.Count > 0)
            {
                retval.Append("&gf=" + string.Join("|", this.GroupFields.Select(x => x.ToString())));
            }

            #endregion

            #region Keyword

            if (!string.IsNullOrEmpty(this.Keyword))
            {
                //This is crazy but it uses "+" not % syntax like all other chars
                //This is for consistency
                retval.Append("&srch=" + HttpUtility.UrlEncode(this.Keyword).Replace("+", "%20"));
            }

            #endregion

            #region NonParsedFieldList
            if (this.NonParsedFieldList != null)
            {
                foreach (var item in this.NonParsedFieldList)
                {
                    if (!string.IsNullOrEmpty(item.Value))
                        retval.Append("&" + item.Key + "=" + item.Value);
                }
            }
            #endregion

            #region SkipDimensions
            if (this.SkipDimensions != null && this.SkipDimensions.Any())
            {
                retval.Append("&skipd=" + string.Join(",", this.SkipDimensions));
            }
            #endregion

            if (this.PageOffset != 1)
                retval.Append("&po=" + this.PageOffset);
            if (this.RecordsPerPage != 10)
                retval.Append("&rpp=" + this.RecordsPerPage);
            if (this.IncludeEmptyDimensions)
                retval.Append("&_ied=1");
            if (!this.IncludeRecords)
                retval.Append("&_ir=0");
            if (!this.IncludeDimensions)
                retval.Append("&_id=0");
            if (this.ExcludeCount)
                retval.Append("&_ec=1");

            if (this.UserList != null)
            {
                var lsv = string.Join(",", this.UserList.Select(x => x.ToString()));
                if (!string.IsNullOrEmpty(lsv))
                    retval.Append("&ul=" + lsv);
            }

            var r = retval.ToString();
            r = r.Trim('&');
            if (this.PageName == null)
            {
                //Do Nothing
            }
            else if (this.PageName == "?")
            {
                r = (string.IsNullOrEmpty(r) ? string.Empty : r);
            }
            else
            {
                r = this.PageName + "?" + r;
            }
            r = r.Replace("??", "?");

            //Do not let URL end with "?"
            if (r.EndsWith("?"))
                r = r.Substring(0, r.Length - 1);
            return r;
        }

        /// <summary />
        public IEnumerable<Tuple<string, string>> GetParameters()
        {
            try
            {
                var retval = new List<Tuple<string, string>>();
                var url = this.ToString();
                var index = url.IndexOf("?");
                if (index != -1)
                    url = url.Substring(index + 1, url.Length - index - 1);

                var arr = url.Split(new char[] { '&' });
                foreach (var s in arr)
                {
                    index = s.IndexOf("=");
                    if (index != -1)
                        retval.Add(new Tuple<string, string>(s.Substring(0, index), s.Substring(index + 1, s.Length - index - 1)));
                }

                var uri = new Uri(url, UriKind.RelativeOrAbsolute);
                return retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public void ResetPaging()
        {
            this.PageOffset = 1;
            this.RecordsPerPage = 10;
        }

    }
}
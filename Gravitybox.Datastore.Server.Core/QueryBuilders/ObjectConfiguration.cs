using Gravitybox.Datastore.Common;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core.QueryBuilders
{
    internal class ObjectConfiguration
    {
        public enum SelectionMode
        {
            Normal,
            Custom,
            Grouping,
        }

        public DimensionItem newDimension = null;
        public List<DimensionItem> dimensionList = null;
        public RepositorySchema schema = null;
        public List<string> orderByColumns = null;
        public List<FieldDefinition> orderByFields = null;
        public List<DimensionDefinition> nonListDimensionDefs = null;
        public string dimensionValueTableParent = null;
        public string dimensionTableParent = null;
        public string dimensionValueTable = null;
        public RepositorySchema parentSchema = null;
        public List<SqlParameter> parameters = null;
        public DataQuery query = null;
        public bool hasFilteredListDims = false;
        public string dataTable = null;
        public string innerJoinClause = null;
        public string whereClause = null;
        public string orderByClause = null;
        public List<FieldDefinition> normalFields = null;
        public DataQueryResults retval = null;
        public SelectionMode usingCustomSelect = SelectionMode.Normal;
        public int extraRecords = 0;
        public int repositoryId = 0;
        public bool UseGroupingSets = false;
        public int dimensionGroups = 0;
        public bool isGeo = false;

        public bool PerfLoadRecords { get; set; }
        public bool PerfLoadCount { get; set; }
        public bool PerfLoadNDim { get; set; }
        public int PerfLoadLDim { get; set; }
        public bool PerfLoadAgg { get; set; }

        /// <summary>
        /// Determine if this is a GroupBy query
        /// </summary>
        public bool IsGrouped
        {
            get { return (this.query.DerivedFieldList?.Count > 0 && this.query.GroupFields?.Count > 0); }
        }

        /// <summary>
        /// A comment to inject into SQL text to force a unique query plan compile
        /// </summary>
        public string QueryPlanDebug
        {
            //For now we will base the query plan separation based on the schema clustering key
            //If there is a field filter that that field then inject it into the SQL text and a new query plan will be made
            //In multi-thread environment it will calculate multiple times but it does not matter. It runs in microseconds!
            get
            {
                try
                {
                    var retval = string.Empty;

                    //If there is a grouping field use it to generate QP
                    var f = this.schema?.FieldList?.FirstOrDefault(x => x.IsDataGrouping);
                    if (f != null)
                    {
                        var v = this.query?.FieldFilters?.FirstOrDefault(x => x.Name == f.Name);
                        if (v != null)
                        {
                            retval += $", {f.Name}={v.Value}";
                        }
                    }

                    //If there is a user then create QP for each one
                    if (this.query?.UserList != null && this.query.UserList.Any())
                        retval += ", uid=" + this.query.UserList?.FirstOrDefault().ToString();

                    //Also add the time so it recycles QP every 10 minutes
                    var min = DateTime.Now.Minute / 10;
                    retval += $", t={DateTime.Now.ToString("ddHH")}.{min}";

                    return retval;
                }
                catch (Exception ex)
                {
                    return string.Empty;
                }
            }
        }

    }
}
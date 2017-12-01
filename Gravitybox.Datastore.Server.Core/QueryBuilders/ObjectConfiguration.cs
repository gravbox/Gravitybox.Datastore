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
        public string dimensionTable = null;
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
    }
}

//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

#pragma warning disable 612
using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Xml.Serialization;
using System.ComponentModel;
using System.Collections.Generic;
using System.Text;
using Gravitybox.Datastore.EFDAL;
using Gravitybox.Datastore.EFDAL.EventArguments;
using System.Text.RegularExpressions;
using System.Linq.Expressions;
using System.Data.Entity;
using System.Data.Linq;
using System.Data.Entity.ModelConfiguration;
using System.ComponentModel.DataAnnotations;

namespace Gravitybox.Datastore.EFDAL.Entity
{
	/// <summary>
	/// The 'DeleteQueue' entity
	/// </summary>
	[DataContract]
	[Serializable]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	[FieldNameConstants(typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))]
	[System.ComponentModel.DataAnnotations.MetadataType(typeof(Gravitybox.Datastore.EFDAL.Entity.Metadata.DeleteQueueMetadata))]
	[EntityMetadata("DeleteQueue", false, false, false, false, "", true, false, false, "dbo")]
	public partial class DeleteQueue : BaseEntity, Gravitybox.Datastore.EFDAL.IBusinessObject, System.ICloneable, Gravitybox.Datastore.EFDAL.ICreatable
	{
		#region FieldNameConstants Enumeration

		/// <summary>
		/// Enumeration to define each property that maps to a database field for the 'DeleteQueue' table.
		/// </summary>
		public enum FieldNameConstants
		{
			/// <summary>
			/// Field mapping for the 'IsReady' property
			/// </summary>
			[System.ComponentModel.Description("Field mapping for the 'IsReady' property")]
			IsReady,
			/// <summary>
			/// Field mapping for the 'RepositoryId' property
			/// </summary>
			[System.ComponentModel.Description("Field mapping for the 'RepositoryId' property")]
			RepositoryId,
			/// <summary>
			/// Field mapping for the 'RowId' property
			/// </summary>
			[System.ComponentModel.DataAnnotations.Key]
			[System.ComponentModel.ReadOnly(true)]
			[System.ComponentModel.Description("Field mapping for the 'RowId' property")]
			RowId,
		}
		#endregion

		#region Constructors

		/// <summary>
		/// Initializes a new instance of the Gravitybox.Datastore.EFDAL.Entity.DeleteQueue class
		/// </summary>
		public DeleteQueue()
		{
			this._isReady = false;

		}

		#endregion

		#region Properties

		/// <summary>
		/// Determine if this item is ready to be processed
		/// </summary>
		/// <remarks>Field: [DeleteQueue].[IsReady], Not Nullable, Default Value: false</remarks>
		[DataMember]
		[System.ComponentModel.Browsable(true)]
		[System.ComponentModel.DisplayName("IsReady")]
		[System.ComponentModel.Description("Determine if this item is ready to be processed")]
		[System.Diagnostics.DebuggerNonUserCode()]
		public virtual bool IsReady
		{
			get { return _isReady; }
			set
			{
				_isReady = value;
			}
		}

		/// <summary>
		/// The property that maps back to the database 'DeleteQueue.RepositoryId' field.
		/// </summary>
		/// <remarks>Field: [DeleteQueue].[RepositoryId], Not Nullable</remarks>
		[DataMember]
		[System.ComponentModel.Browsable(true)]
		[System.ComponentModel.DisplayName("RepositoryId")]
		[System.Diagnostics.DebuggerNonUserCode()]
		public virtual int RepositoryId
		{
			get { return _repositoryId; }
			set
			{
				_repositoryId = value;
			}
		}

		/// <summary>
		/// The property that maps back to the database 'DeleteQueue.RowId' field.
		/// </summary>
		/// <remarks>Field: [DeleteQueue].[RowId], Not Nullable, Primary Key, AutoNumber, Unique, Indexed</remarks>
		[DataMember]
		[System.ComponentModel.Browsable(true)]
		[System.ComponentModel.DisplayName("RowId")]
		[System.ComponentModel.DataAnnotations.Schema.Index(IsUnique = true)]
		[System.ComponentModel.DataAnnotations.Key()]
		[System.Diagnostics.DebuggerNonUserCode()]
		public virtual long RowId
		{
			get { return _rowId; }
			protected internal set
			{
				_rowId = value;
			}
		}

		#endregion

		#region Property Holders

		/// <summary />
		protected bool _isReady;
		/// <summary />
		protected int _repositoryId;
		/// <summary />
		protected long _rowId;

		#endregion

		#region GetMaxLength

		/// <summary>
		/// Gets the maximum size of the field value.
		/// </summary>
		public static int GetMaxLength(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field)
		{
			switch (field)
			{
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.IsReady:
					return 0;
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RepositoryId:
					return 0;
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RowId:
					return 0;
			}
			return 0;
		}

		int Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.GetMaxLength(Enum field)
		{
			return GetMaxLength((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field);
		}

		#endregion

		#region GetFieldNameConstants

		System.Type Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.GetFieldNameConstants()
		{
			return typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants);
		}

		#endregion

		#region GetFieldType

		/// <summary>
		/// Gets the system type of a field on this object
		/// </summary>
		public static System.Type GetFieldType(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field)
		{
			if (field.GetType() != typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))
				throw new Exception("The field parameter must be of type 'Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants'.");

			switch ((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field)
			{
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.IsReady: return typeof(bool);
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RepositoryId: return typeof(int);
				case Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RowId: return typeof(long);
			}
			return null;
		}

		System.Type Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.GetFieldType(Enum field)
		{
			if (field.GetType() != typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))
				throw new Exception("The field parameter must be of type 'Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants'.");

			return GetFieldType((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field);
		}

		#endregion

		#region Get/Set Value

		object Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.GetValue(System.Enum field)
		{
			return ((Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject)this).GetValue(field, null);
		}

		object Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.GetValue(System.Enum field, object defaultValue)
		{
			if (field.GetType() != typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))
				throw new Exception("The field parameter must be of type 'Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants'.");
			return this.GetValue((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field, defaultValue);
		}

		void Gravitybox.Datastore.EFDAL.IBusinessObject.SetValue(System.Enum field, object newValue)
		{
			if (field.GetType() != typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))
				throw new Exception("The field parameter must be of type 'Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants'.");
			this.SetValue((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field, newValue);
		}

		void Gravitybox.Datastore.EFDAL.IBusinessObject.SetValue(System.Enum field, object newValue, bool fixLength)
		{
			if (field.GetType() != typeof(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants))
				throw new Exception("The field parameter must be of type 'Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants'.");
			this.SetValue((Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants)field, newValue, fixLength);
		}

		#endregion

		#region PrimaryKey

		/// <summary>
		/// Generic primary key for this object
		/// </summary>
		Gravitybox.Datastore.EFDAL.IPrimaryKey Gravitybox.Datastore.EFDAL.IReadOnlyBusinessObject.PrimaryKey
		{
			get { return new PrimaryKey(Util.HashPK("DeleteQueue", this.RowId)); }
		}

		#endregion

		#region Clone

		/// <summary>
		/// Creates a shallow copy of this object
		/// </summary>
		public virtual object Clone()
		{
			return Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.Clone(this);
		}

		/// <summary>
		/// Creates a shallow copy of this object with defined, default values and new PK
		/// </summary>
		public virtual object CloneAsNew()
		{
			var item = Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.Clone(this);
			item._rowId = 0;
			item._isReady = false;
			return item;
		}

		/// <summary>
		/// Creates a shallow copy of this object
		/// </summary>
		public static DeleteQueue Clone(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue item)
		{
			var newItem = new DeleteQueue();
			newItem.IsReady = item.IsReady;
			newItem.RepositoryId = item.RepositoryId;
			newItem.RowId = item.RowId;
			return newItem;
		}

		#endregion

		#region GetValue

		/// <summary>
		/// Gets the value of one of this object's properties.
		/// </summary>
		public virtual object GetValue(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field)
		{
			return GetValue(field, null);
		}

		/// <summary>
		/// Gets the value of one of this object's properties.
		/// </summary>
		public virtual object GetValue(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field, object defaultValue)
		{
			if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.IsReady)
				return this.IsReady;
			if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RepositoryId)
				return this.RepositoryId;
			if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RowId)
				return this.RowId;
			throw new Exception("Field '" + field.ToString() + "' not found!");
		}

		#endregion

		#region SetValue

		/// <summary>
		/// Assigns a value to a field on this object.
		/// </summary>
		/// <param name="field">The field to set</param>
		/// <param name="newValue">The new value to assign to the field</param>
		public virtual void SetValue(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field, object newValue)
		{
			SetValue(field, newValue, false);
		}

		/// <summary>
		/// Assigns a value to a field on this object.
		/// </summary>
		/// <param name="field">The field to set</param>
		/// <param name="newValue">The new value to assign to the field</param>
		/// <param name="fixLength">Determines if the length should be truncated if too long. When false, an error will be raised if data is too large to be assigned to the field.</param>
		public virtual void SetValue(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field, object newValue, bool fixLength)
		{
			if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.IsReady)
			{
				this.IsReady = GlobalValues.SetValueHelperBoolNotNullableInternal(newValue, "Field 'IsReady' does not allow null values!");
			}
			else if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RepositoryId)
			{
				this.RepositoryId = GlobalValues.SetValueHelperIntNotNullableInternal(newValue, "Field 'RepositoryId' does not allow null values!");
			}
			else if (field == Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants.RowId)
			{
				throw new Exception("Field '" + field.ToString() + "' is a primary key and cannot be set!");
			}
			else
				throw new Exception("Field '" + field.ToString() + "' not found!");
		}

		#endregion

		#region Navigation Properties

		/// <summary>
		/// The navigation definition for walking DeleteQueue->DeleteQueueItem
		/// </summary>
		[DataMember]
		[XmlIgnore]
		public virtual ICollection<Gravitybox.Datastore.EFDAL.Entity.DeleteQueueItem> DeleteQueueItemList
		{
			get
			{
				if (_DeleteQueueItemList == null) _DeleteQueueItemList = new List<Gravitybox.Datastore.EFDAL.Entity.DeleteQueueItem>();
				return _DeleteQueueItemList;
			}
			set { _DeleteQueueItemList = value; }
		}
		/// <summary />
		protected virtual ICollection<Gravitybox.Datastore.EFDAL.Entity.DeleteQueueItem> _DeleteQueueItemList { get; set; }

		#endregion

		#region Static SQL Methods

		internal static string GetFieldAliasFromFieldNameSqlMapping(string alias)
		{
			alias = alias.Replace("[", string.Empty).Replace("]", string.Empty);
			switch (alias.ToLower())
			{
				case "isready": return "isready";
				case "repositoryid": return "repositoryid";
				case "rowid": return "rowid";
				default: throw new Exception("The select clause is not valid.");
			}
		}

		internal static string GetTableFromFieldAliasSqlMapping(string alias)
		{
			switch (alias.ToLower())
			{
				case "isready": return "DeleteQueue";
				case "repositoryid": return "DeleteQueue";
				case "rowid": return "DeleteQueue";
				default: throw new Exception("The select clause is not valid.");
			}
		}

		internal static string GetTableFromFieldNameSqlMapping(string field)
		{
			switch (field.ToLower())
			{
				case "isready": return "DeleteQueue";
				case "repositoryid": return "DeleteQueue";
				case "rowid": return "DeleteQueue";
				default: throw new Exception("The select clause is not valid.");
			}
		}

		internal static string GetRemappedLinqSql(string sql, string parentAlias, LinqSQLFromClauseCollection childTables)
		{
			sql = System.Text.RegularExpressions.Regex.Replace(sql, "\\[" + parentAlias + "\\]\\.\\[isready\\]", "[" + childTables.GetBaseAliasTable(parentAlias, "DeleteQueue") + "].[isready]", RegexOptions.IgnoreCase);
			sql = System.Text.RegularExpressions.Regex.Replace(sql, "\\[" + parentAlias + "\\]\\.\\[repositoryid\\]", "[" + childTables.GetBaseAliasTable(parentAlias, "DeleteQueue") + "].[repositoryid]", RegexOptions.IgnoreCase);
			sql = System.Text.RegularExpressions.Regex.Replace(sql, "\\[" + parentAlias + "\\]\\.\\[rowid\\]", "[" + childTables.GetBaseAliasTable(parentAlias, "DeleteQueue") + "].[rowid]", RegexOptions.IgnoreCase);
			return sql;
		}

		#endregion

		#region DeleteData

		/// <summary>
		/// Delete all records that match a where condition
		/// </summary>
		/// <param name="where">The expression that determines the records deleted</param>
		/// <returns>The number of rows deleted</returns>
		public static int DeleteData(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where)
		{
			return DeleteData(where: where, optimizer: new QueryOptimizer(), startup: new ContextStartup(null), connectionString: Gravitybox.Datastore.EFDAL.DatastoreEntities.GetConnectionString());
		}

		/// <summary>
		/// Delete all records that match a where condition
		/// </summary>
		/// <param name="where">The expression that determines the records deleted</param>
		/// <param name="optimizer">The optimization object to use for running queries</param>
		/// <returns>The number of rows deleted</returns>
		public static int DeleteData(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, QueryOptimizer optimizer)
		{
			return DeleteData(where: where, optimizer: optimizer, startup: new ContextStartup(null), connectionString: Gravitybox.Datastore.EFDAL.DatastoreEntities.GetConnectionString());
		}

		/// <summary>
		/// Delete all records that match a where condition
		/// </summary>
		/// <param name="where">The expression that determines the records deleted</param>
		/// <param name="connectionString">The database connection string to use for this access</param>
		/// <returns>The number of rows deleted</returns>
		public static int DeleteData(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, string connectionString)
		{
			return DeleteData(where: where, optimizer: new QueryOptimizer(), startup: new ContextStartup(null), connectionString: connectionString);
		}

		/// <summary>
		/// Delete all records that match a where condition
		/// </summary>
		/// <param name="where">The expression that determines the records deleted</param>
		/// <param name="optimizer">The optimization object to use for running queries</param>
		/// <param name="startup">The startup options</param>
		/// <param name="connectionString">The database connection string to use for this access</param>
		/// <returns>The number of rows deleted</returns>
		public static int DeleteData(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, QueryOptimizer optimizer, ContextStartup startup, string connectionString)
		{
			if (optimizer == null)
				optimizer = new QueryOptimizer();
				if (startup == null) startup = new ContextStartup(null);

			using (var connection = Gravitybox.Datastore.EFDAL.DBHelper.GetConnection(Gravitybox.Datastore.EFDAL.Util.StripEFCS2Normal(connectionString)))
			{
				using (var dc = new DataContext(connection))
				{
					var template = dc.GetTable<Gravitybox.Datastore.EFDAL.DeleteQueueQuery>();
					using (var cmd = BusinessEntityQuery.GetCommand<Gravitybox.Datastore.EFDAL.DeleteQueueQuery>(dc, template, where))
					{
						if (!startup.DefaultTimeout && startup.CommandTimeout > 0) cmd.CommandTimeout = startup.CommandTimeout;
						else
						{
							var cb = new System.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
							cmd.CommandTimeout = cb.ConnectTimeout;
						}

						var parser = LinqSQLParser.Create(cmd.CommandText, LinqSQLParser.ObjectTypeConstants.Table);
                        var sb = new StringBuilder();
                        sb.AppendLine("SET ROWCOUNT " + optimizer.ChunkSize + ";");
                        sb.AppendLine("delete [X] from [dbo].[DeleteQueue] [X] inner join (");
                        sb.AppendLine("SELECT [t0].[RowId]");
                        sb.AppendLine(parser.GetFromClause(optimizer));
                        sb.AppendLine(parser.GetWhereClause());
                        sb.AppendLine(") AS [Extent2]");
                        sb.AppendLine("ON [X].[RowId] = [Extent2].[RowId]");
                        sb.AppendLine("select @@ROWCOUNT");
						cmd.CommandText = sb.ToString();
						dc.Connection.Open();
						var startTime = DateTime.Now;
						var affected = 0;
						var count = 0;
						do
						{
							count = (int)cmd.ExecuteScalar();
							affected += count;
						} while (count > 0 && optimizer.ChunkSize > 0);
						var endTime = DateTime.Now;
						optimizer.TotalMilliseconds = (long)endTime.Subtract(startTime).TotalMilliseconds;
						dc.Connection.Close();
						return affected;
					}
				}
			}
		}

		#endregion

		#region UpdateData

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, TSource newValue)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false);
		}

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <param name="connection">An open database connection</param>
		/// <param name="transaction">The database connection transaction</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, TSource newValue, System.Data.IDbConnection connection, System.Data.Common.DbTransaction transaction)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false, startup: null, connection: connection, transaction: transaction);
		}

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <param name="startup">A configuration object</param>
		/// <param name="connectionString">The database connection string</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, TSource newValue, ContextStartup startup, string connectionString)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false, startup: startup, connectionString: connectionString);
		}

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <param name="connectionString">The database connection string</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, TSource newValue, string connectionString)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false, connectionString: connectionString);
		}

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, Gravitybox.Datastore.EFDAL.Entity.DeleteQueue newValue)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false);
		}

		/// <summary>
		/// Update the specified field that matches the Where expression with the new data value
		/// </summary>
		/// <param name="select">The field to update</param>
		/// <param name="where">The expression that determines the records selected</param>
		/// <param name="newValue">The new value to set the specified field in all matching records</param>
		/// <param name="connectionString">The database connection string</param>
		/// <returns>The number of records affected</returns>
		public static int UpdateData<TSource>(Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>> select, Expression<Func<Gravitybox.Datastore.EFDAL.DeleteQueueQuery, bool>> where, Gravitybox.Datastore.EFDAL.Entity.DeleteQueue newValue, string connectionString)
		{
			return BusinessObjectQuery<Gravitybox.Datastore.EFDAL.Entity.DeleteQueue, Gravitybox.Datastore.EFDAL.DeleteQueueQuery, TSource>.UpdateData(select: select, where: where, newValue: newValue, leafTable: "DeleteQueue", getField: GetDatabaseFieldName, hasModifyAudit: false, connectionString: connectionString);
		}

		#endregion

		#region GetDatabaseFieldName

		/// <summary>
		/// Returns the actual database name of the specified field.
		/// </summary>
		internal static string GetDatabaseFieldName(Gravitybox.Datastore.EFDAL.Entity.DeleteQueue.FieldNameConstants field)
		{
			return GetDatabaseFieldName(field.ToString());
		}

		/// <summary>
		/// Returns the actual database name of the specified field.
		/// </summary>
		internal static string GetDatabaseFieldName(string field)
		{
			switch (field)
			{
				case "IsReady": return "IsReady";
				case "RepositoryId": return "RepositoryId";
				case "RowId": return "RowId";
			}
			return string.Empty;
		}

		#endregion

		#region Equals
		/// <summary>
		/// Compares two objects of 'DeleteQueue' type and determines if all properties match
		/// </summary>
		/// <returns>True if all properties match, false otherwise</returns>
		public override bool Equals(object obj)
		{
			var other = obj as Gravitybox.Datastore.EFDAL.Entity.DeleteQueue;
			if (other == null) return false;
			return (
				other.IsReady == this.IsReady &&
				other.RepositoryId == this.RepositoryId &&
				other.RowId == this.RowId
				);
		}

		/// <summary>
		/// Serves as a hash function for this type.
		/// </summary>
		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		#endregion

	}
}

#region Metadata Class

namespace Gravitybox.Datastore.EFDAL.Entity.Metadata
{
	/// <summary>
	/// Metadata class for the 'DeleteQueue' entity
	/// </summary>
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class DeleteQueueMetadata : Gravitybox.Datastore.EFDAL.IMetadata
	{
		#region Properties

		/// <summary>
		/// Metadata information for the 'IsReady' parameter
		/// </summary>
		[System.ComponentModel.DataAnnotations.Required(ErrorMessage = "'IsReady' is required.", AllowEmptyStrings = true)]
		[System.ComponentModel.DataAnnotations.Display(Description = "Determine if this item is ready to be processed", Name = "IsReady", AutoGenerateField = true)]
		public object IsReady;

		/// <summary>
		/// Metadata information for the 'RepositoryId' parameter
		/// </summary>
		[System.ComponentModel.DataAnnotations.Required(ErrorMessage = "'RepositoryId' is required.", AllowEmptyStrings = true)]
		[System.ComponentModel.DataAnnotations.Display(Description = "", Name = "RepositoryId", AutoGenerateField = true)]
		public object RepositoryId;

		/// <summary>
		/// Metadata information for the 'RowId' parameter
		/// </summary>
		[System.ComponentModel.DataAnnotations.Required(ErrorMessage = "'RowId' is required.", AllowEmptyStrings = true)]
		[System.ComponentModel.DataAnnotations.Key()]
		[System.ComponentModel.ReadOnly(true)]
		[System.ComponentModel.DataAnnotations.Display(Description = "", Name = "RowId", AutoGenerateField = true)]
		public object RowId;

		#endregion

		#region Methods
		/// <summary>
		/// Gets the underlying table name.
		/// </summary>
		public virtual string GetTableName()
		{
			return "DeleteQueue";
		}

		/// <summary>
		/// Gets a list of all object fields with alias/code facade applied excluding inheritance.
		/// </summary>
		public virtual List<string> GetFields()
		{
			var retval = new List<string>();
			retval.Add("IsReady");
			retval.Add("RepositoryId");
			retval.Add("RowId");
			return retval;
		}

		/// <summary>
		/// Returns the type of the parent object if one exists.
		/// </summary>
		public virtual System.Type InheritsFrom()
		{
			return null;
		}

		/// <summary>
		/// Returns the database schema name.
		/// </summary>
		public virtual string Schema()
		{
			return "dbo";
		}

		/// <summary>
		/// Returns the actual database name of the specified field.
		/// </summary>
		public virtual string GetDatabaseFieldName(string field)
		{
			switch (field)
			{
				case "IsReady": return "IsReady";
				case "RepositoryId": return "RepositoryId";
				case "RowId": return "RowId";
			}
			return string.Empty;
		}

		#endregion

	}

}

#endregion

#pragma warning restore 612

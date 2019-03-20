//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

using System;
using System.Linq;
using System.Data.Linq.Mapping;
using Gravitybox.Datastore.EFDAL;

namespace Gravitybox.Datastore.EFDAL
{
	#region AppliedPatchInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the AppliedPatch collection.
	/// </summary>
	[Serializable]
	[Table(Name = "AppliedPatch")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class AppliedPatchInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region CacheInvalidateInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the CacheInvalidate collection.
	/// </summary>
	[Serializable]
	[Table(Name = "CacheInvalidate")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class CacheInvalidateInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region ConfigurationSettingInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the ConfigurationSetting collection.
	/// </summary>
	[Serializable]
	[Table(Name = "ConfigurationSetting")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class ConfigurationSettingInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region DeleteQueueInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the DeleteQueue collection.
	/// </summary>
	[Serializable]
	[Table(Name = "DeleteQueue")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class DeleteQueueInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the DeleteQueueItem entity.
		/// </summary>
		[Association(ThisKey = "RowId", OtherKey = "ParentRowId")]
		public Gravitybox.Datastore.EFDAL.DeleteQueueItemInclude DeleteQueueItemList { get; private set; }

	}

	#endregion

	#region DeleteQueueItemInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the DeleteQueueItem collection.
	/// </summary>
	[Serializable]
	[Table(Name = "DeleteQueueItem")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class DeleteQueueItemInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the DeleteQueue entity.
		/// </summary>
		[Association(ThisKey = "ParentRowId", OtherKey = "RowId")]
		public Gravitybox.Datastore.EFDAL.DeleteQueueInclude DeleteQueue { get; private set; }

	}

	#endregion

	#region HousekeepingInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the Housekeeping collection.
	/// </summary>
	[Serializable]
	[Table(Name = "Housekeeping")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class HousekeepingInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region LockStatInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the LockStat collection.
	/// </summary>
	[Serializable]
	[Table(Name = "LockStat")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class LockStatInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region RepositoryInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the Repository collection.
	/// </summary>
	[Serializable]
	[Table(Name = "Repository")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class RepositoryInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region RepositoryActionTypeInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the RepositoryActionType collection.
	/// </summary>
	[Serializable]
	[Table(Name = "RepositoryActionType")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class RepositoryActionTypeInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the RepositoryStat entity.
		/// </summary>
		[Association(ThisKey = "RepositoryActionTypeId", OtherKey = "RepositoryActionTypeId")]
		public Gravitybox.Datastore.EFDAL.RepositoryStatInclude RepositoryStatList { get; private set; }

	}

	#endregion

	#region RepositoryLogInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the RepositoryLog collection.
	/// </summary>
	[Serializable]
	[Table(Name = "RepositoryLog")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class RepositoryLogInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

	#region RepositoryStatInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the RepositoryStat collection.
	/// </summary>
	[Serializable]
	[Table(Name = "RepositoryStat")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class RepositoryStatInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the RepositoryActionType entity.
		/// </summary>
		[Association(ThisKey = "RepositoryActionTypeId", OtherKey = "RepositoryActionTypeId")]
		public Gravitybox.Datastore.EFDAL.RepositoryActionTypeInclude RepositoryActionType { get; private set; }

	}

	#endregion

	#region ServerInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the Server collection.
	/// </summary>
	[Serializable]
	[Table(Name = "Server")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class ServerInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the ServerStat entity.
		/// </summary>
		[Association(ThisKey = "ServerId", OtherKey = "ServerId")]
		public Gravitybox.Datastore.EFDAL.ServerStatInclude ServerStatList { get; private set; }

	}

	#endregion

	#region ServerStatInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the ServerStat collection.
	/// </summary>
	[Serializable]
	[Table(Name = "ServerStat")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class ServerStatInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
		/// <summary>
		/// This is a mapping of the relationship with the Server entity.
		/// </summary>
		[Association(ThisKey = "ServerId", OtherKey = "ServerId")]
		public Gravitybox.Datastore.EFDAL.ServerInclude Server { get; private set; }

	}

	#endregion

	#region ServiceInstanceInclude

	/// <summary>
	/// This is a helper object for creating LINQ definitions for context includes on the ServiceInstance collection.
	/// </summary>
	[Serializable]
	[Table(Name = "ServiceInstance")]
	[System.CodeDom.Compiler.GeneratedCode("nHydrateModelGenerator", "6.0.0")]
	public partial class ServiceInstanceInclude : Gravitybox.Datastore.EFDAL.IContextInclude
	{
	}

	#endregion

}

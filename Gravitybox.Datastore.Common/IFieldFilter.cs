namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public enum ComparisonConstants
    {
        /// <summary />
        LessThan,
        /// <summary />
        GreaterThan,
        /// <summary />
        LessThanOrEq,
        /// <summary />
        GreaterThanOrEq,
        /// <summary />
        Equals,
        /// <summary />
        Like,
        /// <summary />
        NotEqual,
        /// <summary />
        ContainsAny,
        /// <summary />
        ContainsAll,
        /// <summary />
        ContainsNone,
    }

    /// <summary />
    public interface IFieldFilter
    {
        /// <summary />
        string Name { get; set; }
        /// <summary />
        ComparisonConstants Comparer { get; set; }
        /// <summary />
        object Value { get; set; }
        ///// <summary />
        //object Value2 { get; set; }
        /// <summary />
        RepositorySchema.DataTypeConstants DataType { get; set; }
        /// <summary />
        bool FromUrl(string url);
    }

}
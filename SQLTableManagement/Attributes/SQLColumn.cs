using System;

namespace SQLTableManagement.Attributes
{
    /// <summary>
    /// Attribute to map a property to a sql column
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
    public class SQLColumn : Attribute
    {
        /// <summary>
        /// Represents the name of a SQL column
        /// </summary>
        public string ColumnName { get; private set; }
        /// <summary>
        /// Represents if the column is a key field
        /// </summary>
        public bool PrimaryKey { get; set; } = false;

        /// <summary>
        /// Attribute to define a SQL mapping
        /// </summary>
        /// <param name="columnName">Name of mapped column in SQL</param>
        public SQLColumn(string columnName)
        {
            ColumnName = columnName;
        }
    }
}

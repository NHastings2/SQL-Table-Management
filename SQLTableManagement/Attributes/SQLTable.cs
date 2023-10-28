using System;

namespace SQLTableManagement.Attributes
{
    /// <summary>
    /// Attribute to map a class to a SQL table
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public class SQLTable : Attribute
    {
        /// <summary>
        /// Represents the name of a SQL table
        /// </summary>
        public string TableName { get; private set; }
        /// <summary>
        /// Represents the schema of a SQL table
        /// </summary>
        public string Schema { get; set; } = "dbo";

        /// <summary>
        /// Create new instance of SQL table mapping attribute
        /// </summary>
        /// <param name="tableName">Name of mapped SQL table</param>
        public SQLTable(string tableName)
        {
            TableName = tableName;
        }
    }
}

using SQLTableManagement.Attributes;
using SQLTableManagement.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Reflection;
using SQLTableManagement.Helpers;
using System.Net;

namespace SQLTableManagement
{
    public class TableManager : IDisposable
    {
        /// <summary>
        /// Object to represent SQL connection
        /// </summary>
        private SqlConnection SQLConnection { get; set; }

        /// <summary>
        /// Create new SQL Manager
        /// </summary>
        /// <param name="server">Hostname/IP of SQL Server</param>
        /// <param name="database">Database to Connect To</param>
        /// <param name="username">Username to Connect With</param>
        /// <param name="password">Password for provided User</param>
        public TableManager(string server, string database, string username, string password)
        {
            SQLConnection = new SqlConnection($"Server={server};Database={database};User Id={username};Password={password};MultipleActiveResultSets=True");
            SQLConnection.Open();
        }

        public TableManager(string server, string database, NetworkCredential credential)
        {
            SQLConnection = new SqlConnection($"Server={server};Database={database};User Id={credential.UserName};Password={credential.Password};MultipleActiveResultSets=True");
            SQLConnection.Open();
        }

        public TableManager(string connString)
        {
            SQLConnection = new SqlConnection($"{connString};MultipleActiveResultSets=True");
            SQLConnection.Open();
        }

        /// <summary>
        /// Dispose of manager instance
        /// </summary>
        public virtual void Dispose()
        {
            //Check if SQL connection is open
            if (SQLConnection.State == ConnectionState.Open)
                //Close connection if its open
                SQLConnection.Close();

            //Dispose of connection object
            SQLConnection.Dispose();
        }

        /// <summary>
        /// Get Entities From Table Based On Query
        /// </summary>
        /// <typeparam name="T">Type of entity to return</typeparam>
        /// <param name="searchQuery">Query to search table</param>
        /// <returns>List of found entities</returns>
        /// <exception cref="Exception">Exception relating to custom attributes</exception>
        public List<T> Query<T>(string searchQuery = null, bool fullLoad = true)
        {
            //Get table name from entity
            SQLTable attribute = typeof(T).GetCustomAttribute<SQLTable>() ?? throw new Exception("Entity Missing SQLTable Attribute!");

            //Create DataTable for output records
            DataTable outputTable = new DataTable(attribute.TableName);
            //Create list to store column names
            List<string> columnNames = new List<string>();

            //Iterate over each property with SQL column mapping
            foreach (MemberInfo columnProperty in typeof(T).GetMembers(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).Where(x => x.GetCustomAttribute<SQLColumn>() != null))
            {
                //Get the column name from property custom attribute
                string columnName = columnProperty.GetCustomAttribute<SQLColumn>().ColumnName;

                //Add column to output table
                outputTable.Columns.Add(columnName, columnProperty.GetValueType());
                //Add column to DataTable
                columnNames.Add(columnName);
            }

            //Create string with base SQL query
            string sqlQuery = $"SELECT [{string.Join("], [", columnNames.ToArray())}] FROM [{attribute.Schema}].[{attribute.TableName}]";
            //Check if additional query is provided, if so, append to base query
            if (searchQuery != null)
                sqlQuery += $" WHERE ({searchQuery})";

            //Create new SQL Command from base query
            using (SqlCommand selectTable = new SqlCommand(sqlQuery, SQLConnection))
            //Setup data adapter from SQL Command
            using (SqlDataAdapter tableAdapter = new SqlDataAdapter(selectTable))
                //Read resulting query into DataTable
                tableAdapter.Fill(outputTable);

            //Create list for output items
            List<T> result = new List<T>();
            //Iterate over each row in query result
            foreach (DataRow row in outputTable.Rows)
            {
                //Get type of entity
                Type outputFormat = typeof(T);

                //Create new constructor info to create new object
                ConstructorInfo outputConstructor = outputFormat.GetConstructor(new Type[] { }) ?? throw new Exception("No Default Constructor Found!");

                //Invoke and create new instance of object
                T instance = (T)outputConstructor.Invoke(new object[] { });

                //Get all fields in entity with SQL mapping
                List<MemberInfo> fields = outputFormat.GetMembers(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).Where(x => x.GetCustomAttribute<SQLColumn>() != null).ToList();
                //Iterate over each column in DataTable
                foreach (DataColumn column in outputTable.Columns)
                {
                    //Get associated property to map data to
                    MemberInfo property = fields.Find(x => x.GetCustomAttribute<SQLColumn>().ColumnName == column.ColumnName);
                    property.SetValue(instance, row[column]);
                }

                //Check if entity has a load interface attached
                if (typeof(T).GetInterface("ISQLTableLoad") != null && fullLoad)
                    //If so, call load interface to fully load object
                    ((ISQLTableLoad)instance).Load(this);

                //Add entity to result set
                result.Add(instance);
            }

            //Return results
            return result;
        }

        /// <summary>
        /// Check if an entity exists in table
        /// </summary>
        /// <typeparam name="T">Type of Entity</typeparam>
        /// <param name="entity">Entity to Check</param>
        /// <returns>True if entity exists</returns>
        /// <exception cref="NullReferenceException">Error if no keys are found</exception>
        public bool EntityExists<T>(T entity)
        {
            //Get type of entity
            Type entityType = typeof(T);
            //Get table name from entity custom attribute
            SQLTable attribute = entityType.GetCustomAttribute<SQLTable>() ?? throw new Exception("Entity Missing SQLTable Attribute!");

            //Get all properties in entity with SQLColumn attribute primary key
            List<MemberInfo> keyProperties = entityType.GetMembers(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).Where(x => x.GetCustomAttribute<SQLColumn>()?.PrimaryKey ?? false).ToList();
            //Create string to store where clause
            string whereQuery = "";
            //Iterate through each key property
            foreach (MemberInfo property in keyProperties)
            {
                //Get the column name from the SQLColumn attribute
                string columnName = property.GetCustomAttribute<SQLColumn>().ColumnName;
                //Get value of property
                object propertyValue = property.GetValue(entity) ?? throw new Exception("No Properties with SQLColumn Primary Found!");

                //Add column and value to query
                whereQuery += $"[{columnName}] = '{propertyValue}' AND ";
            }

            //Create and execute search for entity and return if record was found
            using (SqlDataReader existQuery = new SqlCommand($"SELECT 1 FROM [{attribute.Schema}].[{attribute.TableName}] WHERE {whereQuery.Substring(0, whereQuery.Length - 5)}", SQLConnection).ExecuteReader())
                return existQuery.HasRows;
        }

        /// <summary>
        /// Insert record into table
        /// </summary>
        /// <typeparam name="T">Type of data to be inserted</typeparam>
        /// <param name="entity">Entity to be inserted</param>
        public void Insert<T>(T entity, bool insertRecusrsive = true) { Insert(new List<T>() { entity }, insertRecusrsive); }
        /// <summary>
        /// Insert records into table
        /// </summary>
        /// <typeparam name="T">Type of data to be inserted</typeparam>
        /// <param name="entities">Entities to be inserted</param>
        public void Insert<T>(List<T> entities, bool insertRecusrsive = true)
        {
            //Convert entity list to DataTable
            DataTable entityTable = GetEntityTable(entities);
            //Upload DataTable to temp SQL table
            UploadToTemp(entityTable);

            //Insert records into base table
            _ = new SqlCommand($"INSERT INTO [{entityTable.Prefix}].[{entityTable.TableName}] SELECT * FROM [{entityTable.Prefix}].[##{entityTable.TableName}]; DROP TABLE [{entityTable.Prefix}].[##{entityTable.TableName}];", SQLConnection).ExecuteNonQuery();

            //Check if insert recursive is enabled and entity contains load feature
            if (insertRecusrsive && typeof(T).GetInterfaces().Contains(typeof(ISQLTableLoad)))
                //Execute save function for each entity
                entities.ForEach(x => ((ISQLTableLoad)x).Save(this));
        }

        /// <summary>
        /// Update record in table
        /// </summary>
        /// <typeparam name="T">Type of data to be updated</typeparam>
        /// <param name="entity">Entity to be updated</param>
        public void Update<T>(T entity, bool updateRecursive = true) { Update(new List<T>() { entity }, updateRecursive); }
        /// <summary>
        /// Update records in table
        /// </summary>
        /// <typeparam name="T">Type of data to be updated</typeparam>
        /// <param name="entities">Entities to be updated</param>
        public void Update<T>(List<T> entities, bool updateRecursive = true)
        {
            //Convert entity list to DataTable
            DataTable entityTable = GetEntityTable(entities);
            //Upload DataTable to temp SQL table
            UploadToTemp(entityTable);

            //Setup Set Statement
            string setStatement = string.Join(", ", entityTable.Columns.Cast<DataColumn>().Select(x => $"t1.[{x.ColumnName}] = t2.[{x.ColumnName}]").ToArray());
            //Setup Join Statement
            string joinStatement = string.Join(", ", entityTable.PrimaryKey.Select(x => $"t1.[{x.ColumnName}] = t2.[{x.ColumnName}]").ToArray());

            //Run Update Statement
            _ = new SqlCommand($"UPDATE t1 SET {setStatement} FROM [{entityTable.Prefix}].[{entityTable.TableName}] AS t1 INNER JOIN [{entityTable.Prefix}].[##{entityTable.TableName}] AS t2 ON {joinStatement}; DROP TABLE [{entityTable.Prefix}].[##{entityTable.TableName}]", SQLConnection).ExecuteNonQuery();

            //Check if insert recursive is enabled and entity contains load feature
            if (updateRecursive && typeof(T).GetInterfaces().Contains(typeof(ISQLTableLoad)))
                //Execute save function for each entity
                entities.ForEach(x => ((ISQLTableLoad)x).Save(this));
        }

        /// <summary>
        /// Upsert record into table
        /// </summary>
        /// <typeparam name="T">Type of data to be upserted</typeparam>
        /// <param name="entity">Entity to be upserted</param>
        public void Upsert<T>(T entity, bool upsertRecursive = true) { Upsert(new List<T>() { entity }, upsertRecursive); }
        /// <summary>
        /// Upsert records into table
        /// </summary>
        /// <typeparam name="T">Type of data to be upserted</typeparam>
        /// <param name="entities">Entities to be upserted</param>
        public void Upsert<T>(List<T> entities, bool upsertRecursive = true)
        {
            //Get a list of all new and update entities
            List<T> updateEntites = entities.FindAll(x => EntityExists(x));
            List<T> newEntities = entities.Except(updateEntites).ToList();

            //If there are new entities
            if (newEntities.Count != 0)
                //Insert new entities
                Insert(newEntities, upsertRecursive);

            //If there are update entities
            if (updateEntites.Count != 0)
                //Update existing entities
                Update(updateEntites, upsertRecursive);
        }

        /// <summary>
        /// Delete record from table
        /// </summary>
        /// <typeparam name="T">Type of data to be deleted</typeparam>
        /// <param name="entity">Entity to be deleted</param>
        public void Delete<T>(T entity, bool deleteRecursive = true) { Delete(new List<T>() { entity }, deleteRecursive); }
        /// <summary>
        /// Delete records from table
        /// </summary>
        /// <typeparam name="T">Type of data to be deleted</typeparam>
        /// <param name="entities">Entities to be deleted</param>
        public void Delete<T>(List<T> entities, bool deleteRecursive = true)
        {
            //Convert entity list to DataTable
            DataTable entityTable = GetEntityTable(entities);
            //Upload DataTable to temp SQL table
            UploadToTemp(entityTable);

            //Delete entities from by join
            string deleteKeys = string.Join(", ", entityTable.PrimaryKey.Select(x => $"t1.[{x.ColumnName}] = t2.[{x.ColumnName}]").ToArray());
            _ = new SqlCommand($"DELETE t1 FROM [{entityTable.Prefix}].[{entityTable.TableName}] AS t1 INNER JOIN [{entityTable.Prefix}].[##{entityTable.TableName}] AS t2 ON {deleteKeys}; DROP TABLE [{entityTable.Prefix}].[##{entityTable.TableName}];", SQLConnection).ExecuteNonQuery();

            //Check if insert recursive is enabled and entity contains load feature
            if (deleteRecursive && typeof(T).GetInterfaces().Contains(typeof(ISQLTableLoad)))
                //Execute save function for each entity
                entities.ForEach(x => ((ISQLTableLoad)x).Delete(this));
        }

        /// <summary>
        /// Convert List of Entities into DataTable
        /// </summary>
        /// <typeparam name="T">Entity Data Type</typeparam>
        /// <param name="entities">List of Entities to Convert</param>
        /// <returns>DataTable of entities</returns>
        /// <exception cref="NullReferenceException">Exception for misconfigured type</exception>
        private DataTable GetEntityTable<T>(List<T> entities)
        {
            //Get type of object
            Type entityType = typeof(T);

            //Get Custom Attribute from Object
            SQLTable attribute = entityType.GetCustomAttribute<SQLTable>() ?? throw new Exception("Entity Missing SQLTable Attribute!");
            //Create storage table
            DataTable outputTable = new DataTable(attribute.TableName)
            {
                Prefix = attribute.Schema
            };

            //Get all properties from the object that are SQL mapped
            MemberInfo[] properties = entityType.GetMembers(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).Where(x => x.GetCustomAttribute<SQLColumn>() != null).ToArray();
            //Iterate over each mapped column
            foreach (MemberInfo property in properties)
                //Add mapped column to output table
                outputTable.Columns.Add(property.GetCustomAttribute<SQLColumn>().ColumnName, property.GetValueType());

            //Find properties marked with primary key and set DataTable primary keys
            List<string> keys = properties.Where(x => x.GetCustomAttribute<SQLColumn>().PrimaryKey).Select(x => x.Name).ToList();
            outputTable.PrimaryKey = outputTable.Columns.Cast<DataColumn>().Where(x => keys.Find(y => y == x.ColumnName) != null).ToArray();

            //Check if Key Columns were found, if not, throw exception
            if (outputTable.PrimaryKey.Count() == 0)
                throw new Exception("No Properties with SQLColumn Primary Found!");

            //Iterate over each custom entity in the list
            foreach (T entity in entities)
            {
                //Create list to store object data
                List<object> rowData = new List<object>();

                //Iterate over each column in the table
                foreach (DataColumn column in outputTable.Columns)
                    //Add entity value to storage list
                    rowData.Add(properties
                        .Where(x => x.GetCustomAttribute<SQLColumn>().ColumnName == column.ColumnName)
                        .First()
                        .GetValue(entity));

                //Add entity data to DataTable
                outputTable.Rows.Add(rowData.ToArray());
            }

            //Return completed DataTable
            return outputTable;
        }

        /// <summary>
        /// Upload DataTable to Temp SQL Table
        /// </summary>
        /// <param name="table">Table to be uploaded</param>
        private void UploadToTemp(DataTable table)
        {
            //Create Temp Table from Destination Table
            _ = new SqlCommand($"SELECT TOP 0 * INTO [{table.Prefix}].[##{table.TableName}] FROM [{table.Prefix}].[{table.TableName}]", SQLConnection).ExecuteNonQuery();

            //Create new Bulk Copy Operation
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(SQLConnection))
            {
                //Set Destination Table Name
                bulkCopy.DestinationTableName = $"[{table.Prefix}].[##{table.TableName}]";

                //Map DataTable to temp table
                foreach (DataColumn column in table.Columns)
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);

                //Write data to temp table
                bulkCopy.WriteToServer(table);
            }
        }
    }
}

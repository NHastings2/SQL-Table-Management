namespace SQLTableManagement.Interfaces
{
    public interface ISQLTableLoad
    {
        /// <summary>
        /// Save addition entities in class
        /// </summary>
        void Save(TableManager manager);

        /// <summary>
        /// Load Sub-Entities in class
        /// </summary>
        void Load(TableManager manager);

        /// <summary>
        /// Delete Sub-Entities in class
        /// </summary>
        void Delete(TableManager manager);
    }
}

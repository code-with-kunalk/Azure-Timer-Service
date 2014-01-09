using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.Reflection;
using System.Data.Services.Client;
using System.Configuration;

namespace AzureTimerService.Helper
{
    public class TableStorageHelper<T> where T : TableServiceEntity
    {
        private CloudTableClient _tableClient;

        private string _tableName;

        /// <summary>
        /// Table name to which helper services are required.
        /// </summary>
        public string TableName
        {
            get
            {
                if (String.IsNullOrEmpty(_tableName))
                    throw new Exception("Table Name is null.");
                return _tableName;
            }
            set
            {
                if (!String.IsNullOrEmpty(value))
                {
                    _tableName = value;
                    if (_account == null)
                        _account = GetStorageAccount();
                    CreateTableClient(_tableName);
                }
            }
        }

        private CloudStorageAccount _account;



        private readonly string _sourceAccountName;
        private readonly string _sourceAccountKey;

        public TableStorageHelper()
        {
            _sourceAccountName = ConfigurationManager.AppSettings["SourceAccountName"];
            _sourceAccountKey = ConfigurationManager.AppSettings["SourceAccountKey"];
            _account = GetStorageAccount();
        }

        private CloudStorageAccount GetStorageAccount()
        {
            return new CloudStorageAccount(new StorageCredentialsAccountAndKey(_sourceAccountName, _sourceAccountKey), true);
        }

        /// <summary>
        /// Inserts an entity
        /// </summary>
        /// <param name="entity">Entity to be inserted</param>
        public void Insert(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            entity = FixMinDate(entity);
            tableServiceContext.AddObject(TableName, entity);
            tableServiceContext.SaveChangesWithRetries();
        }

        /// <summary>
        /// Retrieves entity with matching partition key and row key
        /// </summary>
        /// <param name="partitionKey">Partition key of the desired entity</param>
        /// <param name="rowKey">Row key of the desired entity</param>
        /// <returns></returns>
        public T Get(string partitionKey, string rowKey)
        {
            T returnValue = null;
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            try
            {
                TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
                IQueryable<T> entities = (from e in tableServiceContext.CreateQuery<T>(TableName)
                                          where e.PartitionKey == partitionKey &&
                                          e.RowKey == rowKey
                                          select e);

                returnValue = entities.FirstOrDefault();
            }
            catch (Exception ex)
            {
                if (ex.InnerException != null)
                {
                    var dataServiceClientException = ex.InnerException as DataServiceClientException;
                    if (dataServiceClientException != null)
                    {
                        if (dataServiceClientException.StatusCode == 404)
                            returnValue = null;
                    }
                }
            }

            return returnValue;
        }

        /// <summary>
        /// Returns a list of all entities which match the given partion key.
        /// </summary>
        /// <param name="partitionKey">Partition key of the enenties to retrieved</param>
        /// <returns></returns>
        public List<T> GetList(string partitionKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            var returnValue = (from e in tableServiceContext.CreateQuery<T>(TableName)
                               where e.PartitionKey == partitionKey
                               select e).ToList();
            return returnValue;
        }

        /// <summary>
        /// Returns a list of all entities which match the given partion key.
        /// </summary>
        /// <param name="partitionKey">Partition key of the enenties to retrieved</param>
        /// <param name="start">Starting index</param>
        /// <param name="take">Page size</param>
        /// <returns></returns>
        public List<T> GetListPaginated(string partitionKey, int start, int take)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            var returnValue = (from e in tableServiceContext.CreateQuery<T>(TableName)
                               where e.PartitionKey == partitionKey
                               select e).Skip(start).Take(take).ToList();
            return returnValue;
        }

        /// <summary>
        /// Returns all entities in the table
        /// </summary>
        /// <returns></returns>
        public List<T> GetAll()
        {
            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            var returnValue = (from e in tableServiceContext.CreateQuery<T>(TableName)
                               select e).ToList();
            return returnValue;
        }

        /// <summary>
        /// Returns all entities in the table
        /// </summary>
        /// <param name="start">Starting index</param>
        /// <param name="take">Page size</param>
        /// <returns></returns>
        public List<T> GetAllPaginated(int start, int take)
        {
            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            var returnValue = (from e in tableServiceContext.CreateQuery<T>(TableName)
                               select e).Skip(start).Take(take).ToList();
            return returnValue;
        }

        /// <summary>
        /// Overwrites existing entity with the give entity
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be overwritten</param>
        /// <param name="rowKey">Row key of the entity to be overwritten</param>
        /// <param name="entity">Entity to overwrite with</param>
        public void Replace(string partitionKey, string rowKey, T entity)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            if (entity == null)
                throw new ArgumentNullException("entity");

            entity = FixMinDate(entity);

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            IQueryable<T> entities = (from e in tableServiceContext.CreateQuery<T>(TableName)
                                      where e.PartitionKey == partitionKey && e.RowKey == rowKey
                                      select e);
            T existingEntity = entities.FirstOrDefault();

            Type t = entity.GetType();
            PropertyInfo[] pi = t.GetProperties();

            foreach (PropertyInfo p in pi)
            {
                p.SetValue(existingEntity, p.GetValue(entity, null), null);
            }

            tableServiceContext.UpdateObject(existingEntity);
            tableServiceContext.SaveChangesWithRetries(SaveChangesOptions.ReplaceOnUpdate);

        }

        /// <summary>
        /// Updates the existing entity with the give entity
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be updated</param>
        /// <param name="rowKey">Row key of the entity to be overwritten</param>
        /// <param name="entity">Entity to overwrite with</param>
        public void Update(string partitionKey, string rowKey, T entity)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            if (entity == null)
                throw new ArgumentNullException("entity");

            entity = FixMinDate(entity);

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            IQueryable<T> entities = (from e in tableServiceContext.CreateQuery<T>(TableName)
                                      where e.PartitionKey == partitionKey && e.RowKey == rowKey
                                      select e);

            T existingEntity = entities.FirstOrDefault();

            if (existingEntity == null)
                throw new StorageClientException();

            Type t = entity.GetType();
            PropertyInfo[] pi = t.GetProperties();

            foreach (PropertyInfo p in pi)
            {
                if (p.Name.ToLowerInvariant() != "timestamp")
                {
                    object defaultValue = null;
                    if (p.PropertyType.IsValueType)
                    {
                        if (p.PropertyType.Name.ToLowerInvariant() == "datetime")
                            defaultValue = _tableClient.MinSupportedDateTime;
                        else
                            defaultValue = Activator.CreateInstance(p.PropertyType);
                    }

                    if (!p.PropertyType.IsValueType && (p.GetValue(entity, null) != defaultValue))
                        p.SetValue(existingEntity, p.GetValue(entity, null), null);
                    else if (p.PropertyType.IsValueType)
                        p.SetValue(existingEntity, p.GetValue(entity, null), null);
                }
            }

            tableServiceContext.UpdateObject(existingEntity);
            tableServiceContext.SaveChangesWithRetries();
        }

        /// <summary>
        /// OverwriteReferenceTypesOnly overwrites the existing entity's reference types with the give entity's corresponding values
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be updated</param>
        /// <param name="rowKey">Row key of the entity to be overwritten</param>
        /// <param name="entity">Entity to overwrite with</param>
        public void OverwriteReferenceTypesOnly(string partitionKey, string rowKey, T entity)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            if (entity == null)
                throw new ArgumentNullException("entity");

            entity = FixMinDate(entity);

            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            IQueryable<T> entities = (from e in tableServiceContext.CreateQuery<T>(TableName)
                                      where e.PartitionKey == partitionKey && e.RowKey == rowKey
                                      select e);

            T existingEntity = entities.FirstOrDefault();

            if (existingEntity == null)
                throw new StorageClientException();

            Type t = entity.GetType();
            PropertyInfo[] pi = t.GetProperties();

            foreach (PropertyInfo p in pi)
            {
                if (p.Name.ToLowerInvariant() != "timestamp")
                {
                    object defaultValue = null;
                    if (p.PropertyType.IsValueType)
                    {
                        if (p.PropertyType.Name.ToLowerInvariant() == "datetime")
                            defaultValue = _tableClient.MinSupportedDateTime;
                        else
                            defaultValue = Activator.CreateInstance(p.PropertyType);
                    }

                    if ((!p.PropertyType.IsValueType && (p.GetValue(entity, null) != defaultValue)) ||
                        (p.PropertyType.IsValueType && !defaultValue.Equals(p.GetValue(entity, null))))
                        p.SetValue(existingEntity, p.GetValue(entity, null), null);
                }
            }

            tableServiceContext.UpdateObject(existingEntity);
            tableServiceContext.SaveChangesWithRetries();
        }

        /// <summary>
        /// Overwrites existing entity with the give entity
        /// Or inserts it if no existing found
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be overwritten</param>
        /// <param name="rowKey">Row key of the entity to be overwritten</param>
        /// <param name="entity">Entity to overwrite with</param>
        public void Repsert(string partitionKey, string rowKey, T entity)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            if (entity == null)
                throw new ArgumentNullException("entity");

            try { Replace(partitionKey, rowKey, entity); }
            catch (DataServiceQueryException ex)
            {
                if ((int)ex.Response.StatusCode == 404)
                    Insert(entity);
            }
        }

        /// <summary>
        /// Updates the existing entity with the give entity
        /// Or inserts it if no existing found
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be updated</param>
        /// <param name="rowKey">Row key of the entity to be overwritten</param>
        /// <param name="entity">Entity to overwrite with</param>
        public void Upsert(string partitionKey, string rowKey, T entity)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");

            if (entity == null)
                throw new ArgumentNullException("entity");

            try { Update(partitionKey, rowKey, entity); }
            catch (DataServiceQueryException ex)
            {
                if ((int)ex.Response.StatusCode == 404)
                    Insert(entity);
            }
        }

        /// <summary>
        /// Delete the entity
        /// </summary>
        /// <param name="partitionKey">Partition key of the entity to be deleted</param>
        /// <param name="rowKey">Row key of the entity to be deleted</param>
        public void Delete(string partitionKey, string rowKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            if (string.IsNullOrEmpty(rowKey))
                throw new ArgumentNullException("rowKey");


            TableServiceContext tableServiceContext = _tableClient.GetDataServiceContext();
            IQueryable<T> entities = (from e in tableServiceContext.CreateQuery<T>(TableName)
                                      where e.PartitionKey == partitionKey && e.RowKey == rowKey
                                      select e);

            T entity = entities.FirstOrDefault();

            if (entity != null)
            {
                tableServiceContext.DeleteObject(entity);
                tableServiceContext.SaveChangesWithRetries();
            }
        }

        public bool Delete(string partitionKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
                throw new ArgumentNullException("partitionKey");

            var lstEntities = GetList(partitionKey);
            if (null == lstEntities) return true;
            foreach (T item in lstEntities)
            {
                Delete(item.PartitionKey, item.RowKey);
            }
            return true;
        }

        /// <summary>
        /// Returns TableServiceContext for set TableName
        /// </summary>
        /// <returns></returns>
        public TableServiceContext GetContext()
        {
            return _tableClient.GetDataServiceContext();
        }

        /// <summary>
        /// Fixes null datetime values to the azure MinSupportedDateTime
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private T FixMinDate(T entity)
        {
            if (entity == null)
                throw new ArgumentNullException("entity");

            Type t = entity.GetType();
            PropertyInfo[] pi = t.GetProperties();

            foreach (PropertyInfo p in pi.Where(p => ((p.PropertyType.Name.ToLowerInvariant() == "datetime") && (p.Name.ToLowerInvariant() != "timestamp"))))
            {
                if ((DateTime)entity.GetType().GetProperty(p.Name).GetValue(entity, null) == DateTime.MinValue)
                    p.SetValue(entity, _tableClient.MinSupportedDateTime, null);
            }

            return entity;
        }

        private void CreateTableClient(string tableName)
        {
            _tableClient = _account.CreateCloudTableClient();
            _tableClient.CreateTableIfNotExist(tableName);
            _tableClient.RetryPolicy = RetryPolicies.Retry(4, TimeSpan.Zero);
        }
    }
}

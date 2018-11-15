// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HierarchicalDataTransformerHelper.cs" company="Health Catalyst">
//   Health Catalyst Copyright 2018
// </copyright>
// <summary>
//   Defines the IHierarchicalDataTransformer type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------



namespace DataConverter
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Fabric.Databus.Client;
    using Fabric.Databus.Config;

    using Newtonsoft.Json;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer helper.
    /// </summary>
    public class HierarchicalDataTransformerHelper : IHierarchicalDataTransformerHelper
    {

        /// <summary>
        /// The service client.
        /// </summary>
        private readonly IMetadataServiceClient serviceClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformerHelper"/> class.
        /// </summary>
        /// <param name="serviceClient">
        /// The service client.
        /// </param>
        public HierarchicalDataTransformerHelper(IMetadataServiceClient serviceClient)
        {
            this.serviceClient = serviceClient;
        }

        /// <summary>
        /// The run databus.
        /// </summary>
        /// <param name="config">
        /// The config.
        /// </param>
        /// <param name="jobData">
        /// The job Data.
        /// </param>
        public void RunDatabus(QueryConfig config, JobData jobData)
        {
            var job = new Job
            {
                Config = config,
                Data = jobData
            };
            var runner = new DatabusRunner();
            runner.RunRestApiPipeline(new UnityContainer(), job, new CancellationToken());
        }

        /// <summary>
        /// The get data sources.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <param name="currentDataSources">
        /// The current data sources.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<List<DataSource>> GetDataSources(
            Binding binding,
            Binding[] bindings,
            List<DataSource> currentDataSources)
        {
            var entity = await this.GetEntityFromBinding(binding);
            if (entity != null)
            {
                // TODO: Get the object relationship from the binding for the key levels 
                // TODO: 
                currentDataSources.Add(new DataSource { Path = entity.EntityName, Sql = this.GetSqlFromEntity(entity) });
            }

            var bindingRelationship = binding.ObjectRelationships.FirstOrDefault(x => x.ChildObjectType == "binding");
            if (bindingRelationship != null)
            {
                var relationshipMatch = bindings.FirstOrDefault(x => x.Id == bindingRelationship.ChildObjectId);
                if (relationshipMatch != null)
                {
                    await this.GetDataSources(relationshipMatch, bindings, currentDataSources);
                }
            }
            return currentDataSources;
        }

        /// <summary>
        /// The get bindings for entity async.
        /// </summary>
        /// <param name="entityId">
        /// The entity id.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<Binding[]> GetBindingsForEntityAsync(int entityId)
        {
            var bindings = await this.serviceClient.GetBindingsForEntityAsync(entityId);
            return bindings;
        }

        /// <summary>
        /// The generate data model.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<string> GenerateDataModel(Binding binding, Binding[] bindings)
        {
            var sb = new StringBuilder();
            await this.GetChildText(sb, binding, bindings, true, true);
            var serialized = sb.ToString();
            serialized = serialized.Replace(",}", "}");
            return serialized;
        } 
        
        /// <summary>
        /// The get child text.
        /// </summary>
        /// <param name="builder">
        /// The builder.
        /// </param>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <param name="isFirst">
        /// The is first.
        /// </param>
        /// <param name="isObject">
        /// The is Object.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst, bool isObject)
        {
            var childObjectRelationships = this.GetObjectRelationships(binding);
            var hasChildren = childObjectRelationships.Count > 0;

            var parameterName = $"\"{await this.GetEntityName(binding)}\":";

            if (isFirst)
            {
                parameterName = string.Empty;
            }
            if (!hasChildren)
            {
                builder.Append(isObject ? $"{parameterName}" + "{}" : $"{parameterName}[]");
                if (!isFirst)
                {
                    builder.Append(",");
                }
                return;
            }

            builder.Append(parameterName);
            if (!isObject)
            {
                builder.Append("[");
            }

            builder.Append("{");

            foreach (var childObjectRelationship in childObjectRelationships)
            {
                if (childObjectRelationship != null)
                {
                    var childBinding = this.GetMatchingChild(bindings, childObjectRelationship.ChildObjectId);
                    var childIsObject = this.GetCardinalityFromObjectReference(childObjectRelationship) != "Array";
                    await this.GetChildText(builder, childBinding, bindings, false, childIsObject);
                }
            }

            builder.Append("}");
            if (!isObject)
            {
                builder.Append("]");
            }

            if (!isFirst)
            {
                builder.Append(",");
            }
        }

        /// <summary>
        /// The get config.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<QueryConfig> GetConfig()
        {
            // TODO: Replace this with actual config
            return await Task.Run(() => this.GetQueryConfigFromJsonFile());
        }

        /// <summary>
        /// The get config json file.
        /// </summary>
        /// <param name="filePath">
        /// The file path.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private QueryConfig GetQueryConfigFromJsonFile(string filePath = "config.json")
        {
            var json = System.IO.File.ReadAllText(filePath);
            var deserialzed = (dynamic)JsonConvert.DeserializeObject(json);

            var queryConfig = new QueryConfig
                                  {
                                      ConnectionString = deserialzed.ConnectionString,
                                      Url = deserialzed.Url,
                                      MaximumEntitiesToLoad = deserialzed.MaximumEntitiesToLoad,
                                      EntitiesPerBatch = deserialzed.EntitiesPerBatch,
                                      EntitiesPerUploadFile = deserialzed.EntitiesPerUploadFile,
                                      LocalSaveFolder = deserialzed.LocalSaveFolder,
                                      DropAndReloadIndex = deserialzed.DropAndReloadIndex,
                                      WriteTemporaryFilesToDisk = deserialzed.WriteTemporaryFilesToDisk,
                                      WriteDetailedTemporaryFilesToDisk = deserialzed.WriteDetailedTemporaryFilesToDisk,
                                      CompressFiles = deserialzed.CompressFiles,
                                      UploadToElasticSearch = deserialzed.UploadToElasticSearch,
                                      Index = deserialzed.Index,
                                      Alias = deserialzed.Alias,
                                      EntityType = deserialzed.EntityType,
                                      TopLevelKeyColumn = deserialzed.TopLevelKeyColumn,
                                      UseMultipleThreads = deserialzed.UseMultipleThreads,
                                      KeepTemporaryLookupColumnsInOutput = deserialzed.KeepTemporaryLookupColumnsInOutput,
                                  };

            return queryConfig;
        }

        /// <summary>
        /// The get cardinality from object reference.
        /// </summary>
        /// <param name="objectReference">
        /// The object reference.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetCardinalityFromObjectReference(ObjectReference objectReference)
        {
            return objectReference.AttributeValues.Where(x => x.AttributeName == "Cardinality")
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        /// <summary>
        /// The get entity name.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private async Task<string> GetEntityName(Binding binding)
        {
            if (binding == null)
            {
                return null;
            }
            var entity = await this.GetEntityFromBinding(binding);
            if (entity != null)
            {
                if (entity.EntityName != null)
                {
                    return entity.EntityName;
                }
            }

            return null;
        }


        /// <summary>
        /// The get matching child.
        /// </summary>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <param name="relationshipId">
        /// The relationship id.
        /// </param>
        /// <returns>
        /// The <see cref="Binding"/>.
        /// </returns>
        private Binding GetMatchingChild(Binding[] bindings, int relationshipId)
        {
            return bindings.FirstOrDefault(x => x.Id == relationshipId);
        }

        /// <summary>
        /// The get object relationship.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <returns>
        /// The <see cref="ObjectReference"/>.
        /// </returns>
        private List<ObjectReference> GetObjectRelationships(Binding binding)
        {
            var bindingRelationship = binding.ObjectRelationships.Where(x => x.ChildObjectType == "binding").ToList();
            return bindingRelationship;
        }

        /// <summary>
        /// The get table name from entity.
        /// </summary>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetTableNameFromEntity(Entity entity)
        {
            return this.GetAttributeValueFromEntity(entity, AttributeValue.TableName);
        }

        /// <summary>
        /// The get schema name from entity.
        /// </summary>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetSchemaNameFromEntity(Entity entity)
        {
            return this.GetAttributeValueFromEntity(entity, AttributeValue.SchemaName);
        }

        /// <summary>
        /// The get database name from entity.
        /// </summary>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetDatabaseNameFromEntity(Entity entity)
        {
            return this.GetAttributeValueFromEntity(entity, AttributeValue.DatabaseName);
        }

        /// <summary>
        /// The get attribute value from entity.
        /// </summary>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <param name="attributeName">
        /// The attribute name.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetAttributeValueFromEntity(Entity entity, string attributeName)
        {
            return entity.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        /// <summary>
        /// The get attribute value from binding.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="attributeName">
        /// The attribute name.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetAttributeValueFromBinding(Binding binding, string attributeName)
        {
            return binding.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        /// <summary>
        /// The get entity from binding.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            var entityReference = binding.SourcedByEntities.FirstOrDefault();
            if (entityReference != null)
            {
                var entity = await this.serviceClient.GetEntityAsync(entityReference.SourceEntityId);
                return entity;
            }

            return null;
        }

        /// <summary>
        /// The get sql from entity.
        /// </summary>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private string GetSqlFromEntity(Entity entity)
        {
            // TODO: get key levels from parentKeyfields attribute on object relationship
            if (entity != null)
            {
                var columns = "*";
                if (entity.Fields != null && entity.Fields.Count > 0)
                {
                    columns = string.Join(",", entity.Fields.Select(x => x.FieldName));
                }
                
                return $"select {columns} from [{this.GetDatabaseNameFromEntity(entity)}].[{this.GetSchemaNameFromEntity(entity)}].[{this.GetTableNameFromEntity(entity)}]";
            }

            return null;
        }
    }
}
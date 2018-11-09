// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IHierarchicalDataTransformer.cs" company="Health Catalyst">
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
            await this.GetChildText(sb, binding, bindings, true);
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
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst)
        {
            var childObjectRelationships = this.GetObjectRelationships(binding);
            var hasChildren = childObjectRelationships.Count > 0;
            var isObject = this.GetJsonPropertyType(binding) != "Array";
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
                    await this.GetChildText(builder, childBinding, bindings, false);
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
            return await Task.Run(
                       () => new QueryConfig
                                 {
                                     ConnectionString =
                                         "server=HC2260;initial catalog=EDWAdmin;Trusted_Connection=True;",
                                     Url =
                                         "https://HC2260.hqcatalyst.local/DataProcessingService/v1/BatchExecutions",
                                     MaximumEntitiesToLoad = 1000,
                                     EntitiesPerBatch = 100,
                                     EntitiesPerUploadFile = 100,
                                     LocalSaveFolder = @"C:\Catalyst\databus",
                                     DropAndReloadIndex = false,
                                     WriteTemporaryFilesToDisk = true,
                                     WriteDetailedTemporaryFilesToDisk = true,
                                     CompressFiles = false,
                                     UploadToElasticSearch = true,
                                     Index = "Patients2",
                                     Alias = "patients",
                                     EntityType = "patient",
                                     TopLevelKeyColumn = "BatchDefinitionId",
                                     UseMultipleThreads = false,
                                     KeepTemporaryLookupColumnsInOutput = true
                                 });
        }

        /// <summary>
        /// The get json property type.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetJsonPropertyType(Binding binding)
        {
            return this.GetAttributeValueFromBinding(binding, "JSONPropertyType");
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
            if (entity != null)
            {
                return $"select * from [{GetDatabaseNameFromEntity(entity)}].[{GetSchemaNameFromEntity(entity)}].[{GetTableNameFromEntity(entity)}]";
            }

            return null;
        }
    }
}
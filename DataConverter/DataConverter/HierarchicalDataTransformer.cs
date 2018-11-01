// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HierarchicalDataTransformer.cs" company="Health Catalyst">
//   Copyright 2018 by Health Catalyst.  All rights reserved.
// </copyright>
// <summary>
//   The hierarchical data transformer.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace DataConverter
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;

    using Newtonsoft.Json;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer.
    /// </summary>
    public class HierarchicalDataTransformer : IDataTransformer
    {
        /// <summary>
        /// The service client.
        /// </summary>
        private IMetadataServiceClient serviceClient;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="serviceClient">
        /// The service client.
        /// </param>
        public HierarchicalDataTransformer(IMetadataServiceClient serviceClient)
        {
            this.serviceClient = serviceClient;
        }

        /// <summary>
        /// The transform data async.
        /// </summary>
        /// <param name="bindingExecution">
        /// The binding execution.
        /// </param>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<long> TransformDataAsync(
            BindingExecution bindingExecution,
            Binding binding,
            Entity entity,
            CancellationToken cancellationToken)
        {
            var bindings = await this.serviceClient.GetBindingsForEntityAsync(entity.Id);
            var dataModel = await this.GenerateDataModel(binding, bindings, new ExpandoObject());
            var dataSources = await this.GetDataSources(binding, bindings, new List<DataSource>());

            // TODO: Replace this with real config
            var config = new QueryConfig
                             {
                                 ConnectionString = "server=HC2260;initial catalog=EDWAdmin;Trusted_Connection=True;",
                                 Url = "https://HC2260.hqcatalyst.local/DataProcessingService/v1/BatchExecutions",
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
                             };
            var jobData = new JobData { DataModel = dataModel, MyDataSources = dataSources };

            this.RunDatabus(config, jobData);
            return Convert.ToInt64(1);
        }

        /// <summary>
        /// The can handle.
        /// </summary>
        /// <param name="bindingExecution">
        /// The binding execution.
        /// </param>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="destinationEntity">
        /// The destination entity.
        /// </param>
        /// <returns>
        /// The <see cref="bool"/>.
        /// </returns>
        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == "Nested"; // BindingType.
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
        private async Task<List<DataSource>> GetDataSources(
            Binding binding,
            Binding[] bindings,
            List<DataSource> currentDataSources)
        {
            var entity = await this.GetEntityFromBinding(binding);
            if (entity != null)
            {
                currentDataSources.Add(new DataSource{Path = entity.EntityName, Sql = this.GetSqlFromEntity(entity)});
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

            // TODO: THIS IS FOR TESTING - CHANGE IT BACK
            return new List<DataSource>
                       {
                           new DataSource
                               {
                                   Sql =
                                       "SELECT 2 [BatchDefinitionId], 'Queued' [Status], 'Batch' [PipelineType]"
                               }
                       };
            return currentDataSources;
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
        /// <param name="obj">
        /// The obj.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        private async Task<string> GenerateDataModel(Binding binding, Binding[] bindings, ExpandoObject obj)
        {
            var entity = await this.GetEntityFromBinding(binding);
            if (entity != null)
            {
                if (entity.EntityName != null)
                {
                    // it's a child and we need the entity name to label the next element
                    var jsonPropertyType = this.GetAttributeValueFromBinding(binding, "JSONPropertyType");
                    if (jsonPropertyType != null)
                    {
                        if (jsonPropertyType == "Array")
                        {
                            this.AddProperty(obj, entity.EntityName, new dynamic[1]);
                        }
                        else
                        {
                            this.AddProperty(obj, entity.EntityName, new ExpandoObject());
                        }
                    }
                    else
                    {
                        // it's the parent
                        obj = new ExpandoObject();
                    }
                }
                else
                {
                    // it's the parent
                    obj = new ExpandoObject();
                }
            }

            var bindingRelationship = binding.ObjectRelationships.FirstOrDefault(x => x.ChildObjectType == "binding");
            if (bindingRelationship != null)
            {
                var relationshipMatch = bindings.FirstOrDefault(x => x.Id == bindingRelationship.ChildObjectId);
                if (relationshipMatch != null)
                {
                    await this.GenerateDataModel(relationshipMatch, bindings, obj);
                }
            }
            var serialized = JsonConvert.SerializeObject(obj);

            // TODO: THIS IS FOR TESTING - CHANGE IT BACK
            serialized = "{}";
            return serialized;
        }

        /// <summary>
        /// The add property.
        /// </summary>
        /// <param name="expando">
        /// The expando.
        /// </param>
        /// <param name="propertyName">
        /// The property name.
        /// </param>
        /// <param name="propertyValue">
        /// The property value.
        /// </param>
        private void AddProperty(ExpandoObject expando, string propertyName, object propertyValue)
        {
            var expandoDict = expando as IDictionary<string, object>;
            if (expandoDict.ContainsKey(propertyName))
            {
                expandoDict[propertyName] = propertyValue;
            }
            else
            {
                expandoDict.Add(propertyName, propertyValue);
            }
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
        public async Task<Entity> GetEntityFromBinding(Binding binding)
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

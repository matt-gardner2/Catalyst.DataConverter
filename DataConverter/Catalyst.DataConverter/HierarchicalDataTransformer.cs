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
        /// The helper.
        /// </summary>
        private readonly IMetadataServiceClient metadataServiceClient;

        private readonly Guid guid;
        private readonly DatabusRunner runner;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="metadataServiceClient">
        /// The metadata Service Client.
        /// </param>
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient)
        {
            this.metadataServiceClient = metadataServiceClient ?? throw new ArgumentException("metadataServiceClient cannot be null.");

            this.guid = Guid.NewGuid();
            this.runner = new DatabusRunner();

            LoggingHelper2.Debug(this.guid, "We Got Here: HierarchicalDataTransformer!");
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
            try
            {
                LoggingHelper2.Debug(this.guid, "In TransformDataAsync()");
                var config = this.GetQueryConfigFromJsonFile();
                LoggingHelper2.Debug(this.guid, $"Configuration: {JsonConvert.SerializeObject(config)}");

                var jobData = await this.GetJobData(binding, entity);
                LoggingHelper2.Debug(this.guid, $"JobData: {JsonConvert.SerializeObject(jobData)}");

                this.RunDatabus(config, jobData);
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug(this.guid, $"TransformDataAsync Threw exception: {e}");
            }

            return Convert.ToInt64(1);
        }

        /// <summary>
        /// <see cref="IDataTransformer.CanHandle"/>
        /// </summary>
        /// <param name="bindingExecution"></param>
        /// <param name="binding"></param>
        /// <param name="destinationEntity"></param>
        /// <returns></returns>
        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            //LoggingHelper2.Debug(this.guid, "We got to the CanHandle Method");

            var guid2 = Guid.NewGuid();

            Binding topMost;
            try
            {
                Binding[] allBindings = this.GetBindingsForEntityAsync(destinationEntity).Result;
                topMost = this.GetTopMostBinding(allBindings);
                //LoggingHelper2.Debug(this.guid, $"All bindings ({guid2.ToString().Substring(0, 10)}): {JsonConvert.SerializeObject(allBindings)}");
                //LoggingHelper2.Debug(this.guid, $"binding ({guid2.ToString().Substring(0, 10)}): {JsonConvert.SerializeObject(binding)}");
                //LoggingHelper2.Debug(this.guid, $"TopMost ({guid2.ToString().Substring(0, 10)}): {JsonConvert.SerializeObject(topMost)}");
                //LoggingHelper2.Debug(this.guid, $"Is topmost?? ({guid2.ToString().Substring(0, 10)}): {binding.Id == topMost.Id}");
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug(this.guid, $"Threw exception ({guid2.ToString().Substring(0, 10)}): {e}");
                throw;
            }

            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == "Nested" && binding.Id == topMost.Id; // BindingType.
        }

        private Binding GetTopMostBinding(Binding[] bindings)
        {
            if (bindings == null || bindings.Length == 0)
            {
                LoggingHelper2.Debug(this.guid, "ERROR - Throwing exception: Could not get top most binding from a list with no bindings");
                throw new InvalidOperationException("Could not get top most binding from a list with no bindings");
            }

            return bindings.First(binding => !this.GetAncestorObjectRelationships(binding, bindings).Any());
        }

        private QueryConfig GetQueryConfigFromJsonFile(string filePath = "config.json")
        {
            var json = System.IO.File.ReadAllText(filePath);
            var deserialized = (dynamic)JsonConvert.DeserializeObject(json);

            var queryConfig = new QueryConfig
                                  {
                                      ConnectionString = deserialized.ConnectionString,
                                      Url = deserialized.Url,
                                      MaximumEntitiesToLoad = deserialized.MaximumEntitiesToLoad,
                                      EntitiesPerBatch = deserialized.EntitiesPerBatch,
                                      EntitiesPerUploadFile = deserialized.EntitiesPerUploadFile,
                                      LocalSaveFolder = deserialized.LocalSaveFolder,
                                      DropAndReloadIndex = deserialized.DropAndReloadIndex,
                                      WriteTemporaryFilesToDisk = deserialized.WriteTemporaryFilesToDisk,
                                      WriteDetailedTemporaryFilesToDisk = deserialized.WriteDetailedTemporaryFilesToDisk,
                                      CompressFiles = deserialized.CompressFiles,
                                      UploadToElasticSearch = deserialized.UploadToElasticSearch,
                                      Index = deserialized.Index,
                                      Alias = deserialized.Alias,
                                      EntityType = deserialized.EntityType,
                                      TopLevelKeyColumn = deserialized.TopLevelKeyColumn,
                                      UseMultipleThreads = deserialized.UseMultipleThreads,
                                      KeepTemporaryLookupColumnsInOutput = deserialized.KeepTemporaryLookupColumnsInOutput,
                                  };

            return queryConfig;
        }

        private async Task<JobData> GetJobData(Binding binding, Entity destinationEntity)
        {
            var jobData = new JobData();

            Binding[] allBindings = await this.GetBindingsForEntityAsync(destinationEntity);

            this.ValidateHierarchicalBinding(binding, allBindings);

            List<DataSource> dataSources = new List<DataSource>();

            await this.GenerateDataSources(binding, allBindings, destinationEntity, dataSources, null, "$", isFirst: true);

            jobData.MyDataSources = dataSources;

            return jobData;
        }

        /// <summary>
        /// Execute DataBus with the given configuration and job data
        /// </summary>
        /// <param name="config"></param>
        /// <param name="jobData"></param>
        private void RunDatabus(QueryConfig config, JobData jobData)
        {
            LoggingHelper2.Debug(this.guid, "We are trying to run Databus");
            var job = new Job
                          {
                              Config = config,
                              Data = jobData
                          };
            try
            {
                this.runner.RunRestApiPipeline(new UnityContainer(), job, new CancellationToken());
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug(this.guid, $"Exception thrown by Databus: {e}");
            }

            LoggingHelper2.Debug(this.guid, "Finished executing Databus");
        }

        private void ValidateHierarchicalBinding(Binding binding, Binding[] allBindings)
        {
            if (!allBindings.All(b => b.SourcedByEntities.Count == 1 && b.SourcedByEntities.FirstOrDefault() != null))
            {
                throw new InvalidOperationException("All bindings must have exactly 1 sourced entity");
            }

            if (this.GetAncestorObjectRelationships(binding, allBindings).Any())
            {
                throw new InvalidOperationException("Top-most binding cannot have any ancestor bindings");
            }

            if (allBindings.Any(b => !this.GetAncestorObjectRelationships(b, allBindings).Any() && !this.GetChildObjectRelationships(b).Any()))
            {
                throw new InvalidOperationException("Each binding must be in a relationship");
            }

            // TODO: Innovation Time -- validate that tree is not disjointed
        }

        private async Task GenerateDataSources(
            Binding rootBinding,
            Binding[] allBindings,
            Entity destinationEntity,
            List<DataSource> dataSources,
            ObjectReference relationshipToParent,
            string path,
            bool isFirst)
        {
            var sourceEntity = await this.GetEntityFromBinding(rootBinding);
            LoggingHelper2.Debug(this.guid, $"GenerateDataSources -- sourceEntity: {JsonConvert.SerializeObject(sourceEntity)}");
            dataSources.Add(
                new DataSource
                    {
                        Path = path,
                        TableOrView = this.GetFullyQualifiedTableName(sourceEntity),
                        MySqlEntityColumnMappings =
                            await this.GetColumnsFromEntity(sourceEntity, destinationEntity, rootBinding.SourcedByEntities.First().SourceAliasName),
                        PropertyType = isFirst ? null : this.GetCardinalityFromObjectReference(relationshipToParent),
                        MyRelationships = isFirst ? new List<SqlRelationship>() : await this.GetDatabusRelationships(rootBinding, allBindings, sourceEntity)
                    });

            var childObjectRelationships = this.GetChildObjectRelationships(rootBinding);
            var hasChildren = childObjectRelationships.Count > 0;
            if (!hasChildren)
            {
                return;
            }
            
            foreach (var childObjectRelationship in childObjectRelationships)
            {
                if (childObjectRelationship != null)
                {
                    var childBinding = this.GetMatchingChild(allBindings, childObjectRelationship.ChildObjectId);
                    await this.GenerateDataSources(
                        childBinding,
                        allBindings,
                        destinationEntity,
                        dataSources,
                        childObjectRelationship,
                        string.Join(".", path, await this.GetSourceAliasName(childBinding)),
                        isFirst: false);
                }
            }
        }

        private async Task<string> GetSourceAliasName(Binding binding)
        {
            return binding?.SourcedByEntities?.FirstOrDefault()?.SourceAliasName ?? (await this.GetEntityFromBinding(binding)).EntityName;
        }

        private async Task<List<SqlRelationship>> GetDatabusRelationships(Binding binding, Binding[] allBindings, Entity sourceEntity)
        {
            List<BindingReference> ancestorRelationships = this.GetAncestorObjectRelationships(binding, allBindings);
            List<SqlRelationship> sqlRelationships = new List<SqlRelationship>();

            foreach (BindingReference ancestorRelationship in ancestorRelationships)
            {
                Entity ancestorEntity = await this.GetEntityFromBinding(allBindings.First(b => b.Id == ancestorRelationship.ParentObjectId));

                LoggingHelper2.Debug(this.guid, $"SourceEntity: {JsonConvert.SerializeObject(sourceEntity)}");
                LoggingHelper2.Debug(this.guid, $"ancestorEntity: {JsonConvert.SerializeObject(ancestorEntity)}");

                sqlRelationships.Add(
                    new SqlRelationship
                        {
                            MySource = new SqlRelationshipEntity
                                           {
                                               Entity = this.GetFullyQualifiedTableName(ancestorEntity),
                                               Key = this.CleanJson(
                                                   ancestorRelationship.AttributeValues.GetAttributeTextValue("ParentKeyFields"))
                                           }, // TODO - databus doesn't currently handle comma separated lists here
                            MyDestination =
                                new SqlRelationshipEntity
                                    {
                                        Entity = this.GetFullyQualifiedTableName(sourceEntity),
                                        Key = this.CleanJson(ancestorRelationship.AttributeValues.GetAttributeTextValue("ChildKeyFields"))
                                    } // TODO - databus doesn't currently handle comma separated lists here
                        });
            }

            return sqlRelationships;
        }

        private async Task<Binding[]> GetBindingsForEntityAsync(Entity entity)
        {
            //LoggingHelper2.Debug(this.guid, "Entering GetBindingsForEntityAsync");
            var bindingsForDataMart = await this.metadataServiceClient.GetBindingsForDataMartAsync(entity.DataMartId);
            //LoggingHelper2.Debug(this.guid, $"BindingsForDataMart[{entity.DataMartId}]: {JsonConvert.SerializeObject(bindingsForDataMart)}");
            //LoggingHelper2.Debug(this.guid, $"BindingsForDataMart[{entity.DataMartId}] - filtered by DestEntity: {JsonConvert.SerializeObject(bindingsForDataMart.Where(binding => binding.DestinationEntityId == entity.Id).ToArray())}");

            return bindingsForDataMart.Where(binding => binding.DestinationEntityId == entity.Id).ToArray();
        }

        private string GetCardinalityFromObjectReference(ObjectReference objectReference)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetCardinalityFromObjectReference(...)");
            LoggingHelper2.Debug(this.guid, $"objectReference: {JsonConvert.SerializeObject(objectReference)}");
            return this.GetAttributeValueFromObjectReference(objectReference, "Cardinality").Equals("array", StringComparison.CurrentCultureIgnoreCase) ? "array" : "object";
        }

        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetAttributeValueFromObjectReference(...)");
            LoggingHelper2.Debug(this.guid, $"objectReference: {JsonConvert.SerializeObject(objectReference)}");
            LoggingHelper2.Debug(this.guid, $"attributeName: {attributeName}");

            return objectReference.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        private Binding GetMatchingChild(Binding[] bindings, int childBindingId)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetMatchingChild(...)");
            return bindings.FirstOrDefault(x => x.Id == childBindingId);
        }

        private List<ObjectReference> GetChildObjectRelationships(Binding binding)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetChildObjectRelationships(...)");
            var childRelationships = binding.ObjectRelationships.Where(
                    or => or.ChildObjectType == "Binding"
                          && or.AttributeValues.First(attr => attr.AttributeName == "GenerationGap").ValueToInt()
                          == 1)
                .ToList();

            LoggingHelper2.Debug(this.guid, $"Found the following childRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(childRelationships)}");
            return childRelationships;
        }

        private List<BindingReference> GetAncestorObjectRelationships(Binding binding, Binding[] allBindings)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetAncestorObjectRelationships(...)");
            var parentRelationships = new List<BindingReference>();
            foreach (var otherBinding in allBindings.Where(b => b.Id != binding.Id))
            {
                parentRelationships.AddRange(
                    otherBinding.ObjectRelationships.Where(
                        relationship => relationship.ChildObjectId == binding.Id && relationship.ChildObjectType == "Binding").Select(
                        x => new BindingReference
                        {
                            ChildObjectId = x.ChildObjectId,
                            AttributeValues = x.AttributeValues,
                            ChildObjectType = x.ChildObjectType,
                            ParentObjectId = otherBinding.Id
                        }));
            }

            LoggingHelper2.Debug(this.guid, $"Found the following parentRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(parentRelationships)}");

            return parentRelationships;
        }

        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            LoggingHelper2.Debug(this.guid, "Entering GetEntityFromBinding(...)");
            LoggingHelper2.Debug(this.guid, "binding: " + JsonConvert.SerializeObject(binding));

            if (binding == null || !binding.SourcedByEntities.Any() || binding.SourcedByEntities.FirstOrDefault() == null)
            {
                return null;
            }

            var entityReference = binding.SourcedByEntities.First();
            var entity = await this.metadataServiceClient.GetEntityAsync(entityReference.SourceEntityId);
            LoggingHelper2.Debug(this.guid, $"Found source destinationEntity ({entity.EntityName}) for binding (id = {binding.Id})");
            return entity;
        }

        private async Task<List<SqlEntityColumnMapping>> GetColumnsFromEntity(Entity sourceEntity, Entity destinationEntity, string entityAlias)
        {
            if (sourceEntity == null || destinationEntity == null)
            {
                return null;
            }

            List<SqlEntityColumnMapping> columns = new List<SqlEntityColumnMapping>();

            Field[] sourceEntityFields = await this.metadataServiceClient.GetEntityFieldsAsync(sourceEntity);

            // Add all Active fields (based on destination entity)
            columns.AddRange(
                sourceEntityFields
                    .Where(
                        field => destinationEntity.Fields.Any(
                            destinationField => destinationField.FieldName == $"{sourceEntity.EntityName}__{field.FieldName}" && destinationField.Status != FieldStatus.Omitted))
                    .Select(f => new SqlEntityColumnMapping { Name = f.FieldName, Alias = entityAlias ?? f.FieldName }));

            return columns;
        }

        private string GetFullyQualifiedTableName(Entity sourceEntity)
        {
            return $"[{sourceEntity.DatabaseName}].[{sourceEntity.SchemaName}].[{sourceEntity.EntityName}]";
        }

        private string CleanJson(string dirty)
        {
            return dirty.Replace("[", string.Empty).Replace("]", string.Empty).Replace('"', ' ').Trim();
        }
    }
}

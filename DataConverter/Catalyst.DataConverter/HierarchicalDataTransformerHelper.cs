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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

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
            LoggingHelper2.Debug("Entering the DataTransformerHelper");
            this.serviceClient = serviceClient;
        }

        /// <summary>
        /// Execute DataBus with the given configuration and job data
        /// </summary>
        /// <param name="config"></param>
        /// <param name="jobData"></param>
        public void RunDatabus(QueryConfig config, JobData jobData)
        {
            LoggingHelper2.Debug("We are trying to run Databus");
            var job = new Job
            {
                Config = config,
                Data = jobData
            };
            try
            {
                var runner = new DatabusRunner();
                runner.RunRestApiPipeline(new UnityContainer(), job, new CancellationToken());
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"Exception thrown by Databus: {e}");
            }

            LoggingHelper2.Debug("Finished executing Databus");
        }
        
        /// <summary>
        /// Get the Data Sources that DataBus needs for the given bindings
        /// </summary>
        /// <param name="tipBinding"></param>
        /// <param name="bindings"></param>
        /// <param name="currentDataSources"></param>
        /// <param name="depthMap"></param>
        /// <param name="destinationEntity"></param>
        /// <returns></returns>
        public async Task<List<DataSource>> GetDataSources(
            Binding tipBinding,
            Binding[] bindings,
            List<DataSource> currentDataSources, 
            Dictionary<int, List<int>> depthMap,
            Entity destinationEntity)
        {
            LoggingHelper2.Debug("Entering GetDataSources");
            var sourceEntity = await this.GetEntityFromBinding(tipBinding);
            if (sourceEntity != null)
            {
                var sql = await this.GetSqlFromEntity(sourceEntity, destinationEntity);

                sql = await this.AddKeyLevels(sql, depthMap, tipBinding, bindings, sourceEntity);

                currentDataSources.Add(new DataSource { Path = sourceEntity.EntityName, Sql = sql });
            }

            var bindingRelationships = tipBinding.ObjectRelationships.Where(x => x.ChildObjectType == "Binding").ToList();
            if (bindingRelationships.Count > 0)
            {
                foreach (var relationship in bindingRelationships)
                {
                    var nextTipBinding = bindings.FirstOrDefault(x => x.Id == relationship.ChildObjectId);
                    if (nextTipBinding != null)
                    {
                        await this.GetDataSources(nextTipBinding, bindings, currentDataSources, depthMap, destinationEntity);
                    }
                }
            }

            LoggingHelper2.Debug("We are finishing up GetDataSources");
            return currentDataSources;
        }

        public Binding GetTopMostBinding(Binding[] bindings)
        {
            if (bindings == null || bindings.Length == 0)
            {
                LoggingHelper2.Debug("ERROR - Throwing exception: Could not get top most binding from a list with no bindings");
                throw new InvalidOperationException("Could not get top most binding from a list with no bindings");
            }

            return bindings.First(binding => !this.GetParentObjectRelationships(binding, bindings).Any());
        }
        
        /// <summary>
        /// Get all bindings whose destination entity is the given entity
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public async Task<Binding[]> GetBindingsForEntityAsync(Entity entity)
        {
            LoggingHelper2.Debug("Entering GetBindingsForEntityAsync");
            var bindingsForDataMart = await this.serviceClient.GetBindingsForDataMartAsync(entity.DataMartId);
            return bindingsForDataMart.Where(binding => binding.DestinationEntityId == entity.Id).ToArray();
        }

        /// <summary>
        /// Generate the data model string and depth map for the given bindings, given the top-most Binding as a starting point
        /// </summary>
        /// <param name="topMostBinding"></param>
        /// <param name="allBindings"></param>
        /// <returns></returns>
        public async Task<DataModelDepthMap> GenerateDataModel(Binding topMostBinding, Binding[] allBindings)
        {
            LoggingHelper2.Debug("Entering GenerateDataModel(...)");
            LoggingHelper2.Debug("topMostBinding: " + JsonConvert.SerializeObject(topMostBinding));
            LoggingHelper2.Debug("Bindings: " + JsonConvert.SerializeObject(allBindings));

            var sb = new StringBuilder();
            var bindingDepthMap = new Dictionary<int, List<int>>();
            await this.GetChildText(sb, topMostBinding, allBindings, true, true, bindingDepthMap, 0);
            var serialized = sb.ToString();
            serialized = serialized.Replace(",}", "}");
            LoggingHelper2.Debug("Here's our data model: " + serialized);
            LoggingHelper2.Debug("Final DepthMap: " + JsonConvert.SerializeObject(bindingDepthMap));

            return new DataModelDepthMap { DataModel = serialized, DepthMap = bindingDepthMap };
        } 
        
        /// <summary>
        /// Gets the child text
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="thisBinding"></param>
        /// <param name="allBindings"></param>
        /// <param name="isFirst"></param>
        /// <param name="isObject"></param>
        /// <param name="depthMap"></param>
        /// <param name="depth"></param>
        public async Task GetChildText(StringBuilder builder, Binding thisBinding, Binding[] allBindings, bool isFirst, bool isObject, Dictionary<int, List<int>> depthMap, int depth)
        {
            LoggingHelper2.Debug("Entering GetChildText(...)");
            LoggingHelper2.Debug("Current Model: " + builder);
            LoggingHelper2.Debug("thisBinding: " + JsonConvert.SerializeObject(thisBinding));
            LoggingHelper2.Debug("allBindings: " + JsonConvert.SerializeObject(allBindings));
            LoggingHelper2.Debug("isFirst: " + isFirst);
            LoggingHelper2.Debug("isObject: " + isObject);
            LoggingHelper2.Debug("depth: " + depth);

            LoggingHelper2.Debug("Current DepthMap: " + JsonConvert.SerializeObject(depthMap));

            var childObjectRelationships = this.GetChildObjectRelationships(thisBinding);
            var hasChildren = childObjectRelationships.Count > 0;

            if (!depthMap.ContainsKey(depth))
            {
                depthMap.Add(depth, new List<int> { thisBinding.Id });
            }
            else
            {
                depthMap[depth].Add(thisBinding.Id);
            }

            var parameterName = $"\"{await this.GetEntityName(thisBinding)}\":";

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
                    var childBinding = this.GetMatchingChild(allBindings, childObjectRelationship.ChildObjectId);
                    var childIsObject = this.GetCardinalityFromObjectReference(childObjectRelationship) != "Array";
                    await this.GetChildText(builder, childBinding, allBindings, false, childIsObject, depthMap, depth + 1);
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

        public async Task<QueryConfig> GetConfig()
        {
            LoggingHelper2.Debug("Entering GetConfig(...)");
            return await Task.Run(() => this.GetQueryConfigFromJsonFile());
        }

        public async Task<string> AddKeyLevels(string currentSqlString, Dictionary<int, List<int>> keyleveldepth, Binding binding, Binding[] bindings, Entity entity)
        {
            LoggingHelper2.Debug("Entering AddKeyLevels(...)");
            LoggingHelper2.Debug("currentSqlString: " + currentSqlString);
            LoggingHelper2.Debug("keyleveldepth: " + JsonConvert.SerializeObject(keyleveldepth));
            LoggingHelper2.Debug("binding: " + JsonConvert.SerializeObject(binding));
            LoggingHelper2.Debug("bindings: " + JsonConvert.SerializeObject(bindings));
            LoggingHelper2.Debug("entity: " + JsonConvert.SerializeObject(entity));

            var descendantObjectRelationships = this.GetDescendantObjectRelationships(binding);

            if (descendantObjectRelationships.Any())
            {
                var singleResult = descendantObjectRelationships.Select(
                        x => x.AttributeValues.First(atr => atr.AttributeName == "ParentKeyFields").AttributeValue)
                    .Distinct().ToList();
                if (singleResult.Count != 1)
                {
                    LoggingHelper2.Debug($"Threw exception: All of the children for this binding ({binding.Id}) do not have the same parent key designation");
                    throw new InvalidOperationException(
                        $"All of the children for this binding ({binding.Id}) do not have the same parent key designation.");
                }

                var myDepth = keyleveldepth.Where(x => x.Value.Contains(binding.Id)).Select(x => x.Key)
                    .FirstOrDefault();
                var myColumn = singleResult.First();
                currentSqlString = await this.GetKeyLevelSql(myColumn, currentSqlString, myDepth, entity);
            }

            var parentObjectReferences = this.GetParentObjectRelationships(binding, bindings);
            foreach (var bindingReference in parentObjectReferences)
            {
                var depth = keyleveldepth.Where(x => x.Value.Contains(bindingReference.ParentObjectId))
                    .Select(x => x.Key).FirstOrDefault();
                var column = this.GetAttributeValueFromObjectReference(
                    new ObjectReference { AttributeValues = bindingReference.AttributeValues },
                    "ChildKeyFields");

                currentSqlString = await this.GetKeyLevelSql(column, currentSqlString, depth, entity);
            }

            LoggingHelper2.Debug("KeyLevel'd Sql: " + currentSqlString);

            return currentSqlString;
        }
        
        private async Task<string> GetKeyLevelSql(string keyFieldsString, string originalSql, int depth, Entity sourceEntity)
        {
            LoggingHelper2.Debug("Entering GetKeyLevelSql(...)");
            LoggingHelper2.Debug($"keyFieldsString: {keyFieldsString}");
            LoggingHelper2.Debug($"originalSql: {originalSql}");
            LoggingHelper2.Debug($"depth: {depth}");
            LoggingHelper2.Debug($"entity: {JsonConvert.SerializeObject(sourceEntity)}");

            var convertedToArray = keyFieldsString.Replace("[", string.Empty).Replace("]", string.Empty).Replace('"', ' ').Split(',');
            convertedToArray = convertedToArray.Select(x => x.Trim()).ToArray();

            Field[] sourceEntityFields = await this.serviceClient.GetEntityFieldsAsync(sourceEntity);

            // make sure the column is in the source entity
            foreach (var field in convertedToArray)
            {
                if (sourceEntityFields.All(x => !string.Equals(x.FieldName, field, StringComparison.CurrentCultureIgnoreCase)))
                {
                    return originalSql;
                }
            }

            if (convertedToArray.Length > 1)
            {
                // gotta concatenate
                return Regex.Replace(originalSql, "select ", $"SELECT CONCAT({string.Join(",'-',", convertedToArray)}) AS KeyLevel{depth}, ", RegexOptions.IgnoreCase);
                
                // return originalSql.ToUpper().Replace("SELECT ", $"SELECT CONCAT({string.Join(",'-',", convertedToArray)}) AS KeyLevel{depth}, ");
            }

            return Regex.Replace(originalSql, "select ", $"SELECT {convertedToArray[0]} AS KeyLevel{depth}, ", RegexOptions.IgnoreCase);

            // return originalSql.ToUpper().Replace("SELECT ", $"SELECT {convertedToArray[0]} AS KeyLevel{depth}, ");
        }

        private QueryConfig GetQueryConfigFromJsonFile(string filePath = "config.json")
        {
            LoggingHelper2.Debug("Entering GetQueryConfigFromJsonFile(...)");
            LoggingHelper2.Debug($"filePath: {filePath}");

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

        private string GetCardinalityFromObjectReference(ObjectReference objectReference)
        {
            LoggingHelper2.Debug("Entering GetCardinalityFromObjectReference(...)");
            LoggingHelper2.Debug($"objectReference: {JsonConvert.SerializeObject(objectReference)}");
            return this.GetAttributeValueFromObjectReference(objectReference, "Cardinality");
        }

        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            LoggingHelper2.Debug("Entering GetAttributeValueFromObjectReference(...)");
            LoggingHelper2.Debug($"objectReference: {JsonConvert.SerializeObject(objectReference)}");
            LoggingHelper2.Debug($"attributeName: {attributeName}");

            return objectReference.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        private async Task<string> GetEntityName(Binding binding)
        {
            LoggingHelper2.Debug($"Entering GetEntityName({JsonConvert.SerializeObject(binding)})");
            if (binding == null)
            {
                return null;
            }

            var entity = await this.GetEntityFromBinding(binding);
            if (entity?.EntityName != null)
            {
                LoggingHelper2.Debug($"Found EntityName: {entity.EntityName}");
                return entity.EntityName;
            }

            return null;
        }

        private Binding GetMatchingChild(Binding[] bindings, int childBindingId)
        {
            LoggingHelper2.Debug("Entering GetMatchingChild(...)");
            return bindings.FirstOrDefault(x => x.Id == childBindingId);
        }

        private List<ObjectReference> GetChildObjectRelationships(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetChildObjectRelationships(...)");
            var childRelationships = binding.ObjectRelationships.Where(
                    or => or.ChildObjectType == "Binding"
                          && or.AttributeValues.First(attr => attr.AttributeName == "GenerationGap").ValueToInt()
                          == 1)
                .ToList();

            LoggingHelper2.Debug($"Found the following childRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(childRelationships)}");
            return childRelationships;
        }

        private List<ObjectReference> GetDescendantObjectRelationships(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetDescendantObjectRelationships(...)");
            var descendantRelationships = binding.ObjectRelationships.Where(or => or.ChildObjectType == "Binding").ToList();
            LoggingHelper2.Debug($"Found the following descendantRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(descendantRelationships)}");
            return descendantRelationships;
        }

        private List<BindingReference> GetParentObjectRelationships(Binding binding, Binding[] allBindings)
        {
            LoggingHelper2.Debug("Entering GetParentObjectRelationships(...)");
            var parentRelationships = new List<BindingReference>();
            foreach (var otherBinding in allBindings)
            {
                parentRelationships.AddRange(
                    otherBinding.ObjectRelationships.Where(
                        x => x.ChildObjectId == binding.Id && x.ChildObjectType == "Binding").Select(
                        x => new BindingReference
                                 {
                                     ChildObjectId = x.ChildObjectId,
                                     AttributeValues = x.AttributeValues,
                                     ChildObjectType = x.ChildObjectType,
                                     ParentObjectId = otherBinding.Id
                                 }));
            }

            LoggingHelper2.Debug($"Found the following parentRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(parentRelationships)}");

            return parentRelationships;
        }

        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetEntityFromBinding(...)");
            LoggingHelper2.Debug("binding: " + JsonConvert.SerializeObject(binding));
            var entityReference = binding.SourcedByEntities.FirstOrDefault(); 
            if (entityReference != null)
            {
                var entity = await this.serviceClient.GetEntityAsync(entityReference.SourceEntityId);
                LoggingHelper2.Debug($"Found source entity ({entity.EntityName}) for binding (id = {binding.Id})");
                return entity;
            }

            return null;
        }

        private async Task<string> GetSqlFromEntity(Entity sourceEntity, Entity destinationEntity)
        {
            LoggingHelper2.Debug("Entering GetSqlFromEntity");
            LoggingHelper2.Debug($"sourceEntity: {JsonConvert.SerializeObject(sourceEntity)}");
            LoggingHelper2.Debug($"destinationEntity: {JsonConvert.SerializeObject(destinationEntity)}");

            if (sourceEntity != null && destinationEntity != null)
            {
                Field[] sourceEntityFields = await this.serviceClient.GetEntityFieldsAsync(sourceEntity);
                var columns = "*"; 
                if (sourceEntityFields != null && sourceEntityFields.Length > 0)
                {
                    columns = string.Join(
                        ", ",
                        sourceEntityFields
                            .Where(
                                field => destinationEntity
                                    .Fields
                                    .Any(
                                        deField => deField.FieldName == $"{sourceEntity.EntityName}_{field.FieldName}"  
                                                   && deField.Status != FieldStatus.Omitted))
                            .Select(x => x.FieldName));
                }

                return $"select {columns} from [{sourceEntity.DatabaseName}].[{sourceEntity.SchemaName}].[{sourceEntity.TableName}]";
            }

            return null;
        }

        public class DataModelDepthMap
        {
            public string DataModel { get; set; }

            public Dictionary<int, List<int>> DepthMap { get; set; }
        }
    }
}
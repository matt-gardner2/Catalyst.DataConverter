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

        public async Task<List<DataSource>> GetDataSources(
            Binding binding,
            Binding[] bindings,
            List<DataSource> currentDataSources, 
            Dictionary<int, List<int>> depthMap)
        {
            var entity = await this.GetEntityFromBinding(binding);
            if (entity != null)
            {
                var sql = this.GetSqlFromEntity(entity);

                sql = this.AddKeyLevels(sql, depthMap, binding, bindings);

                currentDataSources.Add(new DataSource { Path = entity.EntityName, Sql = sql });
            }

            var bindingRelationships = binding.ObjectRelationships.Where(x => x.ChildObjectType == "Binding").ToList();
            if (bindingRelationships.Count > 0)
            {
                foreach (var relationship in bindingRelationships)
                {
                    var relationshipMatch = bindings.FirstOrDefault(x => x.Id == relationship.ChildObjectId);
                    if (relationshipMatch != null)
                    {
                        await this.GetDataSources(relationshipMatch, bindings, currentDataSources, depthMap);
                    }
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
        public string GenerateDataModel(Binding binding, Binding[] bindings, out Dictionary<int, List<int>> bindingDepthMap)
        {
            var sb = new StringBuilder();
            bindingDepthMap = new Dictionary<int, List<int>>();
            this.GetChildText(sb, binding, bindings, true, true, bindingDepthMap, 0);
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
        /// <param name="depth">
        /// The depth.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public void GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst, bool isObject, Dictionary<int, List<int>> depthMap, int depth)
        {
            var childObjectRelationships = this.GetChildObjectRelationships(binding);
            var hasChildren = childObjectRelationships.Count > 0;

            if (!depthMap.ContainsKey(depth))
            {
                depthMap.Add(depth, new List<int> { binding.Id });
            }
            else
            {
                depthMap[depth].Add(binding.Id);
            }

            var parameterName = $"\"{Task.Run(()=>this.GetEntityName(binding)).Result}\":";

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
                    this.GetChildText(builder, childBinding, bindings, false, childIsObject, depthMap, depth + 1);
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
            return await Task.Run(() => this.GetQueryConfigFromJsonFile());
        }

        /// <summary>
        /// The add key levels.
        /// </summary>
        /// <param name="currentSqlString">
        /// The current sql string.
        /// </param>
        /// <param name="keyleveldepth">
        /// The keyleveldepth.
        /// </param>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public string AddKeyLevels(string currentSqlString, Dictionary<int, List<int>> keyleveldepth, Binding binding, Binding[] bindings)
        {
            var newSqlString = currentSqlString;
            var childObjectReferences = this.GetChildObjectRelationships(binding);

            if (childObjectReferences.Any())
            {
                var singleResult = childObjectReferences.Select(
                        x => x.AttributeValues.First(atr => atr.AttributeName == "ParentKeyFields").AttributeValue)
                    .Distinct().ToList();
                if (singleResult.Count != 1)
                {
                    throw new InvalidOperationException(
                        $"All of the children for this binding ({binding.Id}) do not have the same parent key designation.");
                }

                var myDepth = keyleveldepth.Where(x => x.Value.Contains(binding.Id)).Select(x => x.Key)
                    .FirstOrDefault();
                var myColumn = singleResult.First();

                // TODO: check if column actually exists in source binding
                newSqlString = this.GetKeyLevelSql(myColumn, currentSqlString, myDepth);
            }


            var parentObjectReferences = this.GetParentObjectRelationships(binding, bindings);
            foreach (var bindingReference in parentObjectReferences)
            {
                var depth = keyleveldepth.Where(x => x.Value.Contains(bindingReference.ParentObjectId))
                    .Select(x => x.Key).FirstOrDefault();
                var column = this.GetAttributeValueFromObjectReference(
                    new ObjectReference { AttributeValues = bindingReference.AttributeValues },
                    "ChildKeyFields");

                // TODO: check if column actually exists in source binding
                newSqlString = this.GetKeyLevelSql(column, newSqlString, depth);
            }

            return newSqlString;
        }
        
        /// <summary>
        /// The get key level sql.
        /// </summary>
        /// <param name="keyFieldsString">
        /// The key fields string.
        /// </param>
        /// <param name="originalSql">
        /// The original sql.
        /// </param>
        /// <param name="depth">
        /// The depth.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetKeyLevelSql(string keyFieldsString, string originalSql, int depth)
        {
            var convertedToArray = keyFieldsString.Replace("[", string.Empty).Replace("]", string.Empty).Replace('"', ' ').Split(',');
            if (convertedToArray.Length > 1)
            {
                // gotta concatenate
                return originalSql.Replace("SELECT ", $"SELECT CONCAT({string.Join(",'-',", convertedToArray.Select(x => x.Trim()))}) AS KeyLevel{depth}, ");
            }

            return originalSql.Replace("SELECT ", $"SELECT {convertedToArray[0].Trim()} AS KeyLevel{depth}, ");
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
            return this.GetAttributeValueFromObjectReference(objectReference, "Cardinality");
        }

        /// <summary>
        /// The get attribute value from object reference.
        /// </summary>
        /// <param name="objectReference">
        /// The object reference.
        /// </param>
        /// <param name="attributeName">
        /// The attribute name.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            return objectReference.AttributeValues.Where(x => x.AttributeName == attributeName)
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
        private List<ObjectReference> GetChildObjectRelationships(Binding binding)
        {
            var bindingRelationship = binding.ObjectRelationships.Where(x => x.ChildObjectType == "Binding").ToList();
            return bindingRelationship;
        }

        /// <summary>
        /// The get parent object relationships.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="allBindings">
        /// The other bindings.
        /// </param>
        /// <returns>
        /// The <see cref="List"/>.
        /// </returns>
        private List<BindingReference> GetParentObjectRelationships(Binding binding, Binding[] allBindings)
        {
            var bindingRelationships = new List<BindingReference>();
            foreach (var otherBinding in allBindings)
            {
                bindingRelationships.AddRange(
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

            return bindingRelationships;
        }

        /// <summary>
        /// The get all object relationships.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="otherBindings">
        /// The other bindings.
        /// </param>
        /// <returns>
        /// The <see cref="List"/>.
        /// </returns>
        private List<BindingReference> GetAllObjectRelationships(Binding binding, Binding[] otherBindings)
        {
            var bindingRelationships = new List<BindingReference>();
            bindingRelationships.AddRange(
                this.GetChildObjectRelationships(binding).Select(
                    x => new BindingReference
                             {
                                 ChildObjectId = x.ChildObjectId,
                                 AttributeValues = x.AttributeValues,
                                 ChildObjectType = x.ChildObjectType,
                                 ParentObjectId = binding.Id
                             }));

            bindingRelationships.AddRange(this.GetParentObjectRelationships(binding, otherBindings));
            return bindingRelationships;
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
                var columns = "*";
                if (entity.Fields != null && entity.Fields.Count > 0)
                {
                    columns = string.Join(", ", entity.Fields.Where(x => x.Status != FieldStatus.Omitted).Select(x => x.FieldName));
                }

                return $"select {columns} from [{this.GetDatabaseNameFromEntity(entity)}].[{this.GetSchemaNameFromEntity(entity)}].[{this.GetTableNameFromEntity(entity)}]";
            }

            return null;
        }
    }
}
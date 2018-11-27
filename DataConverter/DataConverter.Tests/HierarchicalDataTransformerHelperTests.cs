// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HierarchicalDataTransformerHelperTests.cs" company="Health Catalyst">
//   Copyright Health Catalyst 2018
// </copyright>
// <summary>
//   The hierarchical data transformer tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UnitTestProject1
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Dynamic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using DataConverter;

    using Fabric.Databus.Config;

    using Moq;

    using Xunit;

    /// <summary>
    /// The hierarchical data transformer tests.
    /// </summary>
    public class HierarchicalDataTransformerHelperTests
    {
        /// <summary>
        /// The mock meta data service.
        /// </summary>
        private Mock<IMetadataServiceClient> mockMetaDataService;

        /// <summary>
        /// The mock helper.
        /// </summary>
        private Mock<IHierarchicalDataTransformerHelper> mockHelper;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformerHelperTests"/> class.
        /// </summary>
        public HierarchicalDataTransformerHelperTests()
        {
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
        }

        /// <summary>
        /// The run data bus.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        [Fact]
        public async Task GIVEN_EmptyBinding_WHEN_GenerateDataModel_THEN_ReturnsExpected()
        {
            // set this up
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
            for (int i = 0; i < 10; i++)
            {
                this.SetupGetEntity(i);
            }
            var parentBinding = new Binding();
            IHierarchicalDataTransformerHelper helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(parentBinding, new Binding[0], out depthMap);

            Assert.Equal("{}", model);
        }

        /// <summary>
        /// The run data bus.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        [Fact]
        public async Task GIVEN_SimpleBinding_WHEN_GenerateDataModel_THEN_ReturnsExpected()
        {
            // set this up
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
            for (int i = 0; i < 10; i++)
            {
                this.SetupGetEntity(i);
            }
            var parentBinding = this.GetSimpleParentBinding();
            var bindings = this.GetSimpleBindings();
            IHierarchicalDataTransformerHelper helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(parentBinding, bindings, out depthMap);

            Assert.Equal("{\"MyEntity_2\":{},\"MyEntity_3\":{}}", model);
        }

        /// <summary>
        /// The run data bus.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        [Fact]
        public async Task GIVEN_NestedBinding_WHEN_GenerateDataModel_THEN_ReturnsExpected()
        {
            // set this up
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
            for (int i = 0; i < 10; i++)
            {
                this.SetupGetEntity(i);
            }
            var parentBinding = this.GetNestedParentBinding();
            var bindings = this.GetNestedBindings();
            IHierarchicalDataTransformerHelper helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(parentBinding, bindings, out depthMap);

            Assert.Equal("{\"MyEntity_3\":{\"MyEntity_4\":{}}}", model);

        }

        /// <summary>
        /// The run data bus.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        [Fact]
        public async Task GIVEN_NestedBindingWithArray_WHEN_GenerateDataModel_THEN_ReturnsExpected()
        {
            // set this up
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
            for (int i = 0; i < 10; i++)
            {
                this.SetupGetEntity(i);
            }
            var parentBinding = this.GetNestedParentArrayBinding();
            var bindings = this.GetNestedBindings();
            IHierarchicalDataTransformerHelper helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(parentBinding, bindings, out depthMap);
            

            Assert.Equal("{\"MyEntity_2\":[{\"MyEntity_3\":{\"MyEntity_4\":{}}}]}", model);
        }

        [Fact]
        public async Task CanCreateExampleDataModel()
        {
            var exampleModelParentBinding = this.GetExampleModelParentBinding();
            var exampleModelBindings = this.GetExampleModelBindings();
            this.SetupExampleModelGetEntity();
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(exampleModelParentBinding, exampleModelBindings, out depthMap);
            Assert.Equal("{\"identifier\":[],\"name\":[],\"communication\":[],\"us-core-race\":{\"extension\":[]},\"condition\":[{\"category\":{},\"code\":{}}]}", model);
        }

        [Fact]
        public async Task GetConfigReturnsConfig()
        {
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var config = await helper.GetConfig();

            Assert.Equal("server=HC2260;initial catalog=EDWAdmin;Trusted_Connection=True;", config.ConnectionString);
            Assert.Equal("https://HC2260.hqcatalyst.local/DataProcessingService/v1/BatchExecutions", config.Url);
            Assert.Equal(1000, config.MaximumEntitiesToLoad);
            Assert.Equal(100, config.EntitiesPerBatch);
            Assert.Equal(100, config.EntitiesPerUploadFile);
            Assert.Equal("C:\\Catalyst\\databus", config.LocalSaveFolder);
            Assert.False(config.DropAndReloadIndex);
            Assert.True(config.WriteTemporaryFilesToDisk);
            Assert.False(config.CompressFiles);
            Assert.True(config.UploadToElasticSearch);
            Assert.Equal("Patients2", config.Index);
            Assert.Equal("patient", config.EntityType);
            Assert.Equal("KeyLevel0", config.TopLevelKeyColumn);
            Assert.False(config.UseMultipleThreads);
            Assert.True(config.KeepTemporaryLookupColumnsInOutput);
        }

        [Fact]
        public async Task KeyLevelsWorkWithMiddleChildBinding()
        {
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);

            // Parent => 1; Child => 2; Grandchild => 3
            var parent = new Binding
                             {
                                 Id = 1,
                                 ObjectRelationships =
                                     {
                                         new ObjectReference
                                             {
                                                 ChildObjectId = 2,
                                                 AttributeValues =
                                                     {
                                                         new
                                                         ObjectAttributeValue
                                                             {
                                                                 AttributeName
                                                                     = "ChildKeyFields",
                                                                 AttributeValue
                                                                     = "[ \"p1\" ]"
                                                             },
                                                         new
                                                         ObjectAttributeValue
                                                             {
                                                                 AttributeName
                                                                     = "ParentKeyFields",
                                                                 AttributeValue
                                                                     = "[ \"p1\" ]"
                                                             },
                                                         new ObjectAttributeValue { AttributeName = "GenerationGap", AttributeValue = "1" }
                                                     },
                                                 ChildObjectType = "Binding"
                                             },
                                 new ObjectReference
                                     {
                                         ChildObjectId = 3,
                                         AttributeValues =
                                             {
                                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[ \"p1\" ]" },
                                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[ \"p1\" ]" },
                                                                   new ObjectAttributeValue { AttributeName = "GenerationGap", AttributeValue = "2" }
                                             },
                                         ChildObjectType = "Binding"
                                     }
                             }
                             };
            var child = new Binding
                            {
                                Id = 2,
                                ObjectRelationships =
                                    {
                                        new ObjectReference
                                            {
                                                ChildObjectId = 3,
                                                AttributeValues =
                                                    {
                                                        new
                                                        ObjectAttributeValue
                                                            {
                                                                AttributeName
                                                                    = "ChildKeyFields",
                                                                AttributeValue
                                                                    = "[ \"c1\", \"c2\" ]"
                                                            },
                                                        new
                                                        ObjectAttributeValue
                                                            {
                                                                AttributeName
                                                                    = "ParentKeyFields",
                                                                AttributeValue
                                                                    = "[ \"c1\", \"c2\" ]"
                                                            },
                                                        new ObjectAttributeValue { AttributeName = "GenerationGap", AttributeValue = "1" }
                                                    },
                                                ChildObjectType = "Binding"
                                            }
                                    }
                            };
            var grandchild = new Binding { Id = 3 };
            var bindings = new List<Binding> { parent, child, grandchild };
            var keyleveldepth = new Dictionary<int, List<int>>
                                    {
                                        { 1, new List<int> { 1 } },
                                        { 2, new List<int> { 2 } },
                                        { 3, new List<int> { 3 } }
                                    };
            var parentSql = "SELECT p1, p2, Name, Address, Etc FROM [Database].[Schema].[Table]";
            var childSql = "SELECT p1, c1, c2, Name, Address, Etc FROM [Database].[Schema].[Table]";
            var grandchildSql = "SELECT c1, c2, g1, g2, Name, Address, Etc FROM [Database].[Schema].[Table]";

            var entity = new Entity
                             {
                                 Fields =
                                     {
                                         new Field { FieldName = "p1" },
                                         new Field { FieldName = "p2" },
                                         new Field { FieldName = "Name" },
                                         new Field { FieldName = "Address" },
                                         new Field { FieldName = "Etc" },
                                         new Field { FieldName = "c1" },
                                         new Field { FieldName = "c2" },
                                         new Field { FieldName = "g1" },
                                         new Field { FieldName = "g2" },
                                     }
                             };

            var parentActual = helper.AddKeyLevels(parentSql, keyleveldepth, parent, bindings.ToArray(), entity);
            var childActual = helper.AddKeyLevels(childSql, keyleveldepth, child, bindings.ToArray(), entity);
            var grandchildActual = helper.AddKeyLevels(grandchildSql, keyleveldepth, grandchild, bindings.ToArray(), entity);

            Assert.Equal("SELECT p1 AS KeyLevel1, p1, p2, Name, Address, Etc FROM [Database].[Schema].[Table]", parentActual);
            Assert.Equal("SELECT p1 AS KeyLevel1, CONCAT(c1,'-',c2) AS KeyLevel2, p1, c1, c2, Name, Address, Etc FROM [Database].[Schema].[Table]", childActual);
            Assert.Equal("SELECT CONCAT(c1,'-',c2) AS KeyLevel2, p1 AS KeyLevel1, c1, c2, g1, g2, Name, Address, Etc FROM [Database].[Schema].[Table]", grandchildActual);
        }

        [Fact]
        public async Task KeyLevelsWorkSimple()
        {
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);

            // Parent => 1; Child => 2
            var parentBinding = new Binding
                                    {
                                        Id = 1,
                                        ObjectRelationships =
                                            {
                                                new ObjectReference
                                                    {
                                                        ChildObjectId = 2,
                                                        AttributeValues =
                                                            {
                                                                new
                                                                ObjectAttributeValue
                                                                    {
                                                                        AttributeName
                                                                            = "ChildKeyFields",
                                                                        AttributeValue
                                                                            = "[ \"bar\" ]"
                                                                    },
                                                                new
                                                                ObjectAttributeValue
                                                                    {
                                                                        AttributeName
                                                                            = "ParentKeyFields",
                                                                        AttributeValue
                                                                            = "[ \"foo\" ]"
                                                                    }
                                                            },
                                                        ChildObjectType = "Binding"
                                                    }
                                            }
                                    };
            var childBinding = new Binding { Id = 2 };

            var bindings = new List<Binding> { parentBinding, childBinding };

            var parentSql = "SELECT foo, Name, Address, Etc FROM [Database].[Schema].[Table]";
            var childSql = "SELECT bar, Name, Address, Etc FROM [Database].[Schema].[Table]";

            var keyleveldepth = new Dictionary<int, List<int>>
                                    {
                                        { 1, new List<int> { 1 } },
                                        { 2, new List<int> { 2 } }
                                    };

            var entity = new Entity
                             {
                                 Fields =
                                     {
                                         new Field { FieldName = "foo" },
                                         new Field { FieldName = "bar" },
                                         new Field { FieldName = "Name" },
                                         new Field { FieldName = "Address" },
                                         new Field { FieldName = "Etc" },
                                     }
                             };
            var parentActual = helper.AddKeyLevels(parentSql, keyleveldepth, parentBinding, bindings.ToArray(), entity);
            var childActual = helper.AddKeyLevels(childSql, keyleveldepth, childBinding, bindings.ToArray(), entity);

            Assert.Equal("SELECT foo AS KeyLevel1, foo, Name, Address, Etc FROM [Database].[Schema].[Table]", parentActual);
            Assert.Equal("SELECT bar AS KeyLevel1, bar, Name, Address, Etc FROM [Database].[Schema].[Table]", childActual);
        }

        [Fact]
        public async Task RunDataTransformer()
        {
            var exampleModelParentBinding = this.GetExampleModelParentBinding();
            var exampleModelBindings = this.GetExampleModelBindings();
            this.SetupExampleModelGetEntity();
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);


            var bindingsForEntity = exampleModelBindings.ToList();
            bindingsForEntity.Add(exampleModelParentBinding);
            var bindingsArray = bindingsForEntity.ToArray();
            var depthMap = new Dictionary<int, List<int>>();
            var model = helper.GenerateDataModel(exampleModelParentBinding, bindingsArray, out depthMap);
            Assert.Equal("{\"identifier\":[],\"name\":[],\"communication\":[],\"us-core-race\":{\"extension\":[]},\"condition\":[{\"category\":{},\"code\":{}}]}", model);


            var dataSources = await helper.GetDataSources(exampleModelParentBinding, bindingsArray, new List<DataSource>(), depthMap);             
        }

        #region helper methods
        private void SetupExampleModelGetEntity()
        {
            this.mockMetaDataService = new Mock<IMetadataServiceClient>();
            var entities = this.GetExampleModelEntities();
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(1)).Returns(Task.FromResult(entities[0]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(2)).Returns(Task.FromResult(entities[1]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(3)).Returns(Task.FromResult(entities[2]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(4)).Returns(Task.FromResult(entities[3]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(5)).Returns(Task.FromResult(entities[4]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(6)).Returns(Task.FromResult(entities[5]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(7)).Returns(Task.FromResult(entities[6]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(8)).Returns(Task.FromResult(entities[7]));
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(9)).Returns(Task.FromResult(entities[8]));
        }

        private Entity[] GetExampleModelEntities()
        {
            var entities = new Entity[9];

            entities[8] = new Entity
                              {
                                  EntityName = "patient",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBase"
                                              }
                                      },
                                  Id = 9,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "CASE GenderCD WHEN 'Female' THEN 'female' WHEN 'Male' THEN 'male' ELSE 'unknown' END AS gender" },
                                          new Field { FieldName = "BirthDTS as birthDate" }
                                      }
                              };

            // TODO: NEED TO THINK ABOUT UNION JOINS
            entities[0] = new Entity
                              {
                                  EntityName = "identifier",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              },
                                      },
                                  Id = 1,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "'usual' as [use]" },
                                          new Field { FieldName = "'http://www.healthcatalyst.com' as system" },
                                          new Field { FieldName = "MRN as value" },
                                          new Field { FieldName = "'MR' as type" }
                                      }
            };
            entities[1] = new Entity
                              {
                                  EntityName = "name",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              },
                                      },
                                  Id = 2,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "PatientLastNM as family" },
                                          new Field { FieldName = "PatientFirstNM as given" },
                                          new Field { FieldName = "PatientFullNM as text" }
                                      }
            };

            // TODO: THINK ABOUT UNION JOINS
            entities[2] = new Entity
                              {
                                  EntityName = "communication",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              }
                                      },
                                  Id = 3,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "LanguageDSC as language" }
                                      }
                              };
            entities[3] = new Entity
                              {
                                  EntityName = "us-core-race",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              }
                                      },
                                  Id = 4,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race' as url" }
                                      }
            };
            entities[4] = new Entity
                              {
                                  EntityName = "extension",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              }
                                      },
                                  Id = 5,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "'ombCategory' as url" },
                                          new Field { FieldName = @"CASE RaceDSC WHEN 'Declined' THEN '' WHEN 'American Indian or Alaskan Native' THEN '1002-5' WHEN 'Other' THEN '' WHEN 'Black or African American' THEN '2054-5' WHEN 'Native Hawaiian or Other Pacific Islander' THEN '2076-8' WHEN 'White or Caucasian' THEN '2106-3' WHEN 'Two Races' THEN '' WHEN 'Asian' THEN '2028-9' ELSE '' END AS valueCoding" },
                                      }
            };
            entities[5] = new Entity
                              {
                                  EntityName = "condition",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Person"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "SourcePatientBASE"
                                              }
                                      },
                                  Id = 6,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "'ombCategory' as url" },
                                          new Field { FieldName = @"CASE RaceDSC WHEN 'Declined' THEN '' WHEN 'American Indian or Alaskan Native' THEN '1002-5' WHEN 'Other' THEN '' WHEN 'Black or African American' THEN '2054-5' WHEN 'Native Hawaiian or Other Pacific Islander' THEN '2076-8' WHEN 'White or Caucasian' THEN '2106-3' WHEN 'Two Races' THEN '' WHEN 'Asian' THEN '2028-9' ELSE '' END AS valueCoding" },
                                      }
            };
            entities[6] = new Entity
                              {
                                  EntityName = "category",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Clinical"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "DiagnosisBASE"
                                              }
                                      },
                                  Id = 7,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "DiagnosisID" },
                                          new Field { FieldName = "RowSourceDSC" },
                                          new Field { FieldName = @"CASE DiagnosisTypeDSC WHEN 'ICD Problem List Code' THEN 'problem-list-item' WHEN 'ICD Primary Diagnosis Code' THEN 'problem' WHEN 'ICD Diagnosis Cod' THEN 'problem' WHEN 'ICD Admit Diagnosis Code' THEN 'encounter-diagnosis' ELSE 'health-concern' END AS coding" },
                                          new Field { FieldName = "DiagnosisTypeDSC as text" },
                                      }
            };
            entities[7] = new Entity
                              {
                                  EntityName = "code",
                                  AttributeValues =
                                      {
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "DatabaseName",
                                                  AttributeValue = "EDWAdmin"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "SchemaName",
                                                  AttributeValue = "Clinical"
                                              },
                                          new ObjectAttributeValue
                                              {
                                                  AttributeName = "TableName",
                                                  AttributeValue = "DiagnosisBASE"
                                              }
                                      },
                                  Id = 8,
                                  Fields =
                                      {
                                          new Field { FieldName = "PatientID" },
                                          new Field { FieldName = "DiagnosisID" },
                                          new Field { FieldName = "RowSourceDSC" },
                                          new Field { FieldName = @"CASE CodeTypeCD WHEN 'ICD9DX' THEN 'http://hl7.org/fhir/sid/icd-9-cm' WHEN 'ICD10DX' THEN 'http://hl7.org/fhir/sid/icd-10-cm' ELSE NULL END AS system" },
                                          new Field { FieldName = "DiagnosisCD as code" },
                                          new Field { FieldName = "DiagnosisTypeDSC as text" },
                                      }
            };
            return entities;
        }

        private Binding[] GetExampleModelBindings()
        {
            var bindings = new Binding[8];
            bindings[0] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 1, SourceAliasName = "identifier" } },
                                  Id = 1
            };
            bindings[1] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 2, SourceAliasName = "name" } },
                                  Id = 2
            };
            bindings[2] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "communication" } },
                                  Id = 3
            };
            bindings[3] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 4, SourceAliasName = "us-core-race" } },
                                  Id = 4,
                                  ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 5,
                                                  ChildObjectType = "Binding",
                                                  AttributeValues =
                                                      {
                                                          new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" },
                                                          new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                          new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                      }
                                              }
                                      }
            };
            bindings[4] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 5, SourceAliasName = "extension" } },
                                  Id = 5
            };
            bindings[5] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 6, SourceAliasName = "condition" } },
                                  Id = 6,
                                  ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 7,
                                                  ChildObjectType = "Binding",
                                                  AttributeValues =
                                                      {
                                                          new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" },
                                                          new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"DiagnosisID\", \"RowSourceDSC\", \"DiagnosisTypeDSC\"]" },
                                                          new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"DiagnosisID\", \"RowSourceDSC\", \"DiagnosisTypeDSC\"]" },
                                                      }
                                              },
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 8,
                                                  ChildObjectType = "Binding",
                                                  AttributeValues =
                                                      {
                                                          new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" },
                                                          new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"DiagnosisID\", \"RowSourceDSC\", \"DiagnosisTypeDSC\"]" },
                                                          new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"DiagnosisID\", \"RowSourceDSC\", \"DiagnosisTypeDSC\"]" }
                                                      }
                                              }
                                      },
            };
            bindings[6] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 7, SourceAliasName = "category" } },
                                  Id = 7
            };
            bindings[7] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 8, SourceAliasName = "code" } },
                                  Id = 8
            };
            return bindings;
        }

        private Binding GetExampleModelParentBinding()
        {
            return new Binding
                       {
                           SourcedByEntities
                               =
                                   {
                                       new SourceEntityReference
                                           {
                                               SourceEntityId = 9,
                                               SourceAliasName = "patient"
                                           }
                                   },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId = 1,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" },
                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" }
                                               }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 2,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" },
                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" }
                                               }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 3,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" },
                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" }
                                               }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 4,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" },
                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" }
                                               }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 6,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" },
                                                   new ObjectAttributeValue { AttributeName = "ParentKeyFields", AttributeValue = "[\"PatientID\"]" },
                                                   new ObjectAttributeValue { AttributeName = "ChildKeyFields", AttributeValue = "[\"PatientID\"]" }
                                               }
                                       }
                               }
                       };
        }

        /// <summary>
        /// The setup bindings.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding[]"/>.
        /// </returns>
        private Binding[] GetSimpleBindings()
        {
            // Mocks
            Binding[] bindings = new Binding[5];
            bindings[0] = new Binding
            {
                BindingType = "Nested",
                Id = 1,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 2, SourceAliasName = "patient" } },
                ObjectRelationships = { new ObjectReference { AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } } } }
            };
            bindings[1] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "identifier" } },
                ObjectRelationships = { new ObjectReference { AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } } } }
            };
            return bindings;
        }

        /// <summary>
        /// The setup bindings.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding[]"/>.
        /// </returns>
        private Binding[] GetNestedBindings()
        {
            // Mocks
            Binding[] bindings = new Binding[5];
            bindings[0] = new Binding
            {
                BindingType = "Nested",
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 2, SourceAliasName = "patient" } },
                Id = 1,
                ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 2,
                                                  ChildObjectType = "Binding",
                                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } }
                                              }
                                      }
            };
            bindings[1] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "identifier" } },
                ObjectRelationships =
                    {
                        new ObjectReference
                            {
                                ChildObjectId = 3,
                                ChildObjectType = "Binding",
                                AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } }
                            }
                    }
            };
            bindings[2] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 3,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 4, SourceAliasName = "identifier" } },
                ObjectRelationships = { new ObjectReference { AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } } } }
            };
            return bindings;
        }

        /// <summary>
        /// The get parent binding.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding"/>.
        /// </returns>
        private Binding GetSimpleParentBinding()
        {
            return new Binding
                       {
                           SourcedByEntities
                               =
                                   {
                                       new SourceEntityReference
                                           {
                                               SourceEntityId = 1,
                                               SourceAliasName = "patient"
                                           }
                                   },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId = 1,
                                           ChildObjectType = "Binding",
                                           AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 2,
                                           ChildObjectType = "Binding",
                                           AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } }
                                       }
                               }
                       };
        }

        /// <summary>
        /// The get parent Binding.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding"/>.
        /// </returns>
        private Binding GetNestedParentBinding()
        {
            return new Binding
                       {
                           SourcedByEntities
                               =
                                   {
                                       new SourceEntityReference
                                           {
                                               SourceEntityId = 1,
                                               SourceAliasName = "patient"
                                           }
                                   },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId = 2,
                                           ChildObjectType = "Binding",
                                           AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "SingleObject" } } 
                                       }
                                   
                               }
                       };
        }

        /// <summary>
        /// The get parent Binding.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding"/>.
        /// </returns>
        private Binding GetNestedParentArrayBinding()
        {
            return new Binding
                       {
                           SourcedByEntities
                               =
                                   {
                                       new SourceEntityReference
                                           {
                                               SourceEntityId = 1,
                                               SourceAliasName = "patient"
                                           }
                                   },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId = 1,
                                           ChildObjectType = "Binding",
                                           AttributeValues = { new ObjectAttributeValue { AttributeName = "Cardinality", AttributeValue = "Array" } }
                                       }
                                   
                               }
                       };
        }

        /// <summary>
        /// The get an entity.
        /// </summary>
        /// <param name="entityId">
        /// int entityId
        /// </param>
        private void SetupGetEntity(int entityId)
        {
            var entity = this.GetEntity(entityId);
            
            this.mockMetaDataService.Setup(x => x.GetEntityAsync(entityId)).Returns(Task.FromResult(entity));
        }

        /// <summary>
        /// The get entity.
        /// </summary>
        /// <param name="entityId">
        /// int entityId
        /// </param>
        /// <returns>
        /// The <see cref="Entity"/>.
        /// </returns>
        private Entity GetEntity(int entityId)
        {
            return new Entity
            {
                EntityName = $"MyEntity_{entityId}",
                AttributeValues =
                        {
                            new ObjectAttributeValue
                                {
                                    AttributeName =
                                        "DatabaseName",
                                    AttributeValue = "MyDatabase"
                                },
                            new ObjectAttributeValue
                                {
                                    AttributeName = "SchemaName",
                                    AttributeValue = "MySchema"
                                },
                            new ObjectAttributeValue
                                {
                                    AttributeName = "TableName",
                                    AttributeValue = $"MyTable_{entityId}"
                                },
                        }
            };
        }

        #endregion

        //var parentBinding = new Binding
        //                        {
        //                            Id = 1,
        //                            ObjectRelationships =
        //                                {
        //                                    new ObjectReference
        //                                        {
        //                                            ChildObjectId = 2,
        //                                            AttributeValues =
        //                                                {
        //                                                    new
        //                                                    ObjectAttributeValue
        //                                                        {
        //                                                            AttributeName
        //                                                                = "ChildKeyFields",
        //                                                            AttributeValue
        //                                                                = "[ \"bar\" ]"
        //                                                        },
        //                                                    new
        //                                                    ObjectAttributeValue
        //                                                        {
        //                                                            AttributeName
        //                                                                = "ParentKeyFields",
        //                                                            AttributeValue
        //                                                                = "[ \"foo\" ]"
        //                                                        }
        //                                                },
        //                                            ChildObjectType =
        //                                                "Binding"
        //                                        }
        //                                }
        //                        };
        //var childBinding = new Binding { Id = 2 };
        //var bindings = new List<Binding> { parentBinding, childBinding };
        //this.mockMetaDataService = new Mock<IMetadataServiceClient>();
        //this.mockMetaDataService.Setup(x => x.GetBindingsForEntityAsync(It.IsAny<int>())).Returns(Task.FromResult(bindings.ToArray()));
        //var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
        //var result = await helper.TransformDataAsync(parentBinding, new Entity { Id = 1 });
    }
}

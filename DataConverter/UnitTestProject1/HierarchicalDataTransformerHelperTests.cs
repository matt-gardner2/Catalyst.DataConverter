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
    using System.Dynamic;
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
            var model = await helper.GenerateDataModel(parentBinding, new Binding[0]);

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
            var model = await helper.GenerateDataModel(parentBinding, bindings);

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
            var model = await helper.GenerateDataModel(parentBinding, bindings);

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
            var model = await helper.GenerateDataModel(parentBinding, bindings);

            Assert.Equal("{\"MyEntity_2\":[{\"MyEntity_3\":{\"MyEntity_4\":{}}}]}", model);
        }

        [Fact]
        public async Task CanCreateExampleDataModel()
        {
            var exampleModelParentBinding = this.GetExampleModelParentBinding();
            var exampleModelBindings = this.GetExampleModelBindings();
            this.SetupExampleModelGetEntity();
            var helper = new HierarchicalDataTransformerHelper(this.mockMetaDataService.Object);
            var model = await helper.GenerateDataModel(exampleModelParentBinding, exampleModelBindings);
            Assert.Equal("{\"identifier\":[],\"name\":[],\"communication\":[],\"us-core-race\":{\"extension\":[]},\"condition\":[{\"category\":{},\"code\":{}}]}", model);
        }

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
                                                  AttributeValue = "patientTable"
                                              },
                                      },
                                  Id = 9
                              };
            entities[0] = new Entity
                              {
                                  EntityName = "identifier",
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
                                                  AttributeValue = "identifierTable"
                                              },
                                      },
                                  Id = 1
                              };
            entities[1] = new Entity
                              {
                                  EntityName = "name",
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
                                                  AttributeValue = "nameTable"
                                              },
                                      },
                                  Id = 2
                              };
            entities[2] = new Entity
                              {
                                  EntityName = "communication",
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
                                                  AttributeValue = "communicationTable"
                                              },
                                      },
                                  Id = 3
                              };
            entities[3] = new Entity
                              {
                                  EntityName = "us-core-race",
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
                                                  AttributeValue = "raceTable"
                                              },
                                      },
                                  Id = 4
                              };
            entities[4] = new Entity
                              {
                                  EntityName = "extension",
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
                                                  AttributeValue = "communicationExtensionTable"
                                              },
                                      },
                                  Id = 5
                              };
            entities[5] = new Entity
                              {
                                  EntityName = "condition",
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
                                                  AttributeValue = "conditionTable"
                                              },
                                      },
                                  Id = 6
                              };
            entities[6] = new Entity
                              {
                                  EntityName = "category",
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
                                                  AttributeValue = "categoryTable"
                                              },
                                      },
                                  Id = 7
                              };
            entities[7] = new Entity
                              {
                                  EntityName = "code",
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
                                                  AttributeValue = "codeTable"
                                              },
                                      },
                                  Id = 8
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
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                                  Id = 1
                              };
            bindings[1] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 2, SourceAliasName = "name" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                                  Id = 2
                              };
            bindings[2] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "communication" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                                  Id = 3
                              };
            bindings[3] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 4, SourceAliasName = "us-core-race" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
                                  Id = 4,
                                  ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 5,
                                                  ChildObjectType = "binding"
                                              }
                                      }
            };
            bindings[4] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 5, SourceAliasName = "extension" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                                  Id = 5
                              };
            bindings[5] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 6, SourceAliasName = "condition" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                                  Id = 6,
                                  ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 7,
                                                  ChildObjectType = "binding"
                                              },
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 8,
                                                  ChildObjectType = "binding"
                                              }
                                      }
            };
            bindings[6] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 7, SourceAliasName = "category" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
                                  Id = 7
            };
            bindings[7] = new Binding
                              {
                                  BindingType = "Nested",
                                  SourcedByEntities = { new SourceEntityReference { SourceEntityId = 8, SourceAliasName = "code" } },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
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
                                           ChildObjectType = "binding"
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 2,
                                           ChildObjectType = "binding"
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 3,
                                           ChildObjectType = "binding"
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 4,
                                           ChildObjectType = "binding"
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 6,
                                           ChildObjectType = "binding"
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
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 2, SourceAliasName = "patient" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
                Id = 1
            };
            bindings[1] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "identifier" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
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
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
                Id = 1,
                ObjectRelationships =
                                      {
                                          new ObjectReference
                                              {
                                                  ChildObjectId = 2,
                                                  ChildObjectType = "binding"
                                              }
                                      }
            };
            bindings[1] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 3, SourceAliasName = "identifier" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
                ObjectRelationships =
                    {
                        new ObjectReference
                            {
                                ChildObjectId = 3,
                                ChildObjectType = "binding"
                            }
                    }
            };
            bindings[2] = new Binding
            {
                // Name = "identifier",
                BindingType = "Nested",
                Id = 3,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 4, SourceAliasName = "identifier" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Object" } },
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
                                           ChildObjectType = "binding"
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId = 2,
                                           ChildObjectType = "binding"
                                       }
                               }
                       };
        }

        /// <summary>
        /// The get parent binding.
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
                                           ChildObjectType = "binding"
                                       }
                                   
                               }
                       };
        }

        /// <summary>
        /// The get parent binding.
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
                                           ChildObjectType = "binding"
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
    }
}

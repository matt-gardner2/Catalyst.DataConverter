// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HierarchicalDataTransformerTests.cs" company="Health Catalyst">
//   Copyright Health Catalyst 2018
// </copyright>
// <summary>
//   The hierarchical data transformer tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UnitTestProject1
{
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using DataConverter;

    using Moq;

    using Xunit;

    /// <summary>
    /// The hierarchical data transformer tests.
    /// </summary>
    public class HierarchicalDataTransformerTests
    {
        /// <summary>
        /// The mock meta data service.
        /// </summary>
        private Mock<IMetadataServiceClient> mockMetaDataService;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformerTests"/> class.
        /// </summary>
        public HierarchicalDataTransformerTests()
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
        public async Task RunDataBus()
        {
            this.SetupBindings();
            this.SetupGetEntity();
            var parentBinding = this.GetParentBinding();
            var entity = this.GetEntity();
            
            var systemUnderTest = new HierarchicalDataTransformer(this.mockMetaDataService.Object);
            await systemUnderTest.TransformDataAsync(null, parentBinding, entity, new CancellationToken(false));
        }

        /// <summary>
        /// The setup bindings.
        /// </summary>
        private void SetupBindings()
        {
            // Mocks
            Binding[] bindings = new Binding[5];
            bindings[0] = new Binding
            {
                BindingType = "Nested",
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 1, SourceAliasName = "patient" } },
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
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 532, SourceAliasName = "identifier" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
            };
            bindings[2] = new Binding
            {
                // Name = "name",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 23, SourceAliasName = "name" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
            };
            bindings[3] = new Binding
            {
                // Name = "communication",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 25, SourceAliasName = "communication" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
            };
            bindings[4] = new Binding
            {
                // Name = "us-core-race",
                BindingType = "Nested",
                Id = 2,
                SourcedByEntities = { new SourceEntityReference { SourceEntityId = 22, SourceAliasName = "us-core-race" } },
                AttributeValues = { new ObjectAttributeValue { AttributeName = "JSONPropertyType", AttributeValue = "Array" } },
            };
            this.mockMetaDataService.Setup(x => x.GetBindingsForEntityAsync(It.IsAny<int>())).Returns(Task.FromResult(bindings));
        }

        /// <summary>
        /// The get parent binding.
        /// </summary>
        /// <returns>
        /// The <see cref="Binding"/>.
        /// </returns>
        private Binding GetParentBinding()
        {
            return new Binding
                                        {
                                            SourcedByEntities = { new SourceEntityReference { SourceEntityId = 1, SourceAliasName = "patient" } },
                                            ObjectRelationships =
                                                {
                                                    new ObjectReference
                                                        {
                                                            ChildObjectId = 1,
                                                            ChildObjectType =
                                                                "binding"
                                                        }
                                                }
                                        };
        }

        /// <summary>
        /// The get an entity.
        /// </summary>
        private void SetupGetEntity()
        {
            var entity = this.GetEntity(); 

            this.mockMetaDataService.Setup(x => x.GetEntityAsync(It.IsAny<int>())).Returns(Task.FromResult(entity));
        }

        /// <summary>
        /// The get entity.
        /// </summary>
        /// <returns>
        /// The <see cref="Entity"/>.
        /// </returns>
        private Entity GetEntity()
        {
            return new Entity
                {
                    EntityName = "MyEntity",
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
                                    AttributeValue = "MyTable"
                                },
                        }
                };
        }
    }
}

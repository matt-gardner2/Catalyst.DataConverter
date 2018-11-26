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
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using DataConverter;

    using Fabric.Databus.Config;

    using Moq;

    using Xunit;

    /// <summary>
    /// The hierarchical data transformer tests.
    /// </summary>
    public class HierarchicalDataTransformerTests
    {
        /// <summary>
        /// The mock helper.
        /// </summary>
        private Mock<IHierarchicalDataTransformerHelper> mockHelper;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformerTests"/> class.
        /// </summary>
        public HierarchicalDataTransformerTests()
        {
        }

        /// <summary>
        /// The run data bus.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        [Fact]
        public async Task TransformDataAsyncRunsExpectedHelperMethods()
        {
            this.mockHelper = new Mock<IHierarchicalDataTransformerHelper>();
            var outParam = new Dictionary<int, List<int>>();
            this.mockHelper.Setup(x => x.GenerateDataModel(It.IsAny<Binding>(), It.IsAny<Binding[]>(), out outParam)).Returns("{}");
            this.mockHelper
                .Setup(x => x.GetDataSources(It.IsAny<Binding>(), It.IsAny<Binding[]>(), It.IsAny<List<DataSource>>(), outParam))
                .Returns(
                    Task.FromResult(
                        new List<DataSource>
                            {
                                new DataSource
                                    {
                                        Sql =
                                            "SELECT 2 [BatchDefinitionId], 'Queued' [Status], 'Batch' [PipelineType]"
                                    }
                            }));
            var systemUnderTest = new HierarchicalDataTransformer(this.mockHelper.Object);
            await systemUnderTest.TransformDataAsync(null, new Binding(), new Entity(), new CancellationToken(false));
            this.mockHelper.Verify(x => x.GetBindingsForEntityAsync(It.IsAny<int>()), Times.Exactly(1));
            this.mockHelper.Verify(x => x.GenerateDataModel(It.IsAny<Binding>(), It.IsAny<Binding[]>(), out outParam), Times.Exactly(1));
            this.mockHelper.Verify(x => x.GetDataSources(It.IsAny<Binding>(), It.IsAny<Binding[]>(), It.IsAny<List<DataSource>>(), outParam), Times.Exactly(1));
            this.mockHelper.Verify(x => x.GetConfig(), Times.Exactly(1));
        }
    }
}

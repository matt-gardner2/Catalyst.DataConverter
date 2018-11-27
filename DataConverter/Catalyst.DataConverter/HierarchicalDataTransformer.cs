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
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using Fabric.Databus.Config;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer.
    /// </summary>
    public class HierarchicalDataTransformer : IDataTransformer
    {
        /// <summary>
        /// The helper.
        /// </summary>
        private readonly IHierarchicalDataTransformerHelper helper;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="metadataServiceClient">
        /// The metadata Service Client.
        /// </param>
        /// <param name="helper">
        /// The helper.
        /// </param>
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient)
        {
            this.helper = new HierarchicalDataTransformerHelper(metadataServiceClient);
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
            Console.WriteLine("We are in the hierarchical data transformer");
            var bindings = await this.helper.GetBindingsForEntityAsync(entity.Id);
            var depthMap = new Dictionary<int, List<int>>();
            var dataModel = this.helper.GenerateDataModel(binding, bindings, out depthMap);
            var dataSources = await this.helper.GetDataSources(binding, bindings, new List<DataSource>(), depthMap);

            // TODO: JobData data = await this.helper.GetJobData();
            QueryConfig config = await this.helper.GetConfig();
            
            var jobData = new JobData { DataModel = dataModel, MyDataSources = dataSources };

            this.helper.RunDatabus(config, jobData);
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
    }
}

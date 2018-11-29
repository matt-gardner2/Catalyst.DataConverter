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
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Config;

    using Newtonsoft.Json;

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
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient)
        {
            this.helper = new HierarchicalDataTransformerHelper(metadataServiceClient);
            LoggingHelper2.Debug("We Got Here: HierarchicalDataTransformer!");
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
            LoggingHelper2.Debug("We are in the hierarchical data transformer");
            LoggingHelper2.Debug("Binding: " + JsonConvert.SerializeObject(binding));
            LoggingHelper2.Debug("Entity: " + JsonConvert.SerializeObject(entity));

            Binding[] allBindings = await this.helper.GetBindingsForEntityAsync(entity);
            Binding topMostBinding = this.helper.GetTopMostBinding(allBindings);

            HierarchicalDataTransformerHelper.DataModelDepthMap dataModel = await this.helper.GenerateDataModel(topMostBinding, allBindings);
            var dataSources = await this.helper.GetDataSources(topMostBinding, allBindings, new List<DataSource>(), dataModel.DepthMap, entity);

            // TODO: JobData data = await this.helper.GetJobData();
            QueryConfig config = await this.helper.GetConfig();
            LoggingHelper2.Debug("QueryConfig: " + JsonConvert.SerializeObject(config));

            var jobData = new JobData { DataModel = dataModel.DataModel, MyDataSources = dataSources };

            LoggingHelper2.Debug("Final Data Model: " + JsonConvert.SerializeObject(dataModel.DataModel));
            LoggingHelper2.Debug("Final Data Sources: " + JsonConvert.SerializeObject(dataSources));

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
            LoggingHelper2.Debug("We got to the CanHandle Method");

            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == "Nested"; // BindingType.
        }
    }
}

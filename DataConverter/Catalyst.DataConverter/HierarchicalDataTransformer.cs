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
            var config = await this.helper.GetConfig();

            var jobData = await this.helper.GetJobData(binding, entity);
            
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

            Binding topMost = null;
            try
            {
                Binding[] allBindings = this.helper.GetBindingsForEntityAsync(destinationEntity).Result;
                topMost = this.helper.GetTopMostBinding(allBindings);
                LoggingHelper2.Debug($"All bindings: {JsonConvert.SerializeObject(allBindings)}");
                LoggingHelper2.Debug($"binding: {JsonConvert.SerializeObject(binding)}");
                LoggingHelper2.Debug($"TopMost: {JsonConvert.SerializeObject(topMost)}");
                LoggingHelper2.Debug($"Is topmost??: {binding.Id == topMost.Id}");
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"Threw exception: {e}");
                throw;
            }


            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == "Nested" 
                   && binding.Id == topMost.Id; // BindingType.
        }
    }
}

// --------------------------------------------------------------------------------------------------------------------
// <copyright file="IHierarchicalDataTransformerHelper.cs" company="Health Catalyst">
//   Health Catalyst Copyright 2018
// </copyright>
// <summary>
//   Defines the IHierarchicalDataTransformer type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------



namespace DataConverter
{
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Text;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Fabric.Databus.Config;

    /// <summary>
    /// The HierarchicalDataTransformerHelper interface.
    /// </summary>
    public interface IHierarchicalDataTransformerHelper
    {

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
        Task<string> GenerateDataModel(Binding binding, Binding[] bindings);

        /// <summary>
        /// The get data sources.
        /// </summary>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="bindings">
        /// The bindings.
        /// </param>
        /// <param name="currentDataSources">
        /// The current data sources.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<List<DataSource>> GetDataSources(Binding binding, Binding[] bindings, List<DataSource> currentDataSources);

        /// <summary>
        /// The get bindings for entity async.
        /// </summary>
        /// <param name="entityId">
        /// The entity id.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<Binding[]> GetBindingsForEntityAsync(int entityId);

        /// <summary>
        /// The run databus.
        /// </summary>
        /// <param name="config">
        /// The config.
        /// </param>
        /// <param name="jobData">
        /// The job data.
        /// </param>
        void RunDatabus(QueryConfig config, JobData jobData);

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
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst, bool isObject);

        /// <summary>
        /// The get config.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<QueryConfig> GetConfig();
    }
}
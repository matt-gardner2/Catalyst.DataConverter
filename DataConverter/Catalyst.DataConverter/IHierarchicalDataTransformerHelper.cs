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
        /// <param name="depthMap">
        /// The depth Map.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        string GenerateDataModel(Binding binding, Binding[] bindings, out Dictionary<int, List<int>> depthMap);

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
        /// <param name="depthMap">
        /// The depth Map.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<List<DataSource>> GetDataSources(Binding binding, Binding[] bindings, List<DataSource> currentDataSources, Dictionary<int, List<int>> depthMap);

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
        /// <param name="depthMap">
        /// The depth Map.
        /// </param>
        /// <param name="depth">
        /// The depth.
        /// </param>
        void GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst, bool isObject,Dictionary<int, List<int>> depthMap, int depth);

        /// <summary>
        /// The get config.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<QueryConfig> GetConfig();

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
        /// <param name="columnsAvailable">
        /// The columns Available.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        string AddKeyLevels(
            string currentSqlString,
            Dictionary<int, List<int>> keyleveldepth,
            Binding binding,
            Binding[] bindings,
            List<string> columnsAvailable);
    }
}
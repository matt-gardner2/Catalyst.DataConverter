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
        Task<HierarchicalDataTransformerHelper.DataModelDepthMap> GenerateDataModel(Binding topMostBinding, Binding[] bindings);

        /// <summary>
        /// Get the top most binding in the hierarchy
        /// </summary>
        /// <param name="bindings"></param>
        /// <returns></returns>
        Binding GetTopMostBinding(Binding[] bindings);

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
        /// <param name="destinationEntity">
        /// The destination Entity.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<List<DataSource>> GetDataSources(Binding topMostBinding, Binding[] bindings, List<DataSource> currentDataSources, Dictionary<int, List<int>> depthMap, Entity destinationEntity);

        /// <summary>
        /// The get bindings for entity async.
        /// </summary>
        /// <param name="entityId">
        /// The entity id.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<Binding[]> GetBindingsForEntityAsync(Entity entity);

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
        Task GetChildText(StringBuilder builder, Binding binding, Binding[] bindings, bool isFirst, bool isObject,Dictionary<int, List<int>> depthMap, int depth);

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
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        Task<string> AddKeyLevels(
            string currentSqlString,
            Dictionary<int, List<int>> keyleveldepth,
            Binding binding,
            Binding[] bindings,
            Entity entity);
    }
}
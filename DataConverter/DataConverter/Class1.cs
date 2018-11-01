using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataConverter
{
    using System.Threading;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;

    using Unity;

    public class Class1 : IDataTransformer
    {
        public async Task<long> TransformDataAsync(
            BindingExecution bindingExecution,
            Binding binding,
            Entity entity,
            CancellationToken cancellationToken)
        {

            return Convert.ToInt64(1);
        }

        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpver
            return binding.BindingType == "Nested"; // BindingType.
        }

        public void RunDatabus()
        {
            var config = new QueryConfig
            {
                ConnectionString = "server=HC2260;initial catalog=EDWAdmin;Trusted_Connection=True;",
                Url = "https://HC2260.hqcatalyst.local/DataProcessingService/v1/BatchExecutions",
                MaximumEntitiesToLoad = 1000,
                EntitiesPerBatch = 100,
                EntitiesPerUploadFile = 100,
                LocalSaveFolder = @"C:\Catalyst\databus",
                DropAndReloadIndex = false,
                WriteTemporaryFilesToDisk = true,
                WriteDetailedTemporaryFilesToDisk = true,
                CompressFiles = false,
                UploadToElasticSearch = true,
                Index = "Patients2",
                Alias = "patients",
                EntityType = "patient",
                TopLevelKeyColumn = "EDWPatientID",
                UseMultipleThreads = false,
                KeepTemporaryLookupColumnsInOutput = true
            };
            var jobData = new JobData
            {
                DataModel = "{}",
                MyDataSources = new List<DataSource> {
                                    new DataSource
                                        {
                                            Sql =
                                                "SELECT 3 [EDWPatientID], 2 [BatchDefinitionId], 'Queued' [Status], 'Batch' [PipelineType]"
                                        }
                                }
            };
            var job = new Job
            {
                Config = config,
                Data = jobData
            };
            var runner = new DatabusRunner();
            runner.RunRestApiPipeline(new UnityContainer(), job, new CancellationToken());
        }
    }
}

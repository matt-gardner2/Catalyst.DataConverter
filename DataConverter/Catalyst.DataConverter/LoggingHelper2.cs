namespace DataConverter
{
    using System.IO;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    public class LoggingHelper2
    {
        public static void Debug(string message)
        {
            LoggingHelper.Debug(message);
            File.AppendAllText(@"C:\Program Files\Health Catalyst\Data-Processing Engine\logs\DataProcessingEngine.log", message + "\n\n");
        }
    }
}

namespace UnitTestProject1
{
    using System.Threading.Tasks;

    using DataConverter;

    using Xunit;

    public class UnitTest1
    {

        [Fact]
        public async Task RunDataBus()
        {
            Class1 thisClass = new Class1();
            thisClass.RunDatabus();
        }
    }
}

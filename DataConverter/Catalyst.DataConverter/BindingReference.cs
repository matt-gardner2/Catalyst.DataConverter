namespace DataConverter
{
    using Catalyst.DataProcessing.Shared.Models.Metadata;

    /// <summary>
    /// The binding reference.
    /// </summary>
    public class BindingReference : ObjectReference
    {
        /// <summary>
        /// Gets or sets the parent object id.
        /// </summary>
        public int ParentObjectId { get; set; }
    }
}

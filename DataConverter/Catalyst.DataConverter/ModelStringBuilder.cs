// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ModelStringBuilder.cs" company="Health Catalyst">
//   String Builder for data model
// </copyright>
// <summary>
//   Defines the ModelStringBuilder type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace DataConverter
{
    using System.Text;

    /// <summary>
    /// The model string builder.
    /// </summary>
    public class ModelStringBuilder
    {
        /// <summary>
        /// The opening tag.
        /// </summary>
        private StringBuilder openingTag = null;

        /// <summary>
        /// The closing tag.
        /// </summary>
        private StringBuilder closingTag = null;

        /// <summary>
        /// Gets the opening tag.
        /// </summary>
        public StringBuilder OpeningTag
        {
            get
            {
                if (this.openingTag == null)
                {
                    this.openingTag = new StringBuilder();
                    this.openingTag.Append("{");
                }

                return this.openingTag;
            }
        }

        /// <summary>
        /// Gets the closing tag.
        /// </summary>
        public StringBuilder ClosingTag
        {
            get
            {
                if (this.closingTag == null)
                {
                    this.closingTag = new StringBuilder();
                    this.closingTag.Append("}");
                }

                return this.closingTag;
            }
        }
    }
}
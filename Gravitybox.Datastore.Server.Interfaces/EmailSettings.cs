using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gravitybox.Datastore.Server.Interfaces
{
    public class EmailSettings
    {
        #region Property Implementations

        public string Subject { get; set; }
        public string Body { get; set; }
        public string To { get; set; }
        public string From { get; set; }
        public string Bcc { get; set; }
        public string Cc { get; set; }
        public string GlobalRedirect { get; set; }
        public string Sender { get; set; }
        public string ReplyTo { get; set; }
        public string DisplayName { get; set; }
        public List<string> Attachments { get; set; }

        #endregion

    }
}
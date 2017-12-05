using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using Gravitybox.Datastore.Common;
using System.IO;

namespace Gravitybox.Datastore.Server.Core
{
    public class EmailDomain
    {
        private static readonly Dictionary<string, SmtpClient> _mailServerCache = new Dictionary<string, SmtpClient>();

        public static void SendMail(EmailSettings mailItem)
        {
            //If there is no mail server then there is nothing to do
            if (string.IsNullOrEmpty(ConfigHelper.MailServer))
                return;

            try
            {
                if (string.IsNullOrEmpty(mailItem.To)) return;

                var mm = new MailMessage
                {
                    IsBodyHtml = true,
                    BodyEncoding = System.Text.Encoding.UTF8,
                    SubjectEncoding = System.Text.Encoding.UTF8
                };

                var diplayName = mailItem.From;
                if (string.IsNullOrEmpty(diplayName)) diplayName = "noreply@nowhere.com";

                mm.From = new MailAddress(diplayName, diplayName);
                mm.ReplyToList.Add(mm.From);
                mm.Sender = mm.From;

                if (!string.IsNullOrEmpty(mailItem.Cc))
                    mm.CC.Add(mailItem.Cc);
                if (!string.IsNullOrEmpty(mailItem.Bcc))
                    mm.Bcc.Add(mailItem.Bcc);

                //To Address
                foreach (var address in ParseEmail(mailItem.To))
                {
                    mm.To.Add(new MailAddress(address));
                }

                //CC Address
                foreach (var address in ParseEmail(mailItem.Cc))
                {
                    mm.CC.Add(new MailAddress(address));
                }

                //BCC Address
                foreach (var address in ParseEmail(mailItem.Bcc))
                {
                    mm.Bcc.Add(new MailAddress(address));
                }

                mm.Subject = mailItem.Subject;
                mm.Body = mailItem.Body;

                if (mailItem.Attachments != null)
                {
                    mailItem.Attachments.ForEach(x => mm.Attachments.Add(new Attachment(x)));
                }

                if (mailItem.Attachments != null)
                {
                    mailItem.Attachments.ForEach(x =>
                    {
                        if (File.Exists(x))
                            mm.Attachments.Add(new Attachment(x));
                    });
                }

                ParseEmail(mailItem.Bcc).ToList().ForEach(x => mm.Bcc.Add(x.Trim()));

                if (!_mailServerCache.ContainsKey(ConfigHelper.MailServer))
                    _mailServerCache.Add(ConfigHelper.MailServer, new SmtpClient(ConfigHelper.MailServer));
                var mailServer = _mailServerCache[ConfigHelper.MailServer];

                mailServer.Timeout = 10;
                mailServer.Port = Convert.ToInt32(ConfigHelper.MailServerPort);
                if (!string.IsNullOrEmpty(ConfigHelper.MailServerUsername))
                    mailServer.Credentials = new NetworkCredential(ConfigHelper.MailServerUsername, ConfigHelper.MailServerPassword);
                mailServer.Send(mm);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, "Email Failed, To: " + mailItem.To + ", Subject: " + mailItem.Subject);
            }

        }

        private static IEnumerable<string> ParseEmail(string emails)
        {
            if (string.IsNullOrEmpty(emails)) return new string[] { };
            return emails.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
        }

    }
}

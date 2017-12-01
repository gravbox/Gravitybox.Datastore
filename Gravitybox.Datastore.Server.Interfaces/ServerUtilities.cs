#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Gravitybox.Datastore.Common;
using System.Xml.Serialization;
using System.Xml;

namespace Gravitybox.Datastore.Server.Interfaces
{
    public static class ServerUtilities
    {
        public static string CreateWordBlob(string s)
        {
            if (string.IsNullOrEmpty(s)) return null;
            try
            {
                var list = new List<string>();
                #region Load List
                list.Add("a");
                list.Add("about");
                list.Add("1");
                list.Add("after");
                list.Add("2");
                list.Add("all");
                list.Add("also");
                list.Add("3");
                list.Add("an");
                list.Add("4");
                list.Add("and");
                list.Add("5");
                list.Add("another");
                list.Add("6");
                list.Add("any");
                list.Add("7");
                list.Add("are");
                list.Add("8");
                list.Add("as");
                list.Add("9");
                list.Add("at");
                list.Add("0");
                list.Add("be");
                list.Add("$");
                list.Add("because");
                list.Add("been");
                list.Add("before");
                list.Add("being");
                list.Add("between");
                list.Add("both");
                list.Add("but");
                list.Add("by");
                list.Add("came");
                list.Add("can");
                list.Add("come");
                list.Add("could");
                list.Add("did");
                list.Add("do");
                list.Add("does");
                list.Add("each");
                list.Add("else");
                list.Add("for");
                list.Add("from");
                list.Add("get");
                list.Add("got");
                list.Add("has");
                list.Add("had");
                list.Add("he");
                list.Add("have");
                list.Add("her");
                list.Add("here");
                list.Add("him");
                list.Add("himself");
                list.Add("his");
                list.Add("how");
                list.Add("if");
                list.Add("in");
                list.Add("into");
                list.Add("is");
                list.Add("it");
                list.Add("its");
                list.Add("just");
                list.Add("like");
                list.Add("make");
                list.Add("many");
                list.Add("me");
                list.Add("might");
                list.Add("more");
                list.Add("most");
                list.Add("much");
                list.Add("must");
                list.Add("my");
                list.Add("never");
                list.Add("no");
                list.Add("now");
                list.Add("of");
                list.Add("on");
                list.Add("only");
                list.Add("or");
                list.Add("other");
                list.Add("our");
                list.Add("out");
                list.Add("over");
                list.Add("re");
                list.Add("said");
                list.Add("same");
                list.Add("see");
                list.Add("should");
                list.Add("since");
                list.Add("so");
                list.Add("some");
                list.Add("still");
                list.Add("such");
                list.Add("take");
                list.Add("than");
                list.Add("that");
                list.Add("the");
                list.Add("their");
                list.Add("them");
                list.Add("then");
                list.Add("there");
                list.Add("these");
                list.Add("they");
                list.Add("this");
                list.Add("those");
                list.Add("through");
                list.Add("to");
                list.Add("too");
                list.Add("under");
                list.Add("up");
                list.Add("use");
                list.Add("very");
                list.Add("want");
                list.Add("was");
                list.Add("way");
                list.Add("we");
                list.Add("well");
                list.Add("were");
                list.Add("what");
                list.Add("when");
                list.Add("where");
                list.Add("which");
                list.Add("while");
                list.Add("who");
                list.Add("will");
                list.Add("with");
                list.Add("would");
                list.Add("you");
                list.Add("your");
                #endregion

                var text = s;
                text = text.Replace("\"", " ");
                text = text.Replace(",", " ");
                text = text.Replace(".", " ");
                text = text.Replace("!", " ");
                text = text.Replace("?", " ");
                text = text.Replace("\r\n", " ");
                text = text.Replace("\r", " ");
                text = text.Replace("\n", " ");
                var q = text.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).ToList();
                foreach (string word in list)
                {
                    q.RemoveAll(x => string.Compare(x, word, true) == 0);
                }
                return string.Join(" ", q);
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                return s;
            }
        }

        public static void DeleteDirectoryWithRetry(string path)
        {
            var tries = 0;

            try
            {
                if (Directory.Exists(path))
                {
                    // Fix for readonly files when deleting a directory
                    var files = Directory.EnumerateFiles(path, "*.*", SearchOption.AllDirectories);
                    foreach (var file in files)
                    {
                        File.SetAttributes(file, FileAttributes.Normal);
                    }
                    Directory.Delete(path, true);
                }
            }
            catch(Exception ex)
            {
                //Do Nothing
            }

            while (tries < 5)
            {
                try
                {
                    if (Directory.Exists(path))
                        Directory.Delete(path, true);
                    return;
                }
                catch (Exception ex)
                {
                    //Logger.LogError(ex);
                    tries++;
                    System.Threading.Thread.Sleep(500);
                }
            }
            throw new Exception("Could not delete folder '" + path + "'");
        }

        public static string GetSqlType(RepositorySchema.DataTypeConstants type)
        {
            switch (type)
            {
                case RepositorySchema.DataTypeConstants.Bool:
                    return "BIT";
                case RepositorySchema.DataTypeConstants.DateTime:
                    return "DATETIME";
                case RepositorySchema.DataTypeConstants.Float:
                    return "FLOAT";
                case RepositorySchema.DataTypeConstants.GeoCode:
                    return "GEOGRAPHY";
                case RepositorySchema.DataTypeConstants.Int:
                    return "INT";
                case RepositorySchema.DataTypeConstants.String:
                    return "NVARCHAR";
                default:
                    throw new Exception("Unknown type");
            }
        }

        public static Guid RandomizeGuid(Guid g, byte seed)
        {
            var arr = g.ToByteArray();
            for (var ii = 0; ii < 16; ii++)
                arr[ii] = (byte)(arr[ii] ^ seed);
            return new Guid(arr);
        }

        /// <summary>
        /// Serialize an object into a byte array
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static byte[] SerializeObject(object data)
        {
            try
            {
                if (null == data)
                    return new byte[1] { 0 };

                using (var stringWriter = new StringWriter())
                {
                    var serializer = new XmlSerializer(data.GetType());
                    serializer.Serialize(stringWriter, data);
                    var xml = stringWriter.ToString();
                    var bytes = Encoding.UTF8.GetBytes(xml);
                    return bytes;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public static T DeserializeObject<T>(byte[] taskData) where T : new()
        {
            try
            {
                if (taskData == null) return default(T);
                if (taskData.Length == 1 && taskData[0] == 0) return default(T);
                using (var stream = new MemoryStream(taskData))
                using (var streamReader = new StreamReader(stream, Encoding.UTF8))
                using (var xmlReader = GetSafeXmlReader(streamReader))
                {
                    var serializer = new XmlSerializer(typeof(T));
                    return (T)serializer.Deserialize(xmlReader);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static XmlReader GetSafeXmlReader(TextReader reader)
        {
            return XmlReader.Create(reader, GetSafeXmlReaderSettings());
        }

        public static XmlReaderSettings GetSafeXmlReaderSettings()
        {
            var settings = new XmlReaderSettings();
            settings.XmlResolver = null;
            settings.IgnoreComments = true;
            //settings.IgnoreWhitespace = true;
            settings.CheckCharacters = false; // Please don't change this.  Very import to WebInspect FPR parsing.
            settings.DtdProcessing = DtdProcessing.Ignore;
            return settings;
        }


    }
}

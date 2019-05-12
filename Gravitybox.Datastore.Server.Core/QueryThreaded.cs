using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;
using System.Xml;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Gravitybox.Datastore.Server.Core
{
    internal class QueryThreaded
    {
        private DimensionCache _dimensionCache;
        private RepositorySchema _schema;
        private static ConcurrentHashSet<Guid> _runningList = new ConcurrentHashSet<Guid>();

        public QueryThreaded(DimensionCache dimensionCache, RepositorySchema schema, DataQuery query)
        {
            this.Query = query;
            _dimensionCache = dimensionCache;
            _schema = schema;
            this.Key = Guid.NewGuid();
        }

        public Guid Key { get; private set; }
        public bool IsComplete { get; private set; } = false;
        public DataQuery Query { get; private set; }

        public void Run()
        {
            //Only have 1 running async query for a repository
            while (!_runningList.TryAdd(_schema.ID))
                System.Threading.Thread.Sleep(1000);

            try
            {
                var timer = Stopwatch.StartNew();

                List<DimensionItem> dimensionList = null;
                using (var context = new DatastoreEntities(ConfigHelper.ConnectionString))
                {
                    dimensionList = _dimensionCache.Get(context, _schema, _schema.InternalID, new List<DataItem>());
                }

                //There is no such thing as a list field that is not a dimension
                var dataTableFields = _schema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();
                var nonListDimensionFields = _schema.DimensionList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();
                var listDimensionFields = _schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList();

                var parameters = new List<SqlParameter>();
                var sql = SqlHelper.QueryAsync(_schema, _schema.InternalID, this.Query, dimensionList, parameters, ConfigHelper.ConnectionString);

                #region Get all the list dimensions for those fields
                var dimensionMapper = new ConcurrentDictionary<long, Dictionary<long, List<long>>>();
                var timerList = Stopwatch.StartNew();
                Parallel.ForEach(listDimensionFields, new ParallelOptions { MaxDegreeOfParallelism = 4 }, (ditem) =>
                {
                    try
                    {
                        var valueMapper = new Dictionary<long, List<long>>();
                        dimensionMapper.TryAdd(ditem.DIdx, valueMapper);
                        var dTable = SqlHelper.GetListTableName(_schema.ID, ditem.DIdx);

                        //This is the fastest way I could find to load this data
                        using (var connection = new SqlConnection(ConfigHelper.ConnectionString))
                        {
                            connection.Open();
                            using (var command = new SqlCommand($"SELECT Y.[{SqlHelper.RecordIdxField}], Y.[DVIdx] FROM [{dTable}] Y {SqlHelper.NoLockText()} ORDER BY Y.[{SqlHelper.RecordIdxField}], Y.[DVIdx]", connection))
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        var recordIndex = (long)reader[0];
                                        var dvidx = (long)reader[1];
                                        if (!valueMapper.ContainsKey(recordIndex))
                                            valueMapper.Add(recordIndex, new List<long>());
                                        valueMapper[recordIndex].Add(dvidx);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerCQ.LogError(ex);
                        throw;
                    }
                });
                timerList.Stop();
                #endregion

                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, this.Key.ToString());
                var rowCount = 0;
                using (var tempFile = XmlTextWriter.Create(fileName))
                {
                    tempFile.WriteStartDocument();
                    tempFile.WriteStartElement("root");
                    using (var connection = new SqlConnection(ConfigHelper.ConnectionString))
                    {
                        var command = new SqlCommand(sql, connection);
                        command.CommandTimeout = 3600;
                        command.Parameters.AddRange(parameters.ToArray());
                        connection.Open();
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                #region Write headers
                                tempFile.WriteStartElement("headers");
                                foreach (var h in dataTableFields)
                                {
                                    var d = nonListDimensionFields.FirstOrDefault(x => x.Name == h.Name);
                                    if (d == null)
                                        tempFile.WriteElementString("h", h.Name);
                                    else
                                    {
                                        tempFile.WriteStartElement("h");
                                        tempFile.WriteAttributeString("didx", d.DIdx.ToString());
                                        tempFile.WriteValue(d.Name);
                                        tempFile.WriteEndElement();
                                    }
                                }
                                foreach (var d in listDimensionFields)
                                {
                                    tempFile.WriteStartElement("h");
                                    tempFile.WriteAttributeString("didx", d.DIdx.ToString());
                                    tempFile.WriteValue(d.Name);
                                    tempFile.WriteEndElement(); //h
                                }
                                tempFile.WriteEndElement(); //headers
                                #endregion

                                #region Write Dimension Defs
                                tempFile.WriteStartElement("dimensions");
                                foreach (var d in dimensionList)
                                {
                                    tempFile.WriteStartElement("d");
                                    tempFile.WriteAttributeString("didx", d.DIdx.ToString());
                                    tempFile.WriteAttributeString("name", d.Name);
                                    foreach (var r in d.RefinementList)
                                    {
                                        tempFile.WriteStartElement("r");
                                        tempFile.WriteAttributeString("dvidx", r.DVIdx.ToString());
                                        tempFile.WriteValue(r.FieldValue);
                                        tempFile.WriteEndElement(); //r
                                    }
                                    tempFile.WriteEndElement(); //d
                                }
                                tempFile.WriteEndElement(); //dimensions
                                #endregion

                                #region Write Items
                                tempFile.WriteStartElement("items");
                                while (reader.Read())
                                {
                                    var index = 0;
                                    tempFile.WriteStartElement("i");

                                    //Write static fields
                                    var recordIndex = reader.GetInt64(dataTableFields.Count);
                                    var timestamp = reader.GetInt32(dataTableFields.Count + 1);
                                    tempFile.WriteAttributeString("ri", recordIndex.ToString());
                                    tempFile.WriteAttributeString("ts", timestamp.ToString());

                                    #region Write all data table (Z) fields
                                    foreach (var field in dataTableFields)
                                    {
                                        if (reader.IsDBNull(index))
                                        {
                                            tempFile.WriteElementString("v", "~■!N");
                                        }
                                        else
                                        {
                                            switch (field.DataType)
                                            {
                                                case RepositorySchema.DataTypeConstants.Bool:
                                                    tempFile.WriteElementString("v", reader.GetBoolean(index) ? "1" : "0");
                                                    break;
                                                case RepositorySchema.DataTypeConstants.DateTime:
                                                    tempFile.WriteElementString("v", reader.GetDateTime(index).Ticks.ToString());
                                                    break;
                                                case RepositorySchema.DataTypeConstants.Float:
                                                    tempFile.WriteElementString("v", reader.GetDouble(index).ToString());
                                                    break;
                                                case RepositorySchema.DataTypeConstants.GeoCode:
                                                    var geo = (Microsoft.SqlServer.Types.SqlGeography)reader.GetValue(index);
                                                    tempFile.WriteElementString("v", $"{geo.Lat}|{geo.Long}");
                                                    break;
                                                case RepositorySchema.DataTypeConstants.Int:
                                                    tempFile.WriteElementString("v", reader.GetInt32(index).ToString());
                                                    break;
                                                case RepositorySchema.DataTypeConstants.Int64:
                                                    tempFile.WriteElementString("v", reader.GetInt64(index).ToString());
                                                    break;
                                                case RepositorySchema.DataTypeConstants.String:
                                                    tempFile.WriteElementString("v", StripNonValidXMLCharacters(reader.GetString(index)));
                                                    break;
                                                default:
                                                    break;
                                            }
                                        }
                                        index++;
                                    }
                                    #endregion

                                    #region Write List fields
                                    foreach (var field in listDimensionFields)
                                    {
                                        if (dimensionMapper.ContainsKey(field.DIdx) && dimensionMapper[field.DIdx].ContainsKey(recordIndex))
                                        {
                                            tempFile.WriteElementString("v", dimensionMapper[field.DIdx][recordIndex].ToList().ToStringList("|"));
                                        }
                                    }
                                    #endregion

                                    tempFile.WriteEndElement(); //i
                                    rowCount++;
                                }
                                tempFile.WriteEndElement(); //items
                                #endregion
                            }
                            reader.Close();
                        }
                    }
                    tempFile.WriteEndElement(); //root
                }

                //Write file that signifies we are done
                var zipFile = Extensions.ZipFile(fileName);
                var outFile = fileName + ".zzz";
                File.Move(zipFile, outFile);
                var size = (new FileInfo(outFile)).Length;
                System.Threading.Thread.Sleep(300);
                File.Delete(fileName);
                System.Threading.Thread.Sleep(300);
                timer.Stop();
                LoggerCQ.LogInfo($"QueryThreaded Complete: ID={_schema.ID}, File={outFile}, Size={size}, Count={rowCount}, ListElapsed={timerList.ElapsedMilliseconds}, Elapsed={timer.ElapsedMilliseconds}");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex, $"ID={_schema.ID}, Query=\"{this.Query.ToString()}\"");
                File.WriteAllText(Path.Combine(ConfigHelper.AsyncCachePath, this.Key.ToString() + ".error"), "error");
            }
            finally
            {
                this.IsComplete = true;
                _runningList.Remove(_schema.ID);
            }
        }

        /// <summary>
        /// Determines if a repository has a running async query
        /// </summary>
        public static bool HasRunningQuery(Guid id)
        {
            return _runningList.Contains(id);
        }

        /// <summary>
        /// Ensure that a string value is valid XML
        /// </summary>
        private string StripNonValidXMLCharacters(string textIn)
        {
            if (textIn == null || textIn == string.Empty) return string.Empty;
            try
            {
                return XmlConvert.VerifyXmlChars(textIn);
            }
            catch
            {
                //Do Nothing
            }

            var textOut = new StringBuilder(); // Used to hold the output.
            char current; // Used to reference the current character.

            for (int ii = 0; ii < textIn.Length; ii++)
            {
                current = textIn[ii];
                if ((current == 0x9 || current == 0xA || current == 0xD) ||
                    ((current >= 0x20) && (current <= 0xD7FF)) ||
                    ((current >= 0xE000) && (current <= 0xFFFD)) ||
                    ((current >= 0x10000) && (current <= 0x10FFFF)))
                {
                    textOut.Append(current);
                }
                else
                    textOut.Append("?");
            }
            return textOut.ToString();
        }

    }
}
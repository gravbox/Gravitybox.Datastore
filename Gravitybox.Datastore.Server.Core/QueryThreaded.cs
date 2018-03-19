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

namespace Gravitybox.Datastore.Server.Core
{
    internal class QueryThreaded
    {
        private DataQuery _query = null;
        private DimensionCache _dimensionCache;
        private RepositorySchema _schema;

        public QueryThreaded(DimensionCache dimensionCache, RepositorySchema schema, DataQuery query)
        {
            _query = query;
            _dimensionCache = dimensionCache;
            _schema = schema;
            this.Key = Guid.NewGuid();
        }

        public Guid Key { get; private set; }
        public bool IsComplete { get; private set; } = false;

        public void Run()
        {
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
                //var normalFields = _schema.FieldList.Where(x => x.FieldType == RepositorySchema.FieldTypeConstants.Field).ToList();
                var nonListDimensionFields = _schema.DimensionList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();
                var listDimensionFields = _schema.DimensionList.Where(x => x.DimensionType == RepositorySchema.DimensionTypeConstants.List).ToList();

                var parameters = new List<SqlParameter>();
                var sql = SqlHelper.QueryAsync(_schema, _schema.InternalID, _query, dimensionList, parameters, ConfigHelper.ConnectionString);

                #region Get all the list dimensions for those fields
                var dimensionMapper = new Dictionary<long, Dictionary<long, List<long>>>();
                foreach (var ditem in listDimensionFields)
                {
                    var valueMapper = new Dictionary<long, List<long>>();
                    dimensionMapper.Add(ditem.DIdx, valueMapper);
                    var dTable = SqlHelper.GetListTableName(_schema.ID, ditem.DIdx);
                    var ds = SqlHelper.GetDataset(ConfigHelper.ConnectionString, $"select [{SqlHelper.RecordIdxField}], [DVIdx] from [{dTable}] order by [{SqlHelper.RecordIdxField}], [DVIdx]");
                    if(ds.Tables.Count == 1)
                    {
                        foreach (System.Data.DataRow row in ds.Tables[0].Rows)
                        {
                            var recordIndex = (long)row[0];
                            var dvidx = (long)row[1];
                            if (!valueMapper.ContainsKey(recordIndex))
                                valueMapper.Add(recordIndex, new List<long>());
                            valueMapper[recordIndex].Add(dvidx);
                        }
                    }
                }
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
                        var reader = command.ExecuteReader();
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
                            foreach(var d in dimensionList)
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
                                            case RepositorySchema.DataTypeConstants.String:
                                                tempFile.WriteElementString("v", reader.GetString(index));
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
                                        tempFile.WriteElementString("v", string.Join("|", dimensionMapper[field.DIdx][recordIndex].ToList()));
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
                    tempFile.WriteEndElement(); //root
                }

                //Write file that signifies we are done
                var zipFile = Gravitybox.Datastore.Common.Extensions.ZipFile(fileName);
                var outFile = fileName + ".zzz";
                File.Move(zipFile, outFile);
                var size = (new FileInfo(outFile)).Length;
                System.Threading.Thread.Sleep(300);
                File.Delete(fileName);
                System.Threading.Thread.Sleep(300);
                timer.Stop();
                LoggerCQ.LogInfo($"QueryThreaded Complete: File={outFile}, Size={size}, Count={rowCount}, Elapsed={timer.ElapsedMilliseconds}");
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                File.WriteAllText(Path.Combine(ConfigHelper.AsyncCachePath, this.Key.ToString() + ".error"), "error");
            }
            finally
            {
                this.IsComplete = true;
            }
        }
    }
}
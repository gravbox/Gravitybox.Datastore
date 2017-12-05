using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Gravitybox.Datastore.Common;
using Gravitybox.Datastore.EFDAL;

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

                var normalFields = _schema.FieldList.Where(x => x.DataType != RepositorySchema.DataTypeConstants.List).ToList();

                var parameters = new List<SqlParameter>();
                var sql = SqlHelper.QueryAsync(_schema, _schema.InternalID, _query, dimensionList, parameters, ConfigHelper.ConnectionString);

                var fileName = Path.Combine(ConfigHelper.AsyncCachePath, this.Key.ToString());
                using (var tempFile = File.CreateText(fileName))
                {
                    using (var connection = new SqlConnection(ConfigHelper.ConnectionString))
                    {
                        var command = new SqlCommand(sql, connection);
                        command.CommandTimeout = 3600;
                        command.Parameters.AddRange(parameters.ToArray());
                        connection.Open();
                        var reader = command.ExecuteReader();
                        if (reader.HasRows)
                        {
                            //Write header line
                            tempFile.Write(string.Join(",", normalFields.Select(x => x.Name.ToXmlValue())) + Environment.NewLine);

                            while (reader.Read())
                            {
                                var index = 0;
                                foreach (var field in normalFields)
                                {
                                    if (reader.IsDBNull(index))
                                    {
                                        //Do Nothing
                                    }
                                    else
                                    {
                                        switch (field.DataType)
                                        {
                                            case RepositorySchema.DataTypeConstants.Bool:
                                                tempFile.Write(reader.GetBoolean(index).ToString());
                                                break;
                                            case RepositorySchema.DataTypeConstants.DateTime:
                                                tempFile.Write(reader.GetDateTime(index).ToString("yyyy-MM-dd HH:mm:ss"));
                                                break;
                                            case RepositorySchema.DataTypeConstants.Float:
                                                tempFile.Write(reader.GetFloat(index).ToString());
                                                break;
                                            case RepositorySchema.DataTypeConstants.GeoCode:
                                                //TODO
                                                break;
                                            case RepositorySchema.DataTypeConstants.Int:
                                                tempFile.Write(reader.GetInt32(index).ToString());
                                                break;
                                            case RepositorySchema.DataTypeConstants.List:
                                                //TODO
                                                break;
                                            case RepositorySchema.DataTypeConstants.String:
                                                var v = reader.GetString(index);
                                                // always quote string values
                                                //v = string.Format("\"{0}\"", v.ToString().Replace("\"", "\"\""));
                                                v = v.ToXmlValue();
                                                tempFile.Write(v);
                                                break;
                                        }
                                    }

                                    index++;
                                    if (index < normalFields.Count)
                                        tempFile.Write(",");
                                }
                                tempFile.Write(Environment.NewLine);
                            }
                        }
                        reader.Close();
                    }
                }

                //Write file that signifies we are done
                var zipFile = Gravitybox.Datastore.Common.Extensions.ZipFile(fileName);
                var outFile = fileName + ".zzz";
                File.Move(zipFile, outFile);
                var size = (new FileInfo(outFile)).Length;
                System.Threading.Thread.Sleep(300);
                File.Delete(fileName);
                System.Threading.Thread.Sleep(300);
                LoggerCQ.LogInfo("QueryThreaded Complete: File=" + outFile + ", Size=" + size);
                timer.Stop();
            }
            catch (Exception ex)
            {
                LoggerCQ.LogError(ex);
                File.WriteAllText(Path.Combine(ConfigHelper.AsyncCachePath, this.Key.ToString() + ".error"), "error");
            }
        }
    }
}
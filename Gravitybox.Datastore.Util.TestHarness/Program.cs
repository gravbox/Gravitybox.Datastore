#pragma warning disable 0168
using System;
using System.Linq;
using Gravitybox.Datastore.Common.Queryable;
using Gravitybox.Datastore.Common;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.IO;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Reflection;
using System.Threading;

namespace Gravitybox.Datastore.Util.TestHarness
{
    class Program
    {
        private static readonly Guid repoID = new Guid("00000000-0000-0000-0000-17728d4a8361");
        private const string SERVER = "localhost";
        private const int PORT = 1973;
        private static Random _rnd = new Random();

        static void Main(string[] args)
        {
            if (args.Any(x => x == "/console"))
            {
                var F = new TesterForm();
                F.ShowDialog();
                return;
            }

            //GetItems();

            try
            {
                #region Wait for datastore to be ready
                while (!IsDatastoreReady())
                {
                    Console.WriteLine("Waiting for Datastore ready...");
                    System.Threading.Thread.Sleep(1000);
                }
                #endregion

                //CreateRepo();
                //AddData();
                //DuplicateFilters();
                //TestDerivedFields();
                //HitHard();
                PerfTest();
                //Test12();
                //TestAllDimensions();
                //Test44();
                //TestSchema();
                //TestAlive();
                //TestFailover();
                //TestQueryAsync();
                //TestThreading();
                TestManyConnections();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            Console.WriteLine("Press <ENTER> to end...");
            Console.ReadLine();
        }

        private static void TestAlive()
        {
            var timer = Stopwatch.StartNew();
            Parallel.For(0, 1000, new ParallelOptions { MaxDegreeOfParallelism = 20 }, (ii) =>
               {
                   using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                   {
                       repo.IsServerAlive();
                   }
               });
            timer.Stop();
            Console.WriteLine($"Elapsed={timer.ElapsedMilliseconds}");
        }

        private static void DuplicateFilters()
        {
            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
            {
                var q = repo.Query
                    .Where(x => x.Project == "v")
                    .Where(x => x.Project == "b")
                    .Results();
            }
        }

        private static void AddData()
        {
            var rnd = new Random();

            var dimValues = new List<string>();
            for (var ii = 1; ii <= 50; ii++)
                dimValues.Add("Value " + ii);

            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
            {
                //repo.ClearRepository();
                var startTimestamp = repo.GetTimestamp();

                var q = repo.Query.Results();

                for (var ii = 0; ii < 50; ii++)
                {
                    var newItem = new MyItem
                    {
                        Project = "Hello" + ii,
                        Field1 = "V-" + (rnd.Next() % 5),
                        ID = ii,
                        MyList = new string[] { "aa", "bb" },
                        CreatedDate = DateTime.Now.AddMinutes(-_rnd.Next(0, 10000)),
                        Dim2 = "Dim2-" + (rnd.Next() % 10),
                        MyBool = (rnd.Next(100) % 2 == 0) ? true : false,
                        MyFloat = rnd.Next(1, 10000),
                        MyGeo = new GeoCode { Latitude = rnd.Next(-90, 90), Longitude = rnd.Next(-90, 90) },
                        MyBool2 = (rnd.Next(100) % 2 == 0) ? true : false,
                        MyFloat2 = 2,
                        MyFloat3 = 4,
                        SomeInt2 = 5,
                        MyByte = 40,
                        MyShort = 99,
                        MyDecimal = 66,
                        MyDecimal2 = 33,
                        MyLong = 17626,
                    };

                    newItem.Dim2 = dimValues[rnd.Next(0, dimValues.Count)];

                    //repo.Delete.Where(X => X.ID == 1).Commit();
                    //newItem.Project = DateTime.Now.Ticks.ToString();
                    repo.InsertOrUpdate(newItem);
                    //repo.Delete.Where(x => x.ID == ii).Commit();
                    //repo.Delete.Where(x => x.ID == 999).Commit();

                    Console.WriteLine($"Added Item {ii}");
                }


                //repo.Delete.Where(x=>x.ID == -1).Commit();
                //var diagnostics = repo.Update
                //    .Field(x => x.Project, "")
                //    .Field(x => x.Field1, "q")
                //    .Where(x => x.ID == 1)
                //    .Commit();

                //var pp = 0;
                //if (1 == pp)
                //{
                //    repo.Delete.Where(x => x.__Timestamp < startTimestamp && x.ID == 5).Commit();
                //}

            }
        }

        private static void CreateRepo()
        {
            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
            {
                if (repo.RepositoryExists())
                    repo.DeleteRepository();
                if (!repo.RepositoryExists())
                    repo.CreateRepository();
            }
        }

        private static void TestNonParsedField()
        {
            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
            {
                var q = repo.Query
                    .AddNonParsedField("foo", "bar");

                //var dq = q.ToQuery();
                //Console.WriteLine(dq.NonParsedFieldList.Count);

                var r = q.Results();
                Console.WriteLine(r.Query.NonParsedFieldList.Count);
            }
        }

        private static void TestDerivedFields()
        {
            try
            {
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    var results = repo.Query
                        .FieldCount(x => x.ID)
                        .Results();

                    var qq = results.SelectDerivedValue(x => x.ID);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void HitHard()
        {
            try
            {
                long index = 0;
                while (true)
                {
                    using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                    {
                        index++;
                        var id = _rnd.Next(1, 9999999);
                        LoggerCQ.LogInfo("HitHard: ID=" + id + ", Index=" + index);
                        var results = repo.Query
                            .Where(x => x.ID == id)
                            .Results();
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void PerfTest()
        {
            try
            {
                var tList = new List<Task>();

                for (var ii = 0; ii < 10; ii++)
                {
                    tList.Add(new Task(() =>
                    {
                        long index = 0;
                        while (true)
                        {
                            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                            {
                                index++;
                                var id = _rnd.Next(1, 99999999);
                                //LoggerCQ.LogInfo("HitHard: ID=" + id + ", Index=" + index);
                                var results = repo.Query
                                    .Where(x => x.ID == id)
                                    .Results();
                            }
                        }
                    }));
                }

                for (var ii = 0; ii <= 10; ii++)
                {
                    tList.Add(new Task(() =>
                    {
                        while (true)
                        {
                            InsertRandom();
                        }
                    }));
                }

                tList.ForEach(x => x.Start());

                Task.WaitAll(tList.ToArray());

            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void InsertRandom()
        {
            var rnd = new Random();

            var dimValues = new List<string>();
            for (var ii = 1; ii <= 50; ii++)
                dimValues.Add("Value " + ii);

            var timer = Stopwatch.StartNew();
            using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
            {
                var list = new List<MyItem>();
                for (var ii = 0; ii < 50; ii++)
                {
                    var ID = rnd.Next() % 9999999;
                    var newItem = new MyItem
                    {
                        Project = "Hello" + ID,
                        Field1 = "V-" + (rnd.Next() % 5),
                        ID = ID,
                        MyList = new string[] { "aa", "bb" },
                        CreatedDate = DateTime.Now.AddMinutes(-_rnd.Next(0, 10000)),
                        Dim2 = "Dim2-" + (rnd.Next() % 10),
                        MyBool = (rnd.Next(100) % 2 == 0) ? true : false,
                        MyFloat = rnd.Next(1, 10000),
                        MyGeo = new GeoCode { Latitude = rnd.Next(-90, 90), Longitude = rnd.Next(-90, 90) },
                        MyBool2 = (rnd.Next(100) % 2 == 0) ? true : false,
                        MyFloat2 = 2,
                        MyFloat3 = 4,
                        SomeInt2 = 5,
                        MyByte = 40,
                        MyShort = 99,
                        MyDecimal = 66,
                        MyDecimal2 = 33,
                        MyLong = 17626,
                    };
                    newItem.Dim2 = dimValues[rnd.Next(0, dimValues.Count)];
                    list.Add(newItem);
                }
                repo.InsertOrUpdate(list);
                timer.Stop();
                Console.WriteLine($"Added Items: Elapsed={timer.ElapsedMilliseconds}");
            }
        }

        private static void Test12()
        {
            try
            {
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    var query = repo.Query.RecordsPerPage(20);
                    var results = query.Results();
                    var dlist = query.DimensionsOnly();
                    var url = query.ToUrl();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void TestAllDimensions()
        {
            try
            {
                FailoverConfiguration.Servers.Add(new ServerConfig { Server = SERVER });
                using (var repo = new DatastoreRepository<MyItem>(repoID, "@config"))
                {
                    var q = repo.Query.AllDimensions();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void TestFailover()
        {
            var ii = 0;
            //FailoverConfiguration.Servers.Add(new ServerConfig { Server = "localhost", Port = 1973 });
            //FailoverConfiguration.Servers.Add(new ServerConfig { Server = "127.0.0.1", Port = 1974 });
            FailoverConfiguration.Servers.Add(new ServerConfig { Server = "10.13.31.13", Port = 1973 });
            FailoverConfiguration.Servers.Add(new ServerConfig { Server = "127.0.0.1", Port = 1973 });

            FailoverConfiguration.RetryOnFailCount = 0;
            var index = 0;
            do
            {
                var timer = Stopwatch.StartNew();
                try
                {
                    Parallel.For(0, 10000, new ParallelOptions { MaxDegreeOfParallelism = 30 }, (kk) =>
                      {
                          var timer2 = Stopwatch.StartNew();
                          using (var repo = new DatastoreRepository<MyItem>(repoID, "@config", PORT))
                          {
                              Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} Started");
                              var r6 = repo.Query.WhereDimensionValue(_rnd.Next(1, 20)).Results();
                              //var r6 = repo.Query.Results();
                              timer2.Stop();
                              Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} Query Success, Count={index}, Elapsed={timer2.ElapsedMilliseconds}");
                              index++;
                          }
                      });
                }
                catch (Common.Exceptions.FailoverException ex)
                {
                    //Do Nothing - so it will try again
                    timer.Stop();
                    Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")} Failed, Elapsed={timer.ElapsedMilliseconds}");
                }
                catch (Exception ex)
                {
                    timer.Stop();
                    Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss")}, Elapsed={timer.ElapsedMilliseconds}, Error={ex.Message}");
                }
            } while (ii == 0);
        }

        private static void TestQueryAsync()
        {
            try
            {
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    //var results = repo.Query.Results();
                    //using (var pinger = repo.Query.Where(x=>x.Field1 == "V-2").ResultsAsync())
                    using (var pinger = repo.Query.ResultsAsync())
                    {
                        var startTime = DateTime.Now;
                        pinger.WaitUntilReady();
                        Console.WriteLine($"Time: {DateTime.Now.Subtract(startTime).TotalMilliseconds}");
                        var file = pinger.OutputFile;

                        List<MyItem> rr = null;
                        do
                        {
                            rr = pinger.GetItems(99);
                            Console.WriteLine($"Count={rr.Count}");
                        } while (rr.Any());
                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void Test44()
        {
            try
            {
                var ticks = DateTime.Now.Ticks;
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    for (var ii = 0; ii < 1000; ii++)
                    {
                        var r6 = repo.Query
                            .WhereUrl($"?q={ii}")
                            .Results();
                        Console.WriteLine($"Index={ii}");

                        //foreach (var ritem in r6.AllDimensions.SelectMany(x => x.RefinementList))
                        //{
                        //    var r7 = repo.Query
                        //        .WhereUrl("?q=" + ticks++)
                        //        .WhereDimensionValue(ritem.DVIdx)
                        //        .Results();
                        //}

                    }
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void TestManyConnections()
        {
            try
            {
                //using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                //{
                //    var r6 = repo.Query.WhereUrl($"?q={0}").Results();
                //}


                var ticks = DateTime.Now.Ticks;
                //for (var ii = 0; ii < 1000000; ii++)
                Parallel.For(0, 1000, new ParallelOptions { MaxDegreeOfParallelism=12 }, (ii) =>
                   {
                       var timer = Stopwatch.StartNew();
                       using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                       {
                           try
                           {
                               //var v = ii;
                               var v = ii % 100;
                               //var v = 2;
                               var timer5 = Stopwatch.StartNew();
                               var r6 = repo.Query.WhereUrl($"?q={v}").Results();
                               timer5.Stop();
                               Console.WriteLine($"Index={ii}, Elapsed={r6.Diagnostics.ComputeTime}, Elapsed={timer5.ElapsedMilliseconds}");
                           }
                           catch (Exception ex)
                           {
                               timer.Stop();
                               if (ex.InnerException != null)
                                   Console.WriteLine($"Index={ii}, Elapsed={timer.ElapsedMilliseconds}, Error={ex.InnerException.Message}");
                               else
                                   Console.WriteLine($"Index={ii}, Elapsed={timer.ElapsedMilliseconds}, Error={ex.Message}");
                           }
                       }
                   }
                );
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static void TestSchema()
        {
            var q1 = RepositorySchema.CreateFromXml(File.ReadAllText(@"c:\temp\a.xml"));
            var q2 = RepositorySchema.CreateFromXml(File.ReadAllText(@"c:\temp\b.xml"));

            var v1 = q1.GetHashCode();
            var v2 = q2.GetHashCode();

            var c1 = q1.FieldList.Where(x => x.AllowIndex).Count();
            var c2 = q2.FieldList.Where(x => x.AllowIndex).Count();

        }

        #region TestThreading
        //private static void TestThreading()
        //{
        //    try
        //    {
        //        //using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
        //        //{
        //        //    var schema = repo.GetSchema();
        //        //    repo.UpdateSchema(schema);
        //        //}

        //        var timer = Stopwatch.StartNew();
        //        var COUNTER = 0;
        //        var tasks = new List<Task>();

        //        for (var ii = 0; ii < 20000; ii++)
        //        {
        //            var t = Task.Factory.StartNew(() =>
        //            {
        //                try
        //                {
        //                    using (var factory = SystemCoreInteractDomain.GetRepositoryFactory(SERVER, PORT))
        //                    {
        //                        var service = factory.CreateChannel();
        //                        Interlocked.Increment(ref COUNTER);
        //                        var timer2 = Stopwatch.StartNew();
        //                        service.TestHit();
        //                        timer2.Stop();
        //                        Interlocked.Decrement(ref COUNTER);
        //                        Console.WriteLine($"COUNTER={COUNTER}, Elapsed={timer2.ElapsedMilliseconds}");
        //                    }
        //                }
        //                catch (Exception ex)
        //                {
        //                    Console.WriteLine(ex.ToString());
        //                }
        //            });
        //            tasks.Add(t);

        //        }//);

        //        Console.WriteLine("Loaded Tasks");
        //        Task.WaitAll(tasks.ToArray());
        //        timer.Stop();

        //        Console.WriteLine($"Elapsed={timer.ElapsedMilliseconds}");
        //    }
        //    catch (Exception ex)
        //    {
        //        throw;
        //    }
        //}
        #endregion

        public static bool IsDatastoreReady()
        {
            try
            {
                using (var factory = SystemCoreInteractDomain.GetCoreFactory("localhost"))
                {
                    var server = factory.CreateChannel();
                    return server.IsSystemReady();
                }
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public static List<MyItem> GetItems()
        {
            var _headers = new List<DimensionItem>();
            var _dimensions = new List<DimensionItem>();
            const string FILE = @"C:\Users\DB-Server\AppData\Local\Temp\c6827c93-db6a-465c-a3f9-a1112971487c";
            using (var reader = XmlReader.Create(FILE))
            {
                var inHeaders = false;
                while (reader.Read())
                {
                    if (inHeaders && reader.Name == "h")
                    {
                        _headers.Add(new DimensionItem
                        {
                            DIdx = Convert.ToInt64("0" + reader.GetAttribute("didx")),
                            Name = reader.ReadInnerXml(),
                        });
                    }
                    if (reader.Name == "headers") inHeaders = true;
                    if (reader.Name == "dimensions") break;
                }

                DimensionItem currentD = null;
                while (reader.Read())
                {
                    if (reader.Name == "d")
                    {
                        currentD = new DimensionItem { Name = reader.GetAttribute("name"), DIdx = Convert.ToInt64(reader.GetAttribute("didx")) };
                        _dimensions.Add(currentD);
                    }
                    else if (reader.Name == "r")
                    {
                        currentD.RefinementList.Add(new RefinementItem
                        {
                            DIdx = currentD.DIdx,
                            DVIdx = Convert.ToInt64(reader.GetAttribute("dvidx")),
                            FieldValue = reader.ReadElementContentAsString(),
                        });
                    }
                    if (reader.Name == "items") break;
                }

                _dimensions.RemoveAll(x => x.DIdx == 0);
            }

            try
            {
                var retval = new List<MyItem>();
                var ordinalPosition = 0;
                using (var reader = XmlReader.Create(FILE))
                {
                    while (reader.Read())
                    {
                        var isNewItem = false;
                        var newItem = new MyItem();
                        if (reader.Name == "i")
                        {
                            isNewItem = true;
                            long.TryParse(reader.GetAttribute("ri"), out long ri);
                            int.TryParse( reader.GetAttribute("ts"), out int ts);

                            //Setup static values
                            newItem.__RecordIndex = ri;
                            newItem.__Timestamp = ts;
                            newItem.__OrdinalPosition = ordinalPosition++;

                            //Loop through all properties for this new item
                            var elementXml = reader.ReadOuterXml();
                            var doc = XDocument.Parse(elementXml);
                            var fieldIndex = 0;
                            foreach (var n in doc.Descendants().Where(x => x.Name == "v"))
                            {
                                var prop = newItem.GetType().GetProperty(_headers[fieldIndex].Name);
                                var isNull = n.Value == "~■!N";
                                if (isNull)
                                {
                                    prop.SetValue(newItem, null, null);
                                }
                                else if (prop.PropertyType == typeof(int?) || prop.PropertyType == typeof(int))
                                {
                                    prop.SetValue(newItem, int.Parse(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(DateTime?) || prop.PropertyType == typeof(DateTime))
                                {
                                    var dt = new DateTime(Convert.ToInt64(n.Value));
                                    prop.SetValue(newItem, dt, null);
                                }
                                else if (prop.PropertyType == typeof(bool?) || prop.PropertyType == typeof(bool))
                                {
                                    prop.SetValue(newItem, n.Value == "1", null);
                                }
                                else if (prop.PropertyType == typeof(Single?) || prop.PropertyType == typeof(Single))
                                {
                                    prop.SetValue(newItem, Convert.ToSingle(n.Value), null);
                                }
                                else if (prop.PropertyType == typeof(GeoCode))
                                {
                                    var geoArr = n.Value.Split('|');
                                    var geo = new GeoCode { Latitude = Convert.ToDouble(geoArr[0]), Longitude = Convert.ToDouble(geoArr[1]) };
                                    prop.SetValue(newItem, geo, null);
                                }
                                else if (prop.PropertyType == typeof(string))
                                {
                                    prop.SetValue(newItem, n.Value, null);
                                }
                                else if (prop.PropertyType == typeof(string[]))
                                {
                                    //Get real values
                                    var d = _dimensions.FirstOrDefault(x => x.DIdx == _headers[fieldIndex].DIdx);
                                    if (d != null)
                                    {
                                        var varr = n.Value.Split('|').Select(x => Convert.ToInt64(x)).ToList();
                                        var v = d.RefinementList.Where(x => varr.Contains(x.DVIdx)).Select(x => x.FieldValue).ToArray();
                                        prop.SetValue(newItem, v, null);
                                    }
                                }
                                else
                                {
                                    //TODO
                                }

                                fieldIndex++;
                            }
                        }
                        if (isNewItem) retval.Add(newItem);
                    }
                }
                return retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

    }
}
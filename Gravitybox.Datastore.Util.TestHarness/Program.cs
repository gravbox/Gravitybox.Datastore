using System;
using System.Linq;
using Gravitybox.Datastore.Common.Queryable;
using Gravitybox.Datastore.Common;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.IO;

namespace Gravitybox.Datastore.Util.TestHarness
{
    class Program
    {
        private static readonly Guid repoID = new Guid("00000000-0000-0000-0000-17728d4a8361");
        private const string SERVER = "localhost";
        private const int PORT = 1974;

        static void Main(string[] args)
        {
            if (args.Any(x => x == "/console"))
            {
                var F = new TesterForm();
                F.ShowDialog();
                return;
            }

            try
            {
                #region Wait for datastore to be ready
                //while (!IsDatastoreReady())
                //{
                //    Console.WriteLine("Waiting for Datastore ready...");
                //    System.Threading.Thread.Sleep(1000);
                //}
                #endregion

                //CreateRepo();
                //AddData();
                //DuplicateFilters();
                //TestDerivedFields();
                //HitHard();
                //Test12();
                //Test44();
                TestSchema();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            Console.WriteLine("Press <ENTER> to end...");
            Console.ReadLine();
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

                for (var ii = 0; ii < 500; ii++)
                {
                    var newItem = new MyItem
                    {
                        Project = "Hello" + ii,
                        Field1 = "V-" + (rnd.Next() % 5),
                        ID = ii,
                        MyList = new string[] { "aa", "bb" },
                    };

                    newItem.Dim2 = dimValues[rnd.Next(0, dimValues.Count)];

                    //repo.Delete.Where(X => X.ID == 1).Commit();
                    //newItem.Project = DateTime.Now.Ticks.ToString();
                    repo.InsertOrUpdate(newItem);
                    //repo.Delete.Where(x => x.ID == ii).Commit();
                    //repo.Delete.Where(x => x.ID == 999).Commit();
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
                var _rnd = new Random();
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

        private static void Test12()
        {
            try
            {
                var startDate = DateTime.Now.AddDays(-4).Date;
                var endDate = DateTime.Now.AddDays(-3).Date;
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    var query = repo.Query
                        //.WhereUrl("?d=1800000")
                        //.Where(x => startDate <= x.CreatedDate)
                        //.Where(x => x.CreatedDate < endDate)
                        //.OrderByDescending(x => x.CreatedDate)
                        .RecordsPerPage(20)
                        .SkipDimension(1)
                        .SkipDimension(2);

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

        private static void Test44()
        {
            try
            {
                var ticks = DateTime.Now.Ticks;
                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    for (var ii = 0; ii < 100; ii++)
                    {
                        var r6 = repo.Query
                            .WhereUrl("?q=" + ticks++)
                            .Results();

                        foreach (var ritem in r6.AllDimensions.SelectMany(x => x.RefinementList))
                        {
                            var r7 = repo.Query
                                .WhereUrl("?q=" + ticks++)
                                .WhereDimensionValue(ritem.DVIdx)
                                .Results();
                        }

                    }
                }
                return;



                using (var repo = new DatastoreRepository<MyItem>(repoID, SERVER, PORT))
                {
                    var r6 = repo.Query
                        .WhereUrl("?d=1800000")
                        .RecordsPerPage(2)
                        .PageOffset(3)
                        .GroupBy(x => x.Field1)
                        .Select(x => new
                        {
                            XX = x.Key,
                            MaxId = x.Max(z => z.ID),
                            MinQ = x.Min(z => z.ID),
                            C = x.Count(),
                        })
                        //.OrderBy(x => x.MaxId)
                        .Items()
                        ;

                    var r7 = repo.Query
                        .WhereUrl("?d=1800000")
                        .RecordsPerPage(5)
                        .PageOffset(2)
                        .OrderByDescending(x => x.ID)
                        .Select(x => new
                        {
                            MaxId = x.ID,
                            X = x.Field1,
                        })
                        .Items()
                        ;

                    var ii = 0;

                    //        foreach(var item in r6)
                    //        {
                    //            var q1 = item.MaxId;
                    //            var w1 = item.MinQ;
                    //        }

                    //        #region OLD
                    //        var r1 = repo.Query
                    //            .WhereUrl("?d=1800000")
                    //            .PageOffset(1)
                    //            .RecordsPerPage(20)
                    //            .Results();

                    //        var q = r1.Items
                    //           .GroupBy(x => x.Field1)
                    //           .Select(x => new
                    //           {
                    //               x.Key,
                    //               MaxId = x.Max(z => z.ID),
                    //               Count = x.Count()
                    //           })
                    //           .ToList()
                    //           ;

                    //        //var r5 = repo.Query
                    //        //            .WhereUrl("?d=1800000")
                    //        //            .FieldMax(x => x.ID)
                    //        //            .FieldMin(x => x.ID)
                    //        //            .IncludeRecords(false)
                    //        //            .IncludeDimensions(false)
                    //        //            .ExcludeCount(true)
                    //        //            .Results();
                    //        //.Aggregate(x => x.Field1);
                    //        //.Aggregate(x => new { x.Field1, x.ID });

                    //        //var r2 = repo.Query
                    //        //    .WhereUrl("?d=1800000")
                    //        //    .PageOffset(2)
                    //        //    .RecordsPerPage(20)
                    //        //    .Select(x => new { x.CreatedDate, x.Field1, x.ID })
                    //        //    .Items();

                    //        //var r3 = repo.Query
                    //        //    .WhereUrl("?d=1800000")
                    //        //    .PageOffset(2)
                    //        //    .RecordsPerPage(20)
                    //        //    .GroupBy(x => x.Field1)
                    //        //    .SelectIt(x => new
                    //        //    {
                    //        //        x.Key,
                    //        //        MaxId = x.Max(z => z.ID),
                    //        //        Count = x.Count()
                    //        //    })
                    //        //    .Items()
                    //        //    //.Items(x => x.Field1,
                    //        //    //z => new
                    //        //    //{
                    //        //    //    z.Field1,
                    //        //    //    MaxId = z.ID,
                    //        //    //})
                    //        //    ;
                    //        #endregion

                }
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

    }
}
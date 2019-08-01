using Osrm.Client;
//using Osrm.Client.Models;
//using Osrm.Client.Models.Responses;
using Osrm.Client.v5;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Yandex.Geocoder;

namespace ConsoleAppDiscoverCity
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            DateTime datetimeNow = DateTime.Now;

            DataTable dbTbl = new DataTable("houses71dd");

            OleDbConnection dbConn = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\home91\Downloads\fias_dbf;Extended Properties=dBASE IV;");

            dbConn.Open();

            string sqlSelect = $"select HOUSE71.HOUSEGUID, ADDROB71.PARENTGUID, ADDROB71.AOGUID, ADDROB71.FORMALNAME, ADDROB71.SHORTNAME, HOUSE71.HOUSENUM, ADDROB71.ENDDATE, HOUSE71.ENDDATE from ADDROB71, HOUSE71 where ADDROB71.AOGUID = HOUSE71.AOGUID and ADDROB71.POSTALCODE in ('300021','300016','300027')";
            //string sqlSelect = $"select ADDROB71.PARENTGUID, ADDROB71.AOGUID, ADDROB71.FORMALNAME, ADDROB71.SHORTNAME, HOUSE71.HOUSENUM from ADDROB71, HOUSE71 where ADDROB71.AOGUID = HOUSE71.AOGUID and ADDROB71.ENDDATE > {datetimeNow.ToString("dd/MM/yyyy")} and HOUSE71.ENDDATE > {datetimeNow.ToString("dd/MM/yyyy")} and ADDROB71.POSTALCODE = '300057'";
            //string sqlSelect = $"select * from ADDROB71, HOUSE71 where ADDROB71.AOGUID = HOUSE71.AOGUID and ADDROB71.ENDDATE > {datetimeNow.ToString("MM/dd/yyyy")} and HOUSE71.ENDDATE > {datetimeNow.ToString("MM/dd/yyyy")} and ADDROB71.POSTALCODE = '300057'";
            //string sqlSelect = $"select * from ADDROB71, HOUSE71 where ADDROB71.AOGUID = HOUSE71.AOGUID and ADDROB71.ENDDATE > @value1 and HOUSE71.ENDDATE > @value1 and ADDROB71.POSTALCODE = '300057'";

            OleDbCommand dbComm = new OleDbCommand(sqlSelect, dbConn);
            //dbComm.Parameters.AddWithValue("@value1", datetimeNow);
            //dbComm.Parameters.AddWithValue("@value2", datetimeNow);
            OleDbDataAdapter dbAdap = new OleDbDataAdapter(dbComm);

            dbAdap.Fill(dbTbl);

            dbTbl.Columns.Add("ADDRFULL", typeof(String));
            dbTbl.Columns.Add("POINTLAT", typeof(Double));
            dbTbl.Columns.Add("POINTLNG", typeof(Double));

            Console.WriteLine($"Count {dbTbl.Rows.Count} rows from DB.");
            dbTbl.Rows.Cast<DataRow>().All(r =>
            {

                if (((DateTime)r["ADDROB71.ENDDATE"] < datetimeNow) || ((DateTime)r["HOUSE71.ENDDATE"] < datetimeNow))
                {
                    r.Delete();
                    return true;
                }

                string SFN = $"{ GetShortAndFormalNames(dbConn, r["PARENTGUID"].ToString())}, {r["SHORTNAME"]} {r["FORMALNAME"]} {r["HOUSENUM"]}";

                var geocoder = new YandexGeocoder
                {
                    Apikey = "a1d0badd-df1d-4814-8d43-eab723c50133",
                    SearchQuery = SFN,
                    Results = 1,
                    LanguageCode = LanguageCode.en_RU
                };

                var getRes = geocoder.GetResults();

                if (getRes.Count == 0)
                {
                    r.Delete();
                    return true;
                }

                LocationPoint pnt = getRes[0].Point;

                Console.WriteLine($"{SFN} [ {pnt.Longitude}, {pnt.Latitude} ]");

                r["ADDRFULL"] = SFN;
                r["POINTLAT"] = pnt.Latitude;
                r["POINTLNG"] = pnt.Longitude;

                return true;
            });

            dbTbl.AcceptChanges();
            Console.WriteLine($"Count {dbTbl.Rows.Count} rows is live.");

            //string TBLNAME_DISDUR = $"DISDUR{ datetimeNow.ToString("ddMMyyyy")}";
            string TBLNAME_DISDUR = Path.GetRandomFileName();
            {
                OleDbCommand cmd = dbConn.CreateCommand();

                cmd.CommandText = $"CREATE TABLE {TBLNAME_DISDUR} (SRC_HOUSEGUID CHAR(36), DST_HOUSEGUID CHAR(36), DISTANCE DECIMAL, DURATION DECIMAL)";
                cmd.ExecuteNonQuery();
            }


            var osrm = new Osrm5x("http://router.project-osrm.org/", "v1", "car");
            //var result = osrm.Nearest(new Location(52.4224, 13.333086));
            //NearestResponse result = TryNearest(osrm);

            //result.Waypoints.All(w =>
            //{
            //    Console.WriteLine($"{w.Location.Longitude},{w.Location.Latitude}");
            //    return true;
            //});

            dbTbl.Rows.Cast<DataRow>().All(rowSrc =>
            {
                dbTbl.Rows.Cast<DataRow>().All(rowDst =>
                {
                    var locations = new Location[] {
                        new Location((Double)rowSrc["POINTLAT"], (Double)rowSrc["POINTLNG"]),
                        new Location((Double)rowDst["POINTLAT"], (Double)rowDst["POINTLNG"]),
                    };

                    var routeResult = TryRoute(osrm, locations);

                    Console.WriteLine($"{rowSrc["ADDRFULL"]} > {rowDst["ADDRFULL"]} DIS {routeResult.Routes[0].Distance} DUR {routeResult.Routes[0].Duration}");

                    if ((routeResult.Routes[0].Distance < 3000.0) && (routeResult.Routes[0].Duration < 600.0))
                    {
                        OleDbCommand cmd = dbConn.CreateCommand();

                        cmd.CommandText = $"INSERT INTO {TBLNAME_DISDUR} VALUES ('{rowSrc["HOUSEGUID"]}', '{rowDst["HOUSEGUID"]}', {routeResult.Routes[0].Distance}, {routeResult.Routes[0].Duration})";
                        cmd.ExecuteNonQuery();
                    }

                    return true;
                });

                return true;
            });

            dbConn.Close();
            dbConn.Dispose();

            dbTbl.WriteXml("house71dd.xml");
        }

        private static Osrm.Client.Models.RouteResponse TryRoute(Osrm5x osrm, Location[] locations)
        {

            //var result = osrm.Route(locations);
            do
            {
                try
                {
                    return osrm.Route(new Osrm.Client.Models.RouteRequest()
                    {
                        Coordinates = locations,
                        Steps = true,
                        Alternative = true

                    });
                }
                catch (Exception e)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            } while (true);
        }

        private static object GetShortAndFormalNames(OleDbConnection dbConn, string v)
        {
            if (String.IsNullOrEmpty(v)) return String.Empty;

            OleDbCommand dbComm = new OleDbCommand($"select AOGUID, FORMALNAME, SHORTNAME, PARENTGUID from ADDROB71 where AOGUID = '{v}'", dbConn);
            OleDbDataAdapter dbAdap = new OleDbDataAdapter(dbComm);

            DataTable tbl = new DataTable();
            dbAdap.Fill(tbl);

            return $"{GetShortAndFormalNames(dbConn, tbl.Rows[0]["PARENTGUID"].ToString())}, {tbl.Rows[0]["SHORTNAME"]} {tbl.Rows[0]["FORMALNAME"]}";
        }

        private static Osrm.Client.Models.Responses.NearestResponse TryNearest(Osrm5x osrm)
        {
            do
            {
                try
                {
                    return osrm.Nearest(new Osrm.Client.Models.NearestRequest() { Number = 10, Coordinates = new Location[] { new Location(54.186606, 37.627486) } });
                }
                catch (Exception e)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            } while (true);
        }
    }
}

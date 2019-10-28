using Osrm.Client;
using Osrm.Client.v5;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleAppMergingDisDur
{
    class Program
    {
        private static OleDbConnection dbConn = null;
        private static string TBLNAME_DISDUR = Path.GetRandomFileName();

        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            DateTime datetimeNow = DateTime.Now;

            var osrm = new Osrm5x("http://router.project-osrm.org/", "v1", "car");

            dbConn = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\home91\Downloads\fias_dbf;Extended Properties=dBASE IV;");

            dbConn.Open();
            {
                OleDbCommand cmd = dbConn.CreateCommand();

                //cmd.CommandText = $"CREATE TABLE {TBLNAME_DISDUR} (SRC_HOUSEGUID CHAR(36), DST_HOUSEGUID CHAR(36), DISTANCE DECIMAL, DURATION DECIMAL)";
                cmd.CommandText = $"CREATE TABLE {TBLNAME_DISDUR} (WHENQUERY DATETIME, SRC_LAT DECIMAL, SRC_LNG DECIMAL, DST_LAT DECIMAL, DST_LNG DECIMAL, DISTANCE DECIMAL, DURATION DECIMAL)";
                cmd.ExecuteNonQuery();
            }

            foreach (string argv in args)
            {
                OleDbCommand dbCmd = new OleDbCommand($"select * from {argv}", dbConn);

                OleDbDataReader dbRdr = dbCmd.ExecuteReader();

                while (dbRdr.Read())
                {
                    //Console.WriteLine(dbRdr[0].ToString());
                    var src_lat = dbRdr["SRC_LAT"];
                    var src_lng = dbRdr["SRC_LNG"];
                    var dst_lat = dbRdr["DST_LAT"];
                    var dst_lng = dbRdr["DST_LNG"];
                    var dist = dbRdr["DISTANCE"];
                    var dura = dbRdr["DURATION"];

                    //AddNewRoute(osrm, src_lat, src_lng, dst_lat, dst_lng);
                    AddNewLocation(osrm, new Location((Double)src_lat, (Double)src_lng));
                    AddNewLocation(osrm, new Location((Double)dst_lat, (Double)dst_lng));
                }
                dbRdr.Close();
            }

            dbConn.Close();
        }

        private static List<Location> Locs = new List<Location>();

        private static void AddNewLocation(Osrm5x osrm, Location location)
        {
            //Locs.Find(l => { return l == location; });
            if (!Locs.Exists(l => { return l == location; }))
            {
                foreach (Location loc in Locs)
                {
                    //Console.Write(loc.ToString());
                    AddNewRoute(osrm, location, loc);
                    AddNewRoute(osrm, loc, location);
                    //Console.WriteLine();
                }

                Locs.Add(location);
            }
        }

        private static void AddNewRoute(Osrm5x osrm, Location location, Location loc)
        {
            var locations = new Location[] { location, loc };

            var routeResult = TryRoute(osrm, locations);

            Console.WriteLine($"{location.Latitude};{location.Longitude} {loc.Latitude};{loc.Longitude} DIS {routeResult.Routes[0].Distance} DUR {routeResult.Routes[0].Duration}");

            if ((routeResult.Routes[0].Distance < 3000.0) && (routeResult.Routes[0].Duration < 600.0))
            {
                OleDbCommand cmd = dbConn.CreateCommand();

                //cmd.CommandText = $"INSERT INTO {TBLNAME_DISDUR} VALUES ('{rowSrc["HOUSEGUID"]}', '{rowDst["HOUSEGUID"]}', {routeResult.Routes[0].Distance}, {routeResult.Routes[0].Duration})";
                cmd.CommandText = $"INSERT INTO {TBLNAME_DISDUR} VALUES ({DateTime.Now.ToShortDateString()}, {location.Latitude}, {location.Longitude}, {loc.Latitude}, {loc.Longitude}, {routeResult.Routes[0].Distance}, {routeResult.Routes[0].Duration})";
                cmd.ExecuteNonQuery();
            }
        }

        private static void AddNewRoute(Osrm5x osrm, object src_lat, object src_lng, object dst_lat, object dst_lng)
        {
            var locations = new Location[] {
                        new Location((Double)src_lat, (Double)src_lng),
                        new Location((Double)dst_lat, (Double)dst_lng),
                    };

            var routeResult = TryRoute(osrm, locations);
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
    }
}

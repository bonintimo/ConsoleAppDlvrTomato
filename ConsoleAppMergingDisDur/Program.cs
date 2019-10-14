using Osrm.Client;
using Osrm.Client.v5;
using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleAppMergingDisDur
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            DateTime datetimeNow = DateTime.Now;

            var osrm = new Osrm5x("http://router.project-osrm.org/", "v1", "car");

            OleDbConnection dbConn = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\home91\Downloads\fias_dbf;Extended Properties=dBASE IV;");

            dbConn.Open();

            foreach(string argv in args)
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

                    var locations = new Location[] {
                        new Location((Double)src_lat, (Double)src_lng),
                        new Location((Double)dst_lat, (Double)dst_lng),
                    };

                    var routeResult = TryRoute(osrm, locations);
                }
                dbRdr.Close();
            }

            dbConn.Close();
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

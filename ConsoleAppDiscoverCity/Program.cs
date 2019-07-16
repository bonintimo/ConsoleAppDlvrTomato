using Osrm.Client;
using Osrm.Client.Models.Responses;
using Osrm.Client.v5;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
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

            DataTable dbTbl = new DataTable();

            OleDbConnection dbConn = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\home91\Downloads\fias_dbf;Extended Properties=dBASE IV;");

            dbConn.Open();

            string sqlSelect = "select ADDROB71.PARENTGUID, ADDROB71.AOGUID, ADDROB71.FORMALNAME, ADDROB71.SHORTNAME, HOUSE71.HOUSENUM from ADDROB71, HOUSE71 where ADDROB71.AOGUID = HOUSE71.AOGUID and ADDROB71.POSTALCODE = '300057'"; 

            OleDbCommand dbComm = new OleDbCommand(sqlSelect, dbConn);
            OleDbDataAdapter dbAdap = new OleDbDataAdapter(dbComm);

            dbAdap.Fill(dbTbl);

            dbTbl.Rows.Cast<DataRow>().All(r=> {

                string SFN = $"{ GetShortAndFormalNames(dbConn, r["PARENTGUID"].ToString())}, {r["SHORTNAME"]} {r["FORMALNAME"]} {r["HOUSENUM"]}";

                var geocoder = new YandexGeocoder
                {
                    Apikey= "a1d0badd-df1d-4814-8d43-eab723c50133",
                    SearchQuery = SFN,
                    Results = 1,
                    LanguageCode = LanguageCode.en_RU
                };

                LocationPoint pnt = geocoder.GetResults()[0].Point;

                Console.WriteLine($"{SFN}, {pnt.Longitude}, {pnt.Latitude}");

                return true;
            });

            dbConn.Close();
            dbConn.Dispose();

            var osrm = new Osrm5x("http://router.project-osrm.org/", "v1", "car");
            //var result = osrm.Nearest(new Location(52.4224, 13.333086));
            NearestResponse result = TryNearest(osrm);

            result.Waypoints.All(w =>
            {
                Console.WriteLine($"{w.Location.Longitude},{w.Location.Latitude}");
                return true;
            });
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

        private static NearestResponse TryNearest(Osrm5x osrm)
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

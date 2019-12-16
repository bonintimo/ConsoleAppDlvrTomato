using System;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Json;
using System.Net;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Xml.Serialization;
using System.Diagnostics;
using System.Threading;
using System.Globalization;
using System.Device.Location;
using System.Activities.Statements;
using SimpleHttp;

namespace orcplan
{
    //public class RestaurantMachine : StateMachine
    //{
    //    public RestaurantMachine()
    //    {
    //    }

    //    public override bool Equals(object obj)
    //    {
    //        return base.Equals(obj);
    //    }

    //    public override int GetHashCode()
    //    {
    //        return base.GetHashCode();
    //    }

    //    public override string ToString()
    //    {
    //        return base.ToString();
    //    }
    //}

    enum OINFO_STATE { UNDEFINE = 100, BEGINNING = 110, COOKING = 120, READY = 130, TRANSPORTING = 140, PLACING = 150, ENDED = 160 };
    enum RINFO_STATE { UNDEFINE = 200, OFFLINE = 210, ONLINE = 220, BRAKEDOWN = 230 };
    enum CINFO_STATE { UNDEFINE = 300, OFFLINE = 310, ONLINE = 320, BRAKEDOWN = 330, ONROAD = 340 };

    static class MainClass
    {

        public static int MAX_RESTAURANTS_FOR_PLANNING = 2;
        public static int MAX_COURIERS_FOR_PLANNING = 3;
        public static int MAX_BEGINING_ORDERS_TO_ADD = 1;
        public static int MAX_ORDERS_FOR_COURIERS = 3;

        private static List<Task> taskList = new List<Task>();

        private static DateTime TimeOfSimulation = DateTime.MinValue;

        private static DateTime PriorTimeSimulation = DateTime.MinValue;

        private static string BaseDirectoryForDPR = String.Empty;
        private static string BaseDirectoryForCDP = String.Empty;

        public static void Main(string[] args)
        {
            Console.WriteLine($"Delivery Planning System");
            Console.WriteLine($"{Environment.CurrentDirectory}");
            Console.WriteLine($"PC:{Environment.ProcessorCount} CLR:{Environment.Version} WS:{Environment.WorkingSet}");
            Console.WriteLine();

            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            //TspDoTest(args);

            //Console.WriteLine("Hello World!");

            ReadBgnnOrders(@"./ORDERS-2018-10-19-TM3TM18.tsv");
            //ReadBgnnOrders(@"");

            CreateSchemaForDeliveryPlan(args);

            StartWebServer();

            if (File.Exists("georouteinfo.json"))
            {
                GeoRouteInfo = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, GeoRouteData>>(File.ReadAllText("georouteinfo.json"));
                GeoRouteInfoQuantity = GeoRouteInfo.Count;
            }

            bool isContinue = true;
            DataSet deliveryPlan = null, nextPlan = null;
            OINFO_STATE stateNext = OINFO_STATE.UNDEFINE;
            do
            {

                //Console.WriteLine("Write the file name with a delivery plan:");
                Console.WriteLine();
                Console.Write($"{BaseDirectoryForDPR} Type a command:");
                string filePlan = Console.ReadLine();

                switch (filePlan.ToUpper())
                {
                    case "EXIT":
                        isContinue = false;
                        break;

                    case "INIT":
                        InitBaseDirForDPR();
                        deliveryPlan = ReadPlan(@"./tula-all-empty-R2C4.xml");// ReadTestPlan();
                        nextPlan = PlanningForOrders(deliveryPlan);
                        break;

                    case "NEXT":
                        stateNext = ApplyNextEvent(nextPlan);
                        deliveryPlan = nextPlan;
                        if (IsNeedPlanning(stateNext))
                            nextPlan = PlanningForOrders(deliveryPlan);
                        break;

                    case "RUN":
                        while ((nextPlan.Tables[tblOINFO].Rows.Count > 0) || (BgnnOrders.Rows.Count > 0))
                        {
                            stateNext = ApplyNextEvent(nextPlan);
                            deliveryPlan = nextPlan;
                            if (IsNeedPlanning(stateNext))
                                nextPlan = PlanningForOrders(deliveryPlan);
                        }
                        break;

                    default:
                        Console.WriteLine("Unknown command!!!");
                        break;
                }
                //DataSet deliveryPlan = ReadPlan(filePlan);// ReadTestPlan();

                //PlanningForOrders(DateTime.Parse(@"2018-09-10T11:55:00+03:00"), deliveryPlan);

                GC.Collect();
            }
            while (isContinue);

            //tbl.Loa
            //WebClient client = new WebClient();
            //client.Headers["accept"] = "application/json";
            //client.Headers["content-type"] = "application/json";
            //NameValueCollection values = new NameValueCollection();
            //values["data"]= "{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": 37.644166, \"y\": 54.202089 }, { \"type\": \"pedo\", \"x\": 37.690007, \"y\": 54.209826 } ], \"type\": \"jam\", \"output\":\"simple\"}";
            //var response = client.UploadValues( @"http://catalog.api.2gis.ru//carrouting/4.0.0/tula/?key=rueejy0019" , values);


            //string r = GetGeoPathTotal( "54.202089", "37.644166", "54.202089", "37.644166");

            //JsonValue json = JsonValue.Parse(r);
            //var dur = json["result"][0]["duration"];

            //var len = json["result"][0]["length"];

            //Task.WaitAll(taskList.ToArray());

            //Console.WriteLine($"GeoRouteInfo.Count: {GeoRouteInfo.Count}");
        }

        private static int GeoRouteInfoQuantity = 0;

        private static void StoreGeoRouteInfo()
        {
            if (GeoRouteInfo.Count != GeoRouteInfoQuantity)
            {
                string json = Newtonsoft.Json.JsonConvert.SerializeObject(GeoRouteInfo, Newtonsoft.Json.Formatting.Indented);
                File.WriteAllText("georouteinfo.json", json);

                GeoRouteInfoQuantity = GeoRouteInfo.Count;
            }
        }

        private static void StartWebServer()
        {
            //SimpleHttp.Route.Add(
            //    "/actor",
            //    (rqweb, rpweb, argsweb) =>
            //    {
            //    });

            SimpleHttp.Route.Add(
                "/actor/{aname}/{atime}/{alat}/{alng}/",
                (rqweb, rpweb, argsweb) =>
                {
                    rpweb.AsText($"Hi, {argsweb["aname"]}! Your {argsweb["atime"]} at {argsweb["alat"]} {argsweb["alng"]} is received {DateTime.Now.ToString()}");
                });

            SimpleHttp.Route.Add(
                "/",
                (rqweb, rpweb, argsweb) =>
                {
                    if (TheWorkDeliveryPlan != null)
                    {
                        StringBuilder sb = HtmlPlanBuilder(TheWorkDeliveryPlan, "Delivery Planning System", "<meta http-equiv=\"refresh\" content=\"15\">");
                        //rpweb.AsText(sb.ToString(), "");
                        //rpweb.AddHeader("meta", "http-equiv=\"refresh\" content=\"15\"");
                        rpweb.AsBytes(rqweb, Encoding.UTF8.GetBytes(sb.ToString()), "text/html");
                    }
                    else
                    {
                        rpweb.AsText("<html><body>Welcome to Delivery Planning System</body></html>");
                    }
                });

            SimpleHttp.HttpServer.ListenAsync(8787, CancellationToken.None, SimpleHttp.Route.OnHttpRequestAsync).ContinueWith(
                new Action<Task>((t) => { Console.WriteLine("HTTP server has been stopped"); })
                );
        }

        private static void InitBaseDirForDPR()
        {
            BaseDirectoryForDPR = $"{DateTime.Now.ToString("yyyy-MM-dd(HH-mm)")}R{MAX_RESTAURANTS_FOR_PLANNING}C{MAX_COURIERS_FOR_PLANNING}O{MAX_ORDERS_FOR_COURIERS}]";
            Directory.CreateDirectory(BaseDirectoryForDPR);
            File.AppendAllText(Path.Combine(BaseDirectoryForDPR, "EL-O-RC.tsv"), "buildt\tcntTB\tcntTC\tcntTR\tcntTT\tcntTP\tcntTE\tdir\tspanTime\tbeginDateTime\tfinishDateTime\tGeoRouteInfoCount\n");
            File.AppendAllText(Path.Combine(BaseDirectoryForDPR, "OA-O-RC.tsv"), "colOINFO_TB\tcolOINFO_TOT\tcolOINFO_TP\tcolOINFO_TD\tcolOINFO_OID\tcolOINFO_ADDRESS\tcolOINFO_LAT\tcolOINFO_LNG\tcolOINFO_RID\tcolOINFO_TC\tcolOINFO_TR\tcolOINFO_CID\tcolOINFO_TT\tcolOINFO_TE\n");
        }

        private static bool IsNeedPlanning(OINFO_STATE stateNext)
        {
            switch (stateNext)
            {
                case OINFO_STATE.BEGINNING:
                //case OINFO_STATE.COOKING:
                case OINFO_STATE.READY:
                case OINFO_STATE.TRANSPORTING:
                case OINFO_STATE.PLACING:
                    //case OINFO_STATE.ENDED:
                    return true;
                    break;
            }
            return false;
        }

        private static DataTable BgnnOrders = null;

        private static void ReadBgnnOrders(string v)
        {
            //
            //string[] fileBgnnOrders = File.ReadAllLines(v);

            BgnnOrders = ConvertTSVtoDataTable(v);
        }

        private static DataTable ConvertTSVtoDataTable(string strFilePath)
        {
            DataTable dt = new DataTable();

            dt.Columns.Add("OID", typeof(string));
            dt.Columns.Add("ADDRESS", typeof(string));
            dt.Columns.Add("TB", typeof(DateTime));
            dt.Columns.Add("DURATION", typeof(TimeSpan));
            dt.Columns.Add("TOT", typeof(DateTime));

            if (!String.IsNullOrEmpty(strFilePath))
                using (StreamReader sr = new StreamReader(strFilePath))
                {
                    string[] headers = sr.ReadLine().Split('\t');
                    //foreach (string header in headers)
                    //{
                    //    dt.Columns.Add(header);
                    //}
                    while (!sr.EndOfStream)
                    {
                        string[] rows = sr.ReadLine().Split('\t');

                        if (rows.Length == 0)
                        {
                            continue;
                        }

                        DataRow dr = dt.NewRow();
                        //for (int i = 0; i < headers.Length; i++)
                        //{
                        //    dr[i] = rows[i];
                        //}
                        {
                            string[] oid = rows[0].Split(' ');
                            dr["OID"] = oid[1];
                        }
                        {
                            dr["ADDRESS"] = rows[1];
                        }
                        {
                            DateTime tb = DateTime.ParseExact(rows[2], "dd.MM.yyyy HH:mm:ss", CultureInfo.InvariantCulture);
                            dr["TB"] = tb;
                        }
                        {
                            dr["DURATION"] = TimeSpan.ParseExact(rows[3], "mm", CultureInfo.InvariantCulture);
                        }
                        {
                            dr["TOT"] = !String.IsNullOrEmpty(rows[4]) ? DateTime.ParseExact(rows[4], "dd.MM.yyyy HH:mm:ss", CultureInfo.InvariantCulture) : dr["TB"];
                        }

                        dt.Rows.Add(dr);
                    }

                }
            dt.AcceptChanges();

            dt.DefaultView.Sort = "TB";
            dt = dt.DefaultView.ToTable();

            return dt;
        }

        private static void GetYandexLatLng(DataRow dr)
        {
            string requestString = String.Concat("https://geocode-maps.yandex.ru/1.x/?apikey=a1d0badd-df1d-4814-8d43-eab723c50133&format=json&geocode=", WebUtility.HtmlEncode(dr["ADDRESS"].ToString()));

            var request = (HttpWebRequest)WebRequest.Create(requestString);
            //request.Headers[]=;

            //var postData = String.Concat("{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": ", lngS, ", \"y\": ", latS, " }, { \"type\": \"pedo\", \"x\": ", lngD, ", \"y\": ", latD, " } ], \"type\": \"jam\", \"output\":\"simple\"}");

            GeoRouteData geoInfo = null;// String.Empty;
            if (!GeoRouteInfo.TryGetValue(requestString, out geoInfo))
            {

                //var data = Encoding.ASCII.GetBytes(postData);

                request.Method = "GET";
                //request.ContentType = "application/x-www-form-urlencoded";
                //request.ContentLength = data.Length;

                //using (var stream = request.GetRequestStream())
                //{
                //    stream.Write(data, 0, data.Length);
                //}

                var response = (HttpWebResponse)request.GetResponse();

                var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                geoInfo = new GeoRouteData() { TimeMark = DateTime.MinValue, GeoResponse = responseString.ToString() };
                GeoRouteInfo.Add(requestString, geoInfo);
                response.Close();
            }

            JsonValue json = JsonValue.Parse(geoInfo.GeoResponse);

            JsonArray jsonArr = (JsonArray)json["response"]["GeoObjectCollection"]["featureMember"];
            JsonValue jsonVal = null;

            if (jsonArr.Count == 1)
            {
                jsonVal = jsonArr[0];
            }
            else
            {
                foreach (JsonValue v in jsonArr)
                {
                    if (!v.ToString().Contains("district"))
                    {
                        jsonVal = v;
                        break;
                    }
                }
            }

            string[] latlng = jsonVal["GeoObject"]["Point"]["pos"].ToString().Replace("\"", "").Split(' ');
            dr["LAT"] = latlng[1];
            dr["LNG"] = latlng[0];
        }

        private static OINFO_STATE ApplyNextEvent(DataSet nextPlan)
        {
            //
            OINFO_STATE stateNext = OINFO_STATE.UNDEFINE;

            var planEvents = nextPlan.Tables[tblOINFO].Select().Where<DataRow>(row =>
         {
             return (((OINFO_STATE)row[colOINFO_STATE]) != OINFO_STATE.BEGINNING)
             || ((((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.BEGINNING) && (((TimeSpan)row[colOINFO_TD]) >= TimeSpan.FromTicks(0)));
         }).OrderBy<DataRow, DateTime>(row =>
          {
              return GetDateTimeEvent(row);
          });

            DataRow firstEvent = planEvents.FirstOrDefault();
            if (firstEvent != null)
            {
                DateTime dtEvent = GetDateTimeEvent(firstEvent);

                if (dtEvent < TimeOfSimulation) dtEvent = TimeOfSimulation;

                if ((BgnnOrders.Rows.Count > 0) && (dtEvent > (DateTime)BgnnOrders.Rows[0]["TB"]))
                {
                    stateNext = InsertBeginningOrderToPlan(nextPlan);
                }
                else
                {
                    WaitSimulationFor(dtEvent);

                    stateNext = DoNextState(firstEvent, nextPlan, dtEvent);
                    nextPlan.Tables["SUMMARY"].Rows[0]["BUILDT"] = dtEvent;
                    nextPlan.AcceptChanges();
                }
            }
            else
            {
                if ((BgnnOrders.Rows.Count > 0))
                {
                    stateNext = InsertBeginningOrderToPlan(nextPlan);
                }
            }

            UpdateCourierGeoPosition(nextPlan);

            UpdateCourierState(nextPlan);

            return stateNext;
        }

        private static void WaitSimulationFor(DateTime dtEvent)
        {
            TimeSpan span = dtEvent - PriorTimeSimulation;

            if (span > TimeSpan.Zero)
            {
                TimeSpan w = TimeSpan.FromTicks(span.Ticks / 64);

                Console.WriteLine($"wait {w} ... until {DateTime.Now.Add(w)}");
                Thread.Sleep(w);
            }

            PriorTimeSimulation = dtEvent;
        }

        private static void UpdateCourierState(DataSet nextPlan)
        {

            nextPlan.Tables[tblCINFO].Select().All<DataRow>(rowCinfo =>
            {
                //DataRow[] 
                var ordsCinfo = nextPlan.Tables[tblOINFO].Select().Where<DataRow>(oRow =>
                {
                    return (oRow[colOINFO_CID].ToString() == rowCinfo[colCINFO_CID].ToString());
                });//.ToArray();

                //DataRow[] 
                var ordsTrans = ordsCinfo.Where<DataRow>(oRow =>
                {
                    return (((OINFO_STATE)oRow[colOINFO_STATE]) == OINFO_STATE.TRANSPORTING);
                });//.ToArray();

                if (((CINFO_STATE)rowCinfo[colCINFO_STATE]) == CINFO_STATE.ONROAD)
                {
                    if (ordsTrans.Count() == 0)
                    {
                        rowCinfo[colCINFO_STATE] = CINFO_STATE.ONLINE;
                        return true;
                    }
                }

                if (((CINFO_STATE)rowCinfo[colCINFO_STATE]) == CINFO_STATE.ONLINE)
                {
                    if ((ordsTrans.Count() >= MAX_ORDERS_FOR_COURIERS)
                    || ((ordsTrans.Count() > 0) && (ordsCinfo.Count() > MAX_ORDERS_FOR_COURIERS)))
                    {
                        rowCinfo[colCINFO_STATE] = CINFO_STATE.ONROAD;
                        return true;
                    }
                }

                return true;
            });

            nextPlan.AcceptChanges();
        }

        private static void UpdateCourierGeoPosition(DataSet nextPlan)
        {
            DateTime buildt = (DateTime)nextPlan.Tables["SUMMARY"].Rows[0]["BUILDT"];

            nextPlan.Tables[tblCINFO].Select().All<DataRow>(rowCinfo =>
            {
                int ROUTELENGTH = (int)rowCinfo[colCINFO_ROUTELENGTH];

                if (ROUTELENGTH > 0) return true;

                DateTime TOS = (DateTime)rowCinfo[colCINFO_TOS];

                DataRow rInfo = nextPlan.Tables[tblRINFO].Rows.Find((string)rowCinfo[colCINFO_RID4S]);
                string latS = (string)rowCinfo[colCINFO_LAT];
                string lngS = (string)rowCinfo[colCINFO_LNG];
                string latD = (string)rInfo[colRINFO_LAT];
                string lngD = (string)rInfo[colRINFO_LNG];

                {
                    //string r = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                    GeoRouteData r = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                    //JsonValue json = JsonValue.Parse(r);

                    //double distBet = GetGeoPathDistance(latS, lngS, latD, lngD);

                    //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                    //int lll = int.Parse(json["result"][0]["length"].ToString());
                    //int ddd = (int)distBet / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                    //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                    //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                    //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

                    if (r.Duration == 0) return true;

                    TOS = TOS.AddSeconds(r.Duration);
                }

                if (TOS < buildt)
                {
                    rowCinfo[colCINFO_LAT] = latD;
                    rowCinfo[colCINFO_LNG] = lngD;
                    rowCinfo[colCINFO_TOS] = TOS;
                }

                return true;
            });

            nextPlan.AcceptChanges();
        }

        private static OINFO_STATE InsertBeginningOrderToPlan(DataSet nextPlan)
        {
            DateTime tb = (DateTime)BgnnOrders.Rows[0]["TB"];
            nextPlan.Tables["SUMMARY"].Rows[0]["BUILDT"] = tb;

            if (PriorTimeSimulation > DateTime.MinValue)
            {
                WaitSimulationFor(tb);
            }
            else
            {
                PriorTimeSimulation = tb;
            }

            InsertBgnnOrder(BgnnOrders.Rows[0], nextPlan);
            BgnnOrders.Rows.RemoveAt(0);
            BgnnOrders.AcceptChanges();

            nextPlan.AcceptChanges();
            return OINFO_STATE.BEGINNING;
        }

        private static OINFO_STATE DoNextState(DataRow firstEvent, DataSet nextPlan, DateTime dtEvent)
        {
            OINFO_STATE stateNext = OINFO_STATE.UNDEFINE;
            OINFO_STATE currState = (OINFO_STATE)firstEvent[colOINFO_STATE];

            switch (currState)
            {
                case OINFO_STATE.BEGINNING:
                    firstEvent[colOINFO_STATE] = stateNext = OINFO_STATE.COOKING;
                    firstEvent[colOINFO_TC] = dtEvent;
                    break;

                case OINFO_STATE.COOKING:
                    firstEvent[colOINFO_STATE] = stateNext = OINFO_STATE.READY;
                    firstEvent[colOINFO_TR] = dtEvent;
                    break;

                case OINFO_STATE.READY:
                    {
                        DataRow rid = nextPlan.Tables[tblRINFO].Rows.Find(firstEvent[colOINFO_RID]);
                        DataRow cid = nextPlan.Tables[tblCINFO].Rows.Find(firstEvent[colOINFO_CID]);
                        cid[colCINFO_ADDRESS] = rid[colRINFO_ADDRESS];
                        cid[colCINFO_LAT] = rid[colRINFO_LAT];
                        cid[colCINFO_LNG] = rid[colRINFO_LNG];
                        cid[colCINFO_TOS] = GetDateTimeEvent(firstEvent);
                    }

                    firstEvent[colOINFO_STATE] = stateNext = OINFO_STATE.TRANSPORTING;
                    firstEvent[colOINFO_TT] = dtEvent;
                    break;

                case OINFO_STATE.TRANSPORTING:
                    {
                        DataRow cid = nextPlan.Tables[tblCINFO].Rows.Find(firstEvent[colOINFO_CID]);
                        cid[colCINFO_ADDRESS] = firstEvent[colOINFO_ADDRESS];
                        cid[colCINFO_LAT] = firstEvent[colOINFO_LAT];
                        cid[colCINFO_LNG] = firstEvent[colOINFO_LNG];
                        cid[colCINFO_TOS] = GetDateTimeEvent(firstEvent);
                    }

                    firstEvent[colOINFO_STATE] = stateNext = OINFO_STATE.PLACING;
                    firstEvent[colOINFO_TP] = dtEvent;
                    break;

                case OINFO_STATE.PLACING:
                    {
                        DataRow cid = nextPlan.Tables[tblCINFO].Rows.Find(firstEvent[colOINFO_CID]);
                        //cid[colCINFO_ADDRESS] = firstEvent[colORDERS_ADDRESS];
                        //cid[colCINFO_LAT] = firstEvent[colORDERS_LAT];
                        //cid[colCINFO_LNG] = firstEvent[colORDERS_LNG];
                        cid[colCINFO_TOS] = GetDateTimeEvent(firstEvent);
                    }

                    firstEvent[colOINFO_STATE] = stateNext = OINFO_STATE.ENDED;
                    firstEvent[colOINFO_TE] = dtEvent;
                    break;

                case OINFO_STATE.ENDED:

                    File.AppendAllText(Path.Combine(BaseDirectoryForDPR, "OA-O-RC.tsv"), $"{firstEvent[colOINFO_TB]}\t{firstEvent[colOINFO_TOT]}\t{firstEvent[colOINFO_TP]}\t{firstEvent[colOINFO_TD]}\t{firstEvent[colOINFO_OID]}\t{firstEvent[colOINFO_ADDRESS]}\t{firstEvent[colOINFO_LAT]}\t{firstEvent[colOINFO_LNG]}\t{firstEvent[colOINFO_RID]}\t{firstEvent[colOINFO_TC]}\t{firstEvent[colOINFO_TR]}\t{firstEvent[colOINFO_CID]}\t{firstEvent[colOINFO_TT]}\t{firstEvent[colOINFO_TE]}\n");

                    nextPlan.Tables[tblOINFO].Rows.Remove(firstEvent);
                    break;
            }

            try
            {
                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDS"] = (OINFO_STATE)firstEvent[colOINFO_STATE];
                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDO"] = firstEvent[colOINFO_OID].ToString();
            }
            catch
            {
                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDS"] = OINFO_STATE.ENDED;
                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDO"] = "";
            }

            nextPlan.AcceptChanges();
            return stateNext;
        }

        private static void InsertBgnnOrder(DataRow dataRow, DataSet nextPlan)
        {
            try
            {
                DataRow bgnnOrder = nextPlan.Tables[tblOINFO].NewRow();

                bgnnOrder[colOINFO_STATE] = OINFO_STATE.BEGINNING;
                bgnnOrder[colOINFO_OID] = dataRow["OID"];
                bgnnOrder[colOINFO_ADDRESS] = dataRow["ADDRESS"];
                GetYandexLatLng(bgnnOrder);
                bgnnOrder[colOINFO_TB] = dataRow["TB"];
                bgnnOrder[colOINFO_TC] = dataRow["TB"];
                bgnnOrder[colOINFO_TR] = (DateTime)dataRow["TB"] + (TimeSpan)dataRow["DURATION"];

                bgnnOrder[colOINFO_TT] = bgnnOrder[colOINFO_TR];
                bgnnOrder[colOINFO_TP] = bgnnOrder[colOINFO_TR];
                bgnnOrder[colOINFO_TE] = bgnnOrder[colOINFO_TR];

                bgnnOrder[colOINFO_TOT] = dataRow["TOT"];
                bgnnOrder[colOINFO_TD] = (DateTime)bgnnOrder[colOINFO_TP] - (DateTime)dataRow["TOT"];// TimeSpan.FromTicks(0);

                bgnnOrder[colOINFO_RID] = nextPlan.Tables[tblRINFO].Rows[0][colRINFO_RID];
                bgnnOrder[colOINFO_CID] = nextPlan.Tables[tblCINFO].Rows[0][colCINFO_CID];

                nextPlan.Tables[tblOINFO].Rows.Add(bgnnOrder);


                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDS"] = (OINFO_STATE)bgnnOrder[colOINFO_STATE];
                nextPlan.Tables["SUMMARY"].Rows[0]["BUILDO"] = bgnnOrder[colOINFO_OID].ToString();

                nextPlan.AcceptChanges();
            }
            catch
            {
                //appLog.WriteLine($"no geoinfo about [{dataRow["ADDRESS"].ToString()}]");
            }
        }

        private static DateTime GetDateTimeEvent(DataRow firstEvent)
        {
            switch ((OINFO_STATE)firstEvent[colOINFO_STATE])
            {
                case OINFO_STATE.BEGINNING: return (DateTime)firstEvent[colOINFO_TC];
                case OINFO_STATE.COOKING: return (DateTime)firstEvent[colOINFO_TR];
                case OINFO_STATE.READY: return (DateTime)firstEvent[colOINFO_TT];
                case OINFO_STATE.TRANSPORTING: return (DateTime)firstEvent[colOINFO_TP];
                case OINFO_STATE.PLACING: return (DateTime)firstEvent[colOINFO_TE];
                case OINFO_STATE.ENDED: return (DateTime)firstEvent[colOINFO_TE];
                default: return DateTime.MaxValue;
            }
        }

        private static DataSet ReadPlan(string v)
        {
            //throw new NotImplementedException();

            DataSet set = new DataSet("DLVR");
            set.ReadXmlSchema("testxmlschema.xml");
            set.ReadXml(v);

            DateTime buildt = (DateTime)set.Tables["SUMMARY"].Rows[0]["BUILDT"];
            TimeOfSimulation = buildt;
            return set;
        }

        private static TextWriter appLog = null;

        private static long TspRouteLength = 0;

        private static DataSet TheBestDeliveryPlan = null;
        private static DataSet TheFastDeliveryPlan = null;
        private static DataSet TheShortDeliveryPlan = null;
        private static DataSet TheWorkDeliveryPlan = null;

        private static Stopwatch WatchPlanningForCartesian = new Stopwatch();
        private static Stopwatch WatchPlanningForCartesianOrderStateBeginning = new Stopwatch();
        private static Stopwatch WatchPlanningForCartesianOrderStateCookingReady = new Stopwatch();
        private static Stopwatch WatchResortRows = new Stopwatch();

        private static Stopwatch WatchPlanningForCinfo = new Stopwatch();
        private static Stopwatch WatchWriteCurrentDeliveryPlan = new Stopwatch();

        private static Stopwatch WatchPlanningForRoutes = new Stopwatch();

        private static Stopwatch WatchGetRouteInfos = new Stopwatch();

        private static DataSet PlanningForOrders(DataSet deliveryPlan)
        {
            TheBestDeliveryPlan = null;// deliveryPlan;
            TheFastDeliveryPlan = null;
            TheShortDeliveryPlan = null;

            BuildNameIndex(deliveryPlan);

            DateTime buildt = (DateTime)deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"];
            TimeOfSimulation = buildt;

            Console.WriteLine($"================ DELIVERY PLAN AT {TimeOfSimulation}");

            //deliveryPlan.Tables["SUMMARY"].Rows.Clear();
            //DataRow row = deliveryPlan.Tables["SUMMARY"].NewRow();
            //row["BUILDT"] = buildt;
            //deliveryPlan.Tables["SUMMARY"].Rows.Add(row);

            //deliveryPlan.AcceptChanges();

            //throw new NotImplementedException();
            DateTime beginDateTime = DateTime.Now;

            int rCount = deliveryPlan.Tables[tblRINFO].Rows.Count;
            int cCount = deliveryPlan.Tables[tblCINFO].Rows.Count;
            int oCount = deliveryPlan.Tables[tblOINFO].Rows.Count;

            string dir = $"{Path.Combine(BaseDirectoryForDPR, buildt.ToString("yyyy-MM-dd(HH-mm)"))}R{rCount}C{cCount}O{oCount}";

            dir = ExtendDirByCount(dir);

            if (Directory.Exists(dir))
            {
                dir += "(" + Path.GetRandomFileName() + ")";
            }

            Directory.CreateDirectory(dir);
            BaseDirectoryForCDP = dir;

            appLog = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory(), dir, $"DPL.log"));


            appLog.WriteLine($"MAX_RESTAURANTS_FOR_PLANNING: {MAX_RESTAURANTS_FOR_PLANNING}");
            appLog.WriteLine($"MAX_COURIERS_FOR_PLANNING: {MAX_COURIERS_FOR_PLANNING}");
            appLog.WriteLine($"MAX_BEGINING_ORDERS_TO_ADD: {MAX_BEGINING_ORDERS_TO_ADD}");
            appLog.WriteLine($"MAX_ORDERS_FOR_COURIERS: {MAX_ORDERS_FOR_COURIERS}");
            appLog.WriteLine($"BEGIN DATETIME: {beginDateTime}");

            if (false)
            {
                List<DataRow> listPnts = new List<DataRow>(deliveryPlan.Tables[tblOINFO].Select());
                listPnts.AddRange(deliveryPlan.Tables[tblRINFO].Select());
                listPnts.AddRange(deliveryPlan.Tables[tblCINFO].Select());

                TspTour tour = TspDoTsp(listPnts, false, true);

                TspRouteLength = (long)Math.Round(tour.Cost());
            }

            appLog.WriteLine($"GeoRouteInfo.Count: {GeoRouteInfo.Count}");
            appLog.WriteLine();
            foreach (KeyValuePair<string, GeoRouteData> pair in GeoRouteInfo)
            {
                appLog.WriteLine($"###{pair.GetHashCode()}");

                appLog.WriteLine(pair.Key);
                appLog.WriteLine(pair.Value.GeoResponse);
            }

            appLog.Flush();

            //DataTable orders = deliveryPlan.Tables["ORDERS"];
            //DataTable Rinfo = deliveryPlan.Tables["RINFO"];
            //DataTable Cinfo = deliveryPlan.Tables["CINFO"];

            //string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dir, "deliveryPlan.xml");
            //deliveryPlan.WriteXml(xmlFileName);

            //PlanningForInitialPlan(dir + Path.DirectorySeparatorChar, deliveryPlan);

            //PlanningForTunning(dir + Path.DirectorySeparatorChar, deliveryPlan);

            // 20190503 DataRow[] beginningOrders = GetBeginningOrders(deliveryPlan, buildt);

            //beginningOrders.C

            // 20190503 TryBeginningOrders(deliveryPlan, dir, beginningOrders);

            // 20190504 DataSet dlvrPlan = deliveryPlan.Copy();

            DataRow[] bgnnOrders = deliveryPlan.Tables[tblOINFO].Rows.Cast<DataRow>().Where<DataRow>(row =>
            {
                return ((OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.BEGINNING);
            }).ToArray<DataRow>();

            DataRow[] bgnnOrdersReadyToStart = bgnnOrders;

            do
            {
                bgnnOrders = bgnnOrdersReadyToStart;
                TheBestDeliveryPlan = null;// deliveryPlan;
                TheFastDeliveryPlan = null;
                TheShortDeliveryPlan = null;

                //BldCinfoPlans = deliveryPlan.Tables[tblCINFO].Select().Select<DataRow, BuildingCinfoPlan>(row =>
                // {
                //     BuildingCinfoPlan info = new BuildingCinfoPlan();
                //     info.deliveryPlan = deliveryPlan;
                //     info.row = row;
                //     info.BuildRouteForCinfoSecondFld = BuildRouteForCinfoSecond;
                //     return info;
                // }).ToArray();

                PlanningForCartesian(dir + Path.DirectorySeparatorChar, 0, deliveryPlan, bgnnOrders);

                bgnnOrdersReadyToStart = bgnnOrders.Where<DataRow>(row =>
                {
                    return (((TimeSpan)row[colOINFO_TD]) >= TimeSpan.FromTicks(0));
                }).ToArray<DataRow>();

            }
            while (bgnnOrders.Length != bgnnOrdersReadyToStart.Length);

            //XmlSerializer serialiser1 = new XmlSerializer(typeof(Dictionary<string, string>));
            //TextWriter Filestream1 = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory(), dir, @"georouteinfo.xml"));
            //serialiser1.Serialize(Filestream1, GeoRouteInfo);
            //Filestream1.Close();


            appLog.WriteLine($"GeoRouteInfo.Count: {GeoRouteInfo.Count}");
            DateTime finishDateTime = DateTime.Now;
            appLog.WriteLine($"FINISH DATETIME: {finishDateTime}");
            appLog.WriteLine($"ELAPSED TIME: {finishDateTime - beginDateTime}");

            appLog.WriteLine($"WatchPlanningForCartesian: {WatchPlanningForCartesian.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchPlanningForCartesianOrderStateBeginning: {WatchPlanningForCartesianOrderStateBeginning.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchPlanningForCartesianOrderStateCookingReady: {WatchPlanningForCartesianOrderStateCookingReady.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchResortRows: {WatchResortRows.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchPlanningForCinfo: {WatchPlanningForCinfo.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchWriteCurrentDeliveryPlan: {WatchWriteCurrentDeliveryPlan.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchPlanningForRoutes: {WatchPlanningForRoutes.ElapsedMilliseconds}");
            appLog.WriteLine($"WatchGetRouteInfos: {WatchGetRouteInfos.ElapsedMilliseconds}");
            appLog.Close();

            WriteDeliveryPlan("TBDP", dir, TheBestDeliveryPlan);
            WriteDeliveryPlan("TFDP", dir, TheFastDeliveryPlan);
            WriteDeliveryPlan("TSDP", dir, TheShortDeliveryPlan);

            int cntTB = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.BEGINNING).Count();
            int cntTC = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.COOKING).Count();
            int cntTR = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.READY).Count();
            int cntTT = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.TRANSPORTING).Count();
            int cntTP = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.PLACING).Count();
            int cntTE = TheBestDeliveryPlan.Tables[tblOINFO].Select().Where(row => (OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.ENDED).Count();

            File.AppendAllText(Path.Combine(BaseDirectoryForDPR, "EL-O-RC.tsv"), $"{buildt}\t{cntTB}\t{cntTC}\t{cntTR}\t{cntTT}\t{cntTP}\t{cntTE}\t{dir}\t{finishDateTime - beginDateTime}\t{beginDateTime}\t{finishDateTime}\t{GeoRouteInfo.Count}\n");

            StoreGeoRouteInfo();
            return TheBestDeliveryPlan;
        }

        private static void WriteDeliveryPlan(string kind, string dir, DataSet plan)
        {
            string fnBUILDT = ((DateTime)plan.Tables["SUMMARY"].Rows[0]["BUILDT"]).ToString("yyyy-MM-dd-HH-mm");
            string fnBUILDO = plan.Tables["SUMMARY"].Rows[0]["BUILDO"].ToString();
            string fnTOTALENGTH = plan.Tables["SUMMARY"].Rows[0]["TOTALENGTH"].ToString();
            string fnTOTALDUR = ((TimeSpan)plan.Tables["SUMMARY"].Rows[0]["TOTALDURATION"]).ToString(@"hh\-mm");
            string filenameTBDP = $"{kind}({fnBUILDT}({fnBUILDO}({fnTOTALENGTH}({fnTOTALDUR}";

            plan.WriteXml(Path.Combine(Directory.GetCurrentDirectory(), dir, $"{filenameTBDP}.xml"));

            WriteHtmlVersionTheBestDeliveryPlan(plan, Path.Combine(Directory.GetCurrentDirectory(), dir, $"{filenameTBDP}.html"), filenameTBDP);
        }

        private static void TryBeginningOrders(DataSet deliveryPlan, string dir, DataRow[] beginningOrders)
        {
            foreach (DataRow ord in beginningOrders)
            {
                string dirOID = Path.Combine(dir, ord[colOINFO_OID].ToString());
                Directory.CreateDirectory(dirOID);
                PlanningForCartesian(dirOID + Path.DirectorySeparatorChar, 0, deliveryPlan, new DataRow[] { ord });
            }
        }

        private static DataRow[] GetBeginningOrders(DataSet deliveryPlan, DateTime buildt)
        {
            //MAX_BEGINING_ORDERS_TO_ADD???
            DataRow[] beginningOrders = deliveryPlan.Tables[tblOINFO].Rows.Cast<DataRow>().Where<DataRow>(row =>
            {
                DateTime newStart = (DateTime)row[colOINFO_TC] + TimeSpan.FromTicks(Math.Abs(((TimeSpan)row[colOINFO_TD]).Ticks) / 2);
                bool isStart = (((TimeSpan)row[colOINFO_TD]).Ticks < 0) && ((OINFO_STATE)row[colOINFO_STATE] == OINFO_STATE.BEGINNING) && (newStart < buildt);

                TimeSpan timeForReady = (DateTime)row[colOINFO_TR] - (DateTime)row[colOINFO_TC];
                if (isStart)
                {
                    row[colOINFO_TC] = buildt;
                    row[colOINFO_TR] = buildt + timeForReady;
                    //row[colORDERS_RID] = deliveryPlan.Tables[tblRINFO].Rows[0][colRINFO_RID];
                    //row[colORDERS_CID] = deliveryPlan.Tables[tblCINFO].Rows[0][colCINFO_CID];
                }
                else
                {
                    //row[colOINFO_TC] = newStart;
                    //row[colOINFO_TR] = newStart + timeForReady;
                }
                return isStart;
            }).ToArray<DataRow>();
            deliveryPlan.AcceptChanges();
            return beginningOrders;
        }

        private static string ExtendDirByCount(string dir)
        {
            int cnt = Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), $"{dir}(???)").Count();

            return $"{dir}({cnt.ToString("D3")})";
        }

        private static void WriteHtmlVersionTheBestDeliveryPlan(DataSet theBestDeliveryPlan, string v, string pageTitle)
        {
            StringBuilder scriptPlan = HtmlPlanBuilder(theBestDeliveryPlan, pageTitle);

            File.WriteAllText(v, scriptPlan.ToString());

            if (false)
            {
                Process proc = System.Diagnostics.Process.Start(v);
                Thread.Sleep(TimeSpan.FromSeconds(5));
            }
        }

        private static StringBuilder HtmlPlanBuilder(DataSet theBestDeliveryPlan, string pageTitle, string metaExt = "")
        {
            StringBuilder scriptPlan = new StringBuilder();

            scriptPlan.AppendLine("<!DOCTYPE html>");
            //scriptPlan.AppendLine($"<html><head><title>TBDP {theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"].ToString()}</title>");
            scriptPlan.AppendLine($"<html><head><title>{pageTitle}</title>");
            scriptPlan.AppendLine("<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\"/>");
            scriptPlan.AppendLine(metaExt);
            scriptPlan.AppendLine("<script src=\"https://api-maps.yandex.ru/2.1/?lang=ru_RU&amp;apikey=a1d0badd-df1d-4814-8d43-eab723c50133\" type=\"text/javascript\"></script>");
            //scriptPlan.AppendLine("<script src=\"animated_line.js\" type =\"text/javascript\"></script>");
            scriptPlan.AppendLine("<script type=\"text/javascript\">");

            string planCenterLat = "54.206134";
            string planCenterLng = "37.669204";

            string ordrCenterLat = planCenterLat;
            string ordrCenterLng = planCenterLng;
            DataRow buildOrdr = theBestDeliveryPlan.Tables[tblOINFO].Rows.Find(theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDO"].ToString());
            if (buildOrdr != null)
            {
                ordrCenterLat = buildOrdr[colOINFO_LAT].ToString();
                ordrCenterLng = buildOrdr[colOINFO_LNG].ToString();
            }

            scriptPlan.AppendLine("ymaps.ready(init);");
            scriptPlan.AppendLine($"function init() {{ var myMap = new ymaps.Map(\"map\", {{ center: [{planCenterLat}, {planCenterLng}], zoom: 13, controls: ['smallMapDefaultSet'] }}, {{ searchControlProvider: 'yandex#search' }});");
            scriptPlan.AppendLine($"function setCenterPlan() {{ myMap.setCenter([{planCenterLat}, {planCenterLng}], 13, {{ checkZoomRange: true }}); }}");
            scriptPlan.AppendLine($"function setCenterOrder() {{ myMap.setCenter([{ordrCenterLat}, {ordrCenterLng}], 13, {{ checkZoomRange: true }}); }}");
            //scriptPlan.AppendLine($"myMap.controls.add( new ymaps.control.Button(\"{theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"].ToString()} {Enum.GetName(typeof(OINFO_STATE), (OINFO_STATE)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDS"])} {theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["MED"]} {theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"]}\"), {{maxWidth:2000, float: 'right'}});");
            scriptPlan.AppendLine($"var btnBuildOrdr = new ymaps.control.Button(\"{theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"].ToString()} {Enum.GetName(typeof(OINFO_STATE), (OINFO_STATE)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDS"])} {((TimeSpan)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["MED"]).ToString(@"hh\:mm\:ss")} {((TimeSpan)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"]).ToString(@"hh\:mm\:ss")} {theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDO"].ToString()}\");");
            scriptPlan.AppendLine($"btnBuildOrdr.events.add(['click'], function (sender) {{ if(btnBuildOrdr.isSelected()) {{ setCenterPlan(); }} else {{ setCenterOrder(); }} }});");
            scriptPlan.AppendLine($"myMap.controls.add( btnBuildOrdr, {{maxWidth:2000, float: 'right'}});");

            scriptPlan.AppendLine(ConsoleAppDlvrTomato.Properties.Resources.listbox4map);

            int btnPosition = 200;

            foreach (DataRow oInfo in theBestDeliveryPlan.Tables[tblOINFO].Select().Where(r => { return ((TimeSpan)r[colOINFO_TD]) < TimeSpan.Zero; }))
            {
                btnPosition = ScriptPlanAddOrder(btnPosition, scriptPlan, oInfo);
            }

            foreach (DataRow rInfo in theBestDeliveryPlan.Tables[tblRINFO].Rows)
            {
                int hashCode = rInfo.GetHashCode();
                scriptPlan.AppendLine("{");
                scriptPlan.AppendLine($"var btnLayout = ymaps.templateLayoutFactory.createClass('<div style=\"font-family: Arial,Helvetica,sans-serif; font-size: 12px; color: black; background-color: #F08080; border: {{% if state.selected %}}3{{% else %}}1{{% endif %}}px solid; padding: 3px;\"><span>{{{{data.content}}}}</span></div>');");
                scriptPlan.AppendLine($"myMap.geoObjects.add(new ymaps.Placemark([{rInfo[colRINFO_LAT].ToString()}, {rInfo[colRINFO_LNG].ToString()}], {{ iconContent: '{rInfo[colRINFO_RID]}', hintContent: '{rInfo[colRINFO_LAT]} {rInfo[colRINFO_LNG]} {rInfo[colRINFO_ADDRESS]}'}}, {{preset: 'islands#redStretchyIcon'}} ));");
                scriptPlan.AppendLine($"var btn{hashCode} = new ymaps.control.Button(\"{rInfo[colRINFO_RID]}\");");
                scriptPlan.AppendLine($"btn{hashCode}.options.set('layout', btnLayout);");
                scriptPlan.AppendLine($"myMap.controls.add(btn{hashCode}, {{maxWidth: 2000, float: 'none', position: {{ left: 10, right: 'auto', top: {btnPosition}, bottom: 'auto' }} }});");
                btnPosition += 35;
                scriptPlan.AppendLine("}");

                foreach (DataRow oInfo in theBestDeliveryPlan.Tables[tblOINFO].Select().Where(r => { return (((TimeSpan)r[colOINFO_TD]) >= TimeSpan.Zero) && (r[colOINFO_RID].ToString() == rInfo[colRINFO_RID].ToString()); }))
                {
                    btnPosition = ScriptPlanAddOrder(btnPosition, scriptPlan, oInfo);
                }
            }

            foreach (DataRow cInfo in theBestDeliveryPlan.Tables[tblCINFO].Rows)
            {
                int hashCode = cInfo.GetHashCode();
                scriptPlan.AppendLine("{");
                scriptPlan.AppendLine($"var btnLayout = ymaps.templateLayoutFactory.createClass('<div style=\"font-family: Arial,Helvetica,sans-serif; font-size: 12px; color: black; background-color: #87CEFA; border: {{% if state.selected %}}3{{% else %}}1{{% endif %}}px solid; padding: 3px;\"><span>{{{{data.content}}}}</span></div>');");
                scriptPlan.AppendLine($"var coll{hashCode} = new ymaps.GeoObjectCollection(null, {{ }});");
                scriptPlan.AppendLine($"var btn{hashCode} = new ymaps.control.Button(\"{cInfo[colCINFO_CID]} {cInfo[colCINFO_ROUTELENGTH]} {cInfo[colCINFO_ROUTE].ToString()}\");");
                scriptPlan.AppendLine($"btn{hashCode}.options.set('layout', btnLayout);");
                scriptPlan.AppendLine($"btn{hashCode}.events.add(['click'], function (sender) {{ if(!btn{hashCode}.isSelected()) {{ addColl{hashCode}(); btn{hashCode}.options.set('maxWidth', 2000); myMap.setBounds(coll{hashCode}.getBounds()); }} else {{ delColl{hashCode}(); btn{hashCode}.options.set('maxWidth', 200); }} }});");
                scriptPlan.AppendLine($"myMap.controls.add(btn{hashCode}, {{maxWidth: 200, float: 'none', position: {{ left: 'auto', right: 10, top: {50 + 35 * cInfo.Table.Rows.IndexOf(cInfo)}, bottom: 'auto' }} }});");

                //scriptPlan.AppendLine($"myMap.geoObjects.add(new ymaps.Placemark([{cInfo[colCINFO_LAT].ToString()}, {cInfo[colCINFO_LNG].ToString()}], {{ iconCaption: '{cInfo[colCINFO_CID]}', hintContent: '{cInfo[colCINFO_LAT]} {cInfo[colCINFO_LNG]} {cInfo[colCINFO_ADDRESS]}'}}, {{preset: 'islands#blueCircleDotIconWithCaption'}} ));");
                scriptPlan.AppendLine($"coll{hashCode}.add(new ymaps.Placemark([{cInfo[colCINFO_LAT].ToString()}, {cInfo[colCINFO_LNG].ToString()}], {{ iconCaption: '{cInfo[colCINFO_CID]}', hintContent: '{cInfo[colCINFO_LAT]} {cInfo[colCINFO_LNG]} {cInfo[colCINFO_ADDRESS]}'}}, {{preset: 'islands#blueCircleDotIconWithCaption'}} ));");

                string[] pnts = cInfo[colCINFO_ROUTE].ToString().Split('-');

                string latP = cInfo[colCINFO_LAT].ToString();
                string lngP = cInfo[colCINFO_LNG].ToString();
                string coords = $"[{latP},{lngP}]";

                string latD = "";
                string lngD = "";

                foreach (string pnt in pnts)
                {
                    if (String.IsNullOrEmpty(pnt)) continue;

                    string[] partsOfP = pnt.Split("[]".ToCharArray());
                    if (partsOfP.Length > 1)
                    {// R
                        latD = theBestDeliveryPlan.Tables[tblRINFO].Rows.Find(partsOfP[0])[colRINFO_LAT].ToString();
                        lngD = theBestDeliveryPlan.Tables[tblRINFO].Rows.Find(partsOfP[0])[colRINFO_LNG].ToString();
                    }
                    else
                    {// O
                        latD = theBestDeliveryPlan.Tables[tblOINFO].Rows.Find(pnt)[colOINFO_LAT].ToString();
                        lngD = theBestDeliveryPlan.Tables[tblOINFO].Rows.Find(pnt)[colOINFO_LNG].ToString();

                    }

                    coords += $",[{latD},{lngD}]";
                }

                coords += $",[{theBestDeliveryPlan.Tables[tblRINFO].Rows.Find(cInfo[colCINFO_RID4S])[colRINFO_LAT].ToString()},{theBestDeliveryPlan.Tables[tblRINFO].Rows.Find(cInfo[colCINFO_RID4S])[colRINFO_LNG].ToString()}]";

                //scriptPlan.AppendLine($"myMap.geoObjects.add(new ymaps.Polyline([{coords}], {{ balloonContent:'{cInfo[colCINFO_CID].ToString()} : {cInfo[colCINFO_ROUTE].ToString()}'}}, {{strokeColor: '#000000', strokeWidth: 6, strokeOpacity: 0.3 }} ));");
                scriptPlan.AppendLine($"coll{hashCode}.add(new ymaps.Polyline([{coords}], {{ balloonContent:'{cInfo[colCINFO_CID].ToString()} : {cInfo[colCINFO_ROUTE].ToString()}'}}, {{strokeColor: '#000000', strokeWidth: 3, strokeOpacity: 0.3 }} ));");
                scriptPlan.AppendLine($"coll{hashCode}.add(new ymaps.multiRouter.MultiRoute({{ referencePoints:[{coords}], params: {{ results: 1 }} }}, {{  wayPointVisible:false, viaPointVisible:false,  pinVisible:false, boundsAutoApply: false }}));");
                scriptPlan.AppendLine($"function addColl{hashCode}() {{ myMap.geoObjects.add(coll{hashCode}); }}");
                scriptPlan.AppendLine($"function delColl{hashCode}() {{ myMap.geoObjects.remove(coll{hashCode}); }}");
                //scriptPlan.AppendLine($"addColl{hashCode}();");
                //scriptPlan.AppendLine($"{{ var animLine = new ymaps.AnimatedLine([{coords}], {{ balloonContent:'{cInfo[colCINFO_CID].ToString()} : {cInfo[colCINFO_ROUTE].ToString()}'}}, {{strokeColor: '#000000', strokeWidth: 6, strokeOpacity: 0.3, animationTime: 4000 }} );");
                //scriptPlan.AppendLine($"myMap.geoObjects.add(animLine);");
                //scriptPlan.AppendLine($"function playAnimation() {{ animLine.animate().then(function(){{return ymaps.vow.delay(null, 2000);}}).then(function(){{playAnimation();}}); }}");
                //scriptPlan.AppendLine($"playAnimation(); }}");
                scriptPlan.AppendLine("}");
            }


            scriptPlan.AppendLine("}");
            scriptPlan.AppendLine("</script>");

            scriptPlan.AppendLine("<style> html, body, #map { width: 100%; height: 100%; padding: 0; margin: 0; } </style></head>");
            scriptPlan.AppendLine("<body><div id = \"map\"></div></body></html>");
            return scriptPlan;
        }

        private static int ScriptPlanAddOrder(int btnPos, StringBuilder scriptPlan, DataRow oInfo)
        {
            int hashCode = oInfo.GetHashCode();
            scriptPlan.AppendLine("{");
            scriptPlan.AppendLine($"var btnLayout = ymaps.templateLayoutFactory.createClass('<div style=\"font-family: Arial,Helvetica,sans-serif; font-size: 12px; color: black; background-color: #90EE90; border: {{% if state.selected %}}3{{% else %}}1{{% endif %}}px solid; padding: 3px;\"><span>{{{{data.content}}}}</span></div>');");

            TimeSpan td = ((TimeSpan)oInfo[colOINFO_TD]);
            string tdfrmt = td < TimeSpan.Zero ? td.ToString(@"\(hh\:mm\)") : td.ToString(@"hh\:mm");

            if (((OINFO_STATE)oInfo[colOINFO_STATE]) == OINFO_STATE.BEGINNING)
            {
                scriptPlan.AppendLine($"myMap.geoObjects.add(new ymaps.Placemark([{oInfo[colOINFO_LAT].ToString()}, {oInfo[colOINFO_LNG].ToString()}], {{ iconCaption: '{oInfo[colOINFO_OID]} {DispOrderState((OINFO_STATE)oInfo[colOINFO_STATE])} {oInfo[colOINFO_RID]} {oInfo[colOINFO_CID]} {tdfrmt}', hintContent: '{oInfo[colOINFO_LAT]} {oInfo[colOINFO_LNG]} {oInfo[colOINFO_ADDRESS]}'}}, {{preset: 'islands#greenDotIconWithCaption'}} ));");
            }
            else
            {
                scriptPlan.AppendLine($"myMap.geoObjects.add(new ymaps.Placemark([{oInfo[colOINFO_LAT].ToString()}, {oInfo[colOINFO_LNG].ToString()}], {{ iconContent: '{oInfo[colOINFO_OID]} {DispOrderState((OINFO_STATE)oInfo[colOINFO_STATE])} {oInfo[colOINFO_RID]} {oInfo[colOINFO_CID]} {tdfrmt}', hintContent: '{oInfo[colOINFO_LAT]} {oInfo[colOINFO_LNG]} {oInfo[colOINFO_ADDRESS]}'}}, {{preset: 'islands#greenStretchyIcon'}} ));");
            }

            string ordrTimes = ((DateTime)oInfo[colOINFO_TB]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.BEGINNING) ? " HH:mm B" : " HH:mm ."))
                + ((DateTime)oInfo[colOINFO_TC]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.COOKING) ? " HH:mm C" : " HH:mm ."))
                + ((DateTime)oInfo[colOINFO_TR]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.READY) ? " HH:mm R" : " HH:mm ."))
                + ((DateTime)oInfo[colOINFO_TT]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.TRANSPORTING) ? " HH:mm T" : " HH:mm ."))
                + ((DateTime)oInfo[colOINFO_TP]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.PLACING) ? " HH:mm P" : " HH:mm ."))
                + ((DateTime)oInfo[colOINFO_TE]).ToString(((((OINFO_STATE)oInfo[colCINFO_STATE]) == OINFO_STATE.ENDED) ? " HH:mm E" : " HH:mm ."));
            scriptPlan.AppendLine($"var btn{hashCode} = new ymaps.control.Button(\"{oInfo[colOINFO_OID]} {ordrTimes}\");");
            scriptPlan.AppendLine($"btn{hashCode}.options.set('layout', btnLayout);");
            //scriptPlan.AppendLine($"myMap.controls.add(btn{hashCode}, {{maxWidth: 2000, float: 'none', position: {{ left: 10, right: 'auto', top: {200 + 35 * oInfo.Table.Rows.IndexOf(oInfo)}, bottom: 'auto' }} }});");
            scriptPlan.AppendLine($"myMap.controls.add(btn{hashCode}, {{maxWidth: 2000, float: 'none', position: {{ left: 15, right: 'auto', top: {btnPos}, bottom: 'auto' }} }});");
            btnPos += 35;

            scriptPlan.AppendLine("}");

            return btnPos;
        }

        private static object DispOrderState(OINFO_STATE oRDER_STATE)
        {
            switch (oRDER_STATE)
            {
                case OINFO_STATE.BEGINNING: return "B";
                case OINFO_STATE.COOKING: return "C";
                case OINFO_STATE.READY: return "R";
                case OINFO_STATE.TRANSPORTING: return "T";
                case OINFO_STATE.PLACING: return "P";
                case OINFO_STATE.ENDED: return "E";
            }
            return "U";
        }

        private static int tblOINFO;
        private static int colOINFO_STATE;
        private static int colOINFO_OID;
        private static int colOINFO_ADDRESS;
        private static int colOINFO_LAT;
        private static int colOINFO_LNG;
        private static int colOINFO_TB;
        private static int colOINFO_TC;
        private static int colOINFO_TR;
        private static int colOINFO_TT;
        private static int colOINFO_TP;
        private static int colOINFO_TE;
        private static int colOINFO_TOT;
        private static int colOINFO_RID;
        private static int colOINFO_CID;
        private static int colOINFO_TD;

        private static int tblRINFO;
        private static int colRINFO_STATE;
        private static int colRINFO_RID;
        private static int colRINFO_ADDRESS;
        private static int colRINFO_LAT;
        private static int colRINFO_LNG;

        private static int tblCINFO;
        private static int colCINFO_STATE;
        private static int colCINFO_CID;
        private static int colCINFO_ADDRESS;
        private static int colCINFO_LAT;
        private static int colCINFO_LNG;
        private static int colCINFO_TOS;
        private static int colCINFO_ROUTE;
        private static int colCINFO_ROUTELENGTH;
        private static int colCINFO_RID4S;

        private static void BuildNameIndex(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();

            tblOINFO = deliveryPlan.Tables.IndexOf("OINFO");
            colOINFO_STATE = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("OSTATE");
            colOINFO_OID = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("OID");
            colOINFO_ADDRESS = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("ADDRESS");
            colOINFO_LAT = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("LAT");
            colOINFO_LNG = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("LNG");
            colOINFO_TB = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TB");
            colOINFO_TC = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TC");
            colOINFO_TR = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TR");
            colOINFO_TT = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TT");
            colOINFO_TP = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TP");
            colOINFO_TE = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TE");
            colOINFO_TOT = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TOT");
            colOINFO_RID = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("RID");
            colOINFO_CID = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("CID");
            colOINFO_TD = deliveryPlan.Tables[tblOINFO].Columns.IndexOf("TD");

            tblRINFO = deliveryPlan.Tables.IndexOf("RINFO");
            colRINFO_STATE = deliveryPlan.Tables[tblRINFO].Columns.IndexOf("RSTATE");
            colRINFO_RID = deliveryPlan.Tables[tblRINFO].Columns.IndexOf("RID");
            colRINFO_ADDRESS = deliveryPlan.Tables[tblRINFO].Columns.IndexOf("ADDRESS");
            colRINFO_LAT = deliveryPlan.Tables[tblRINFO].Columns.IndexOf("LAT");
            colRINFO_LNG = deliveryPlan.Tables[tblRINFO].Columns.IndexOf("LNG");

            tblCINFO = deliveryPlan.Tables.IndexOf("CINFO");
            colCINFO_STATE = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("CSTATE");
            colCINFO_CID = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("CID");
            colCINFO_ADDRESS = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("ADDRESS");
            colCINFO_LAT = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("LAT");
            colCINFO_LNG = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("LNG");
            colCINFO_TOS = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("TOS");
            colCINFO_ROUTE = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("ROUTE");
            colCINFO_ROUTELENGTH = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("ROUTELENGTH");
            colCINFO_RID4S = deliveryPlan.Tables[tblCINFO].Columns.IndexOf("RID4S");
        }

        private static void PlanningForTunning(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders)
        {
            //throw new NotImplementedException();

            TimeSpan spanMAX = TimeSpan.MinValue;
            DataRow rowMAX = null;
            deliveryPlan.Tables[tblOINFO].Select().All((r) =>
            {

                if ((TimeSpan)r[colOINFO_TD] > spanMAX)
                {
                    spanMAX = (TimeSpan)r[colOINFO_TD];
                    rowMAX = r;
                }

                return true;
            });

            switch ((OINFO_STATE)rowMAX[colOINFO_STATE])
            {
                case OINFO_STATE.BEGINNING:
                    PlanningForTunningOrderStateBeginning(dir, deliveryPlan, bgnnOrders, rowMAX);
                    break;

                case OINFO_STATE.COOKING:
                case OINFO_STATE.READY:
                    PlanningForTunningOrderStateCookingReady(dir, deliveryPlan, bgnnOrders, rowMAX);
                    break;

                case OINFO_STATE.TRANSPORTING:
                case OINFO_STATE.PLACING:
                    PlanningForCinfo(dir + "(" + rowMAX[colOINFO_RID].ToString() + ")(" + rowMAX[colOINFO_CID].ToString() + ")", 0, deliveryPlan, bgnnOrders, 0);
                    break;

                default: break;
            }

        }

        private static void PlanningForTunningOrderStateCookingReady(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow rowMAX)
        {
            foreach (DataRow c in deliveryPlan.Tables[tblCINFO].Rows)
            {
                rowMAX[colOINFO_CID] = c[colCINFO_CID];
                deliveryPlan.Tables[tblOINFO].AcceptChanges();

                PlanningForCinfo(dir + "(" + rowMAX[colOINFO_RID].ToString() + ")(" + c[colCINFO_CID] + ")", 0, deliveryPlan, bgnnOrders, 0);
            }
        }

        private static void PlanningForTunningOrderStateBeginning(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow rowMAX)
        {
            foreach (DataRow r in deliveryPlan.Tables[tblRINFO].Rows)
            {
                foreach (DataRow c in deliveryPlan.Tables[tblCINFO].Rows)
                {
                    rowMAX[colOINFO_RID] = r[colRINFO_RID];
                    rowMAX[colOINFO_CID] = c[colCINFO_CID];
                    deliveryPlan.Tables[tblOINFO].AcceptChanges();

                    PlanningForCinfo(dir + "(" + r[colRINFO_RID] + ")(" + c[colCINFO_CID] + ")", 0, deliveryPlan, bgnnOrders, 0);
                }
            }
        }

        private static void PlanningForInitialPlan(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders)
        {
            //DataSet initPlan = CreateInitialPlanByNearest(deliveryPlan);// CreateInitialPlan(deliveryPlan);
            //DataSet initPlan = CreateInitialPlan(deliveryPlan);// CreateInitialPlan(deliveryPlan);
            DataSet initPlan = CreateInitialPlanWithTsp(deliveryPlan);
            PlanningForCinfo(dir, 0, initPlan, bgnnOrders, 0);

            //initPlan.WriteXml(Path.Combine(Directory.GetCurrentDirectory(), dir, "initplan.xml"));
        }

        private static DataSet CreateInitialPlanWithTsp(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();
            DataSet initPlan = deliveryPlan.Copy();

            List<DataRow> listPnts = new List<DataRow>(initPlan.Tables[tblOINFO].Select());
            listPnts.AddRange(initPlan.Tables[tblRINFO].Select());

            TspTour tour = TspDoTsp(listPnts, false, true);

            TspStop mark = tour.Anchor;

            while (mark.City.CityInfo.Table.Columns.Contains("OSTATE")) mark = mark.Next;

            TspStop markStop = mark;

            string markRID = mark.City.CityInfo["RID"].ToString();

            do
            {
                mark = mark.Next;

                if (!mark.City.CityInfo.Table.Columns.Contains("OSTATE"))
                {
                    markRID = mark.City.CityInfo["RID"].ToString();
                }
                else
                //if (mark.City.CityInfo.Table.Columns.Contains("STATE"))
                {
                    OINFO_STATE oidSTATE = (OINFO_STATE)mark.City.CityInfo["OSTATE"];
                    if (oidSTATE == OINFO_STATE.BEGINNING)
                    {
                        mark.City.CityInfo["RID"] = markRID;
                    }
                }
            }
            while (mark != markStop);

            initPlan.AcceptChanges();

            return initPlan;
        }

        private static DataSet CreateInitialPlanByNearest(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();

            DataSet initPlan = deliveryPlan.Copy();

            int minRinfo = int.MaxValue;
            int minCinfo = int.MaxValue;

            foreach (DataRow order in initPlan.Tables[tblOINFO].Rows)
            {
                if (((OINFO_STATE)order["OSTATE"]) != OINFO_STATE.BEGINNING) continue;

                foreach (DataRow rinfo in initPlan.Tables[tblRINFO].Rows)
                {
                    //string r = GetGeoPathTotal2Gis(rinfo["LAT"].ToString(), rinfo["LNG"].ToString(), order["LAT"].ToString(), order["LNG"].ToString());
                    //string r = GetGeoPathTotalYandex(DateTime.MinValue, rinfo["LAT"].ToString(), rinfo["LNG"].ToString(), order["LAT"].ToString(), order["LNG"].ToString());
                    GeoRouteData r = GetGeoPathTotalOSRM(DateTime.MinValue, rinfo["LAT"].ToString(), rinfo["LNG"].ToString(), order["LAT"].ToString(), order["LNG"].ToString());
                    //JsonValue json = JsonValue.Parse(r);

                    //double distBet = GetGeoPathDistance(rinfo["LAT"].ToString(), rinfo["LNG"].ToString(), order["LAT"].ToString(), order["LNG"].ToString());

                    //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                    //int lll = int.Parse(json["result"][0]["length"].ToString());
                    //int ddd = (int)distBet / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                    //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                    //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                    //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

                    if (r.Duration < minRinfo)
                    {
                        order["RID"] = rinfo["RID"];
                        minRinfo = r.Duration;

                        minCinfo = int.MaxValue;

                        foreach (DataRow cinfo in initPlan.Tables[tblCINFO].Rows)
                        {
                            //string rCinfo = GetGeoPathTotal2Gis(cinfo["LAT"].ToString(), cinfo["LNG"].ToString(), rinfo["LAT"].ToString(), rinfo["LNG"].ToString());
                            //string rCinfo = GetGeoPathTotalYandex(DateTime.MinValue, cinfo["LAT"].ToString(), cinfo["LNG"].ToString(), rinfo["LAT"].ToString(), rinfo["LNG"].ToString());
                            GeoRouteData rCinfo = GetGeoPathTotalOSRM(DateTime.MinValue, cinfo["LAT"].ToString(), cinfo["LNG"].ToString(), rinfo["LAT"].ToString(), rinfo["LNG"].ToString());
                            //JsonValue jsonCinfo = JsonValue.Parse(rCinfo);

                            //double distBetCinfo = GetGeoPathDistance(cinfo["LAT"].ToString(), cinfo["LNG"].ToString(), rinfo["LAT"].ToString(), rinfo["LNG"].ToString());

                            //int dddCinfo = int.Parse(jsonCinfo["result"][0]["duration"].ToString());
                            //int lllCinfo = int.Parse(jsonCinfo["result"][0]["length"].ToString());
                            //int dddCinfo = (int)distBetCinfo / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                            //int lllCinfo = (int)distBetCinfo;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                            //int dddCinfo = (int)double.Parse(json["routes"][0]["duration"].ToString());
                            //int lllCinfo = (int)double.Parse(json["routes"][0]["distance"].ToString());

                            if (rCinfo.Duration < minCinfo)
                            {
                                order["CID"] = cinfo["CID"];
                                minCinfo = rCinfo.Duration;
                            }
                        }
                    }
                }
            }

            initPlan.AcceptChanges();

            return initPlan;
        }

        private static DataSet CreateInitialPlan(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();

            DataSet initPlan = deliveryPlan.Copy();

            //var json = JsonValue.Parse("{ \"start\": { \"lat\": 54.998493600520156, \"lon\": 82.65769958496095 }, \"finish\": { \"lat\": 55.05516914322075, \"lon\": 83.15208435058594 }, \"checkpoints\": [ { \"lat\": 54.99770587584445, \"lon\": 82.79502868652345 }, { \"lat\": 54.99928130973027, \"lon\": 82.92137145996095, \"delay\": 300 }, { \"lat\": 55.04533538802211, \"lon\": 82.98179626464844 }, { \"lat\": 55.072470687600536, \"lon\": 83.04634094238281 } ] }");
            StringBuilder Q = new StringBuilder();
            Q.Append("{ ");
            //Q.Append("\"detailed\": true, ");
            Q.Append("\"start\": ");
            Q.Append("{ ");
            Q.Append($"\"lat\": {initPlan.Tables["RINFO"].Rows[0]["LAT"].ToString()} ");
            Q.Append(", ");
            Q.Append($"\"lon\": {initPlan.Tables["RINFO"].Rows[0]["LNG"].ToString()} ");
            Q.Append("} ");
            Q.Append(", ");
            Q.Append("\"finish\": ");
            Q.Append("{ ");
            Q.Append($"\"lat\": {initPlan.Tables["RINFO"].Rows[0]["LAT"].ToString()} ");
            Q.Append(", ");
            Q.Append($"\"lon\": {initPlan.Tables["RINFO"].Rows[0]["LNG"].ToString()} ");
            Q.Append("} ");
            Q.Append(", ");
            Q.Append("\"checkpoints\": ");
            Q.Append("[ ");
            Q.Append(String.Join(", ", initPlan.Tables["OINFO"].Select().Reverse().Select<DataRow, string>((r) => { return "{ \"lat\": " + r["LAT"].ToString() + ", \"lon\": " + r["LNG"].ToString() + "}"; })));
            Q.Append(", ");
            Q.Append(String.Join(", ", initPlan.Tables["RINFO"].Select().Reverse().Skip(1).Select<DataRow, string>((r) => { return "{ \"lat\": " + r["LAT"].ToString() + ", \"lon\": " + r["LNG"].ToString() + "}"; })));
            //Q.Append(", ");
            //Q.Append(String.Join(", ", initPlan.Tables["CINFO"].Select().Select<DataRow, string>((r) => { return "{ \"lat\": " + r["LAT"].ToString() + ", \"lon\": " + r["LNG"].ToString() + "}"; })));
            //Q.Append("{ ");
            //Q.Append($"\"lat\": {initPlan.Tables["ORDERS"].Rows[0]["LAT"].ToString()} ");
            //Q.Append(", ");
            //Q.Append($"\"lon\": {initPlan.Tables["ORDERS"].Rows[0]["LNG"].ToString()} ");
            //Q.Append("} ");
            Q.Append("] ");
            Q.Append("}");

            string tspRoute = GetTspRoute2Gis(Q.ToString());

            JsonValue jsonTspRoute = JsonValue.Parse(tspRoute);

            return initPlan;
        }

        private static void PlanningForCartesian(string dir, int vOrder, DataSet deliveryPlan, DataRow[] bgnnOrders)
        {
            WatchPlanningForCartesian.Start();
            //throw new NotImplementedException();

            ClearRouteInformation(deliveryPlan);

            DataRowCollection OrdersRows = deliveryPlan.Tables[tblOINFO].Rows;

            if (vOrder >= OrdersRows.Count)
            {
                //string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dir + ".xml");//Path.GetRandomFileName());
                ////string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dir, "(" + deliveryPlan.Tables["ORDERS"].Rows. + ")(" + deliveryPlan.Tables["ORDERS"].Rows[v]["CID"] + ").xml");
                ////string xmlFileName = dir + ".xml";
                //deliveryPlan.WriteXml(xmlFileName);


                //DataSet copyDeliveryPlan = deliveryPlan.Copy();
                //Task task = Task.Run(() =>
                // {
                WatchPlanningForCartesian.Stop();
                //PlanningForCinfo(dir, 0, deliveryPlan, bgnnOrders, 0);
                //Console.WriteLine(dir);

                PlanningRoutesParallel(dir, deliveryPlan, bgnnOrders);
                // });
                //task.Start();
                //taskList.Add(task);
            }
            else
            {
                switch ((OINFO_STATE)OrdersRows[vOrder][colOINFO_STATE])
                {
                    case OINFO_STATE.BEGINNING:
                        WatchPlanningForCartesian.Stop();
                        PlanningForCartesianOrderStateBeginning(dir, vOrder, deliveryPlan, OrdersRows, bgnnOrders);
                        WatchPlanningForCartesian.Start();
                        break;

                    case OINFO_STATE.COOKING:
                    case OINFO_STATE.READY:
                        WatchPlanningForCartesian.Stop();
                        //foreach (DataRow c in ResortRows(deliveryPlan.Tables[tblRINFO].Select($"RID = '{OrdersRows[vOrder][colORDERS_RID].ToString()}'")[0], deliveryPlan.Tables[tblCINFO].Rows).Take(3))
                        PlanningForCartesianOrderStateCookingReady(dir, vOrder, deliveryPlan, OrdersRows, bgnnOrders);
                        WatchPlanningForCartesian.Start();
                        break;

                    case OINFO_STATE.TRANSPORTING:
                    case OINFO_STATE.PLACING:
                    case OINFO_STATE.ENDED:
                        WatchPlanningForCartesian.Stop();
                        PlanningForCartesian(dir + "(" + OrdersRows[vOrder][colOINFO_RID].ToString() + "-" + OrdersRows[vOrder][colOINFO_CID].ToString() + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                        //PlanningForCartesian(dir + "(" + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                        WatchPlanningForCartesian.Start();
                        break;

                    default: break;
                }
            }
        }

        private delegate Func<bool> BuildRouteForCinfoSecondDlgt(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow cInfo);

        private class BuildingCinfoPlan : EventWaitHandle
        {
            public BuildingCinfoPlan() : base(false, EventResetMode.ManualReset)
            {
                Task.Run(() =>
                {
                    while (true)
                    {
                        WaitOne();
                        Reset();
                        BuildRouteForCinfoSecondFld(dir, deliveryPlan, bgnnOrders, row);
                        Set();
                        Reset();
                    }
                });
            }

            public string dir { get; set; }
            public DataSet deliveryPlan { get; set; }
            public DataRow[] bgnnOrders { get; set; }
            public DataRow row { get; set; }
            public BuildRouteForCinfoSecondDlgt BuildRouteForCinfoSecondFld { get; set; }

            //ManualResetEvent mre = new ManualResetEvent(false);
        }

        private static BuildingCinfoPlan[] BldCinfoPlans = null;

        private static void PlanningRoutesParallel(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders)
        {
            //BldCinfoPlans.All(info =>
            //{
            //    info.dir = dir;
            //    info.bgnnOrders = bgnnOrders;
            //    info.Set();
            //    return true;
            //});

            //ManualResetEvent.WaitAll(BldCinfoPlans);

            System.Threading.Tasks.Parallel.ForEach(deliveryPlan.Tables[tblCINFO].Select(), row =>
            {
                //BuildRouteForCinfoSecond(dir, deliveryPlan, bgnnOrders, row);
                BuildRouteForCinfoInternal(deliveryPlan, bgnnOrders, row);
            });
            //Task[] cinfoTasks = 
            //deliveryPlan.Tables[tblCINFO].Select().All(row =>//.Select<DataRow, Task>(row =>
            //{

            //return Task.Run(BuildRouteForCinfoSecond(dir, deliveryPlan, bgnnOrders, row));
            //    return true;
            //});//.ToArray();

            //Task.WaitAll(cinfoTasks);

            //if (!cinfoTasks.All((t) => { return ((Task<bool>)t).Result; }))
            //{
            //    Console.WriteLine("The building of routes has a problem.");
            //    return;
            //}

            deliveryPlan.AcceptChanges();

            int totalRouteLength = CalcTotalRouteLength(deliveryPlan);

            WriteCurrentDeliveryPlan(dir, deliveryPlan, totalRouteLength);
        }

        private static int CalcTotalRouteLength(DataSet deliveryPlan)
        {
            int r = 0;

            foreach (DataRow cInfo in deliveryPlan.Tables[tblCINFO].Rows)
            {
                r += (int)cInfo[colCINFO_ROUTELENGTH];
            }

            return r;
        }

        private static Func<bool> BuildRouteForCinfoSecond(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow cInfo)
        {
            return () =>
            {
                try
                {
                    // Change the thread priority to the one required.
                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;//.Lowest;

                    BuildRouteForCinfoInternal(deliveryPlan, bgnnOrders, cInfo);
                }
                finally
                {
                    // Restore the thread default priority.
                    Thread.CurrentThread.Priority = ThreadPriority.Normal;
                }

                return true;
            };
        }

        //private class RoutePoint
        //{
        //    public DataRow OinfoRow { get; set; }
        //    public DataRow RinfoRow { get; set; }
        //}

        private static void BuildRouteForCinfoInternal(DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow cInfo)
        {
            LinkedList<string> routeList = new LinkedList<string>();

            //DataRow[] 
            var planOrders = deliveryPlan.Tables[tblOINFO].Select().Where(row =>
            {
                if (row is null) return false;

                lock (row)
                {
                    return (((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.COOKING
                            || ((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.READY
                            || ((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.TRANSPORTING
                            || ((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.READY
                            || (((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.BEGINNING && bgnnOrders.Contains(row)))
                            && row[colOINFO_CID].ToString() == cInfo[colCINFO_CID].ToString();
                }
            }).OrderBy<DataRow, DateTime>(row =>
            {
                lock (row)
                {
                    return (DateTime)row[colOINFO_TR] + (TimeSpan)TimeForDeliveringDirect(deliveryPlan, row);
                }
            });//.ToArray();

            string currRID = String.Empty;
            LinkedListNode<string> ridNode = null;
            LinkedListNode<string> oidNode = null;

            foreach (DataRow oInfo in planOrders)
            {
                lock (oInfo)
                {
                    if (((OINFO_STATE)oInfo[colOINFO_STATE]) == OINFO_STATE.TRANSPORTING)
                    {
                        if (((CINFO_STATE)cInfo[colCINFO_STATE]) == CINFO_STATE.ONROAD)
                        {
                            routeList.AddFirst(new LinkedListNode<string>(oInfo[colOINFO_OID].ToString()));
                            continue;
                        }
                    }

                    string ridID = oInfo[colOINFO_RID].ToString();
                    if (ridID != currRID)
                    {
                        ridNode = null;
                        oidNode = null;

                        if (!String.IsNullOrEmpty(currRID))
                        {
                            // dis routeList.AddLast(new LinkedListNode<string>($"{currRID}[]"));
                        }

                        if (((OINFO_STATE)oInfo[colOINFO_STATE]) != OINFO_STATE.TRANSPORTING)
                        {
                            ridNode = new LinkedListNode<string>($"{oInfo[colOINFO_RID].ToString()}[{oInfo[colOINFO_OID].ToString()}]");
                            routeList.AddLast(ridNode);
                        }
                        else
                        { // state of order is OINFO_STATE.TRANSPORTING
                            if (((CINFO_STATE)cInfo[colCINFO_STATE]) != CINFO_STATE.ONROAD)
                            {
                                ridNode = new LinkedListNode<string>($"{oInfo[colOINFO_RID].ToString()}[]");
                                routeList.AddLast(ridNode);
                            }
                        }

                        oidNode = new LinkedListNode<string>(oInfo[colOINFO_OID].ToString());
                        routeList.AddLast(oidNode);

                        currRID = ridID;
                    }
                    else
                    {
                        if (((OINFO_STATE)oInfo[colOINFO_STATE]) != OINFO_STATE.TRANSPORTING)
                        {
                            LinkedListNode<string> nodeRID = new LinkedListNode<string>($"{oInfo[colOINFO_RID].ToString()}[{oInfo[colOINFO_OID].ToString()}]");
                            if (ridNode == null)
                            {
                                routeList.AddFirst(nodeRID);
                            }
                            else
                            {
                                routeList.AddAfter(ridNode, nodeRID);
                            }
                            ridNode = nodeRID;
                        }
                        else
                        { // state of order is OINFO_STATE.TRANSPORTING and state of courier isn't CINFO_STATE.ONROAD
                            LinkedListNode<string> nodeRID = new LinkedListNode<string>($"{oInfo[colOINFO_RID].ToString()}[]");
                            routeList.AddAfter(ridNode, nodeRID);
                            ridNode = nodeRID;
                        }

                        LinkedListNode<string> nodeOID = new LinkedListNode<string>(oInfo[colOINFO_OID].ToString());
                        routeList.AddAfter(oidNode, nodeOID);
                        oidNode = nodeOID;
                    }
                }
            }

            if (!String.IsNullOrEmpty(currRID))
            {
                // dis routeList.AddLast(new LinkedListNode<string>($"{currRID}[]"));
            }

            routeList = TunningRouteList(cInfo, deliveryPlan, routeList);

            string route = routeList.Aggregate<string, string, string>("", (a, b) => String.Concat(a, "-", b), a => a) + "-";

            lock (deliveryPlan)
            {
                int routeLength = GetRouteInfos(cInfo.Table.Rows.IndexOf(cInfo), deliveryPlan, routeList);
                //deliveryPlan.AcceptChanges();

                cInfo[colCINFO_ROUTE] = route;
                cInfo[colCINFO_ROUTELENGTH] = routeLength;
            }
        }

        private static TimeSpan TimeForDeliveringDirect(DataSet deliveryPlan, DataRow row)
        {
            DataRow rInfo = deliveryPlan.Tables[tblRINFO].Rows.Find(row[colOINFO_RID].ToString());

            //double dst = GetGeoPathDistance(rInfo[colRINFO_LAT].ToString(), rInfo[colRINFO_LNG].ToString(), row[colOINFO_LAT].ToString(), row[colOINFO_LNG].ToString());

            GeoRouteData r = GetGeoPathTotalOSRM(DateTime.MinValue, rInfo[colRINFO_LAT].ToString(), rInfo[colRINFO_LNG].ToString(), row[colOINFO_LAT].ToString(), row[colOINFO_LNG].ToString());
            //JsonValue json = JsonValue.Parse(r);
            //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
            //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

            return TimeSpan.FromSeconds(r.Duration);
        }

        private static LinkedList<string> TunningRouteList(DataRow cInfo, DataSet deliveryPlan, LinkedList<string> routeList)
        {
            DataTable OINFO = deliveryPlan.Tables[tblOINFO];
            DataTable RINFO = deliveryPlan.Tables[tblRINFO];
            DataTable CINFO = deliveryPlan.Tables[tblCINFO];

            LinkedList<string> routeTunning = new LinkedList<string>();
            List<DataRow> listPnts = new List<DataRow>();
            listPnts.Add(cInfo);

            for (LinkedListNode<string> idxLinked = routeList.First; idxLinked != null; idxLinked = idxLinked.Next)
            {
                string[] dst = idxLinked.Value.Split("[]".ToCharArray());
                if (dst.Length > 1)
                {
                    if (listPnts.Count > 1) TspAndAdd(listPnts, routeTunning);

                    routeTunning.AddLast(idxLinked.Value);

                    DataRow RinfoRow = RINFO.Rows.Find(dst[0]);
                    listPnts = new List<DataRow>();
                    listPnts.Add(RinfoRow);
                    if (RinfoRow is null) throw new Exception();
                }
                else
                {
                    DataRow OrdersRow = OINFO.Rows.Find(dst[0]);
                    if (OrdersRow is null)
                    {
                        Thread.Sleep(10);
                        OrdersRow = OINFO.Rows.Find(dst[0]);
                    }
                    listPnts.Add(OrdersRow);
                    if (OrdersRow is null) throw new Exception();
                }


                //routeTunning.AddLast(idxLinked.Value);
            }
            if (listPnts.Count > 1) TspAndAdd(listPnts, routeTunning);

            return routeTunning;
        }

        private static void TspAndAdd(List<DataRow> listPnts, LinkedList<string> routeTunning)
        {
            TspTour tour = TspDoTsp(listPnts, false, true);

            //TspStop[] stops = tour.Cycle().ToArray();
            //int cnt = tour.Cycle().Count();
            //for (int i = 1; i < stops.Length; i++)
            foreach (TspStop stop in tour.Cycle().Skip(1))
            {
                //routeTunning.AddLast(stops[i].City.CityID);
                routeTunning.AddLast(stop.City.CityID);
            }
        }

        private static Func<bool> BuildRouteForCinfo(string dir, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow cInfo)
        {
            return () =>
            {
                //string route = String.Join("-", deliveryPlan.Tables[tblORDERS].Select($"CID = '{cInfo[colCINFO_CID]}'").Select<DataRow, string>(r => { return $"{r[colORDERS_RID].ToString()}:{r[colORDERS_OID].ToString()}"; }));

                LinkedList<string> routeList = new LinkedList<string>();

                //DataRow[] transOrders = deliveryPlan.Tables[tblORDERS].Select().Where(row =>
                //{
                //    lock (row)
                //    {
                //        return (((ORDER_STATE)row[colORDERS_STATE]) == ORDER_STATE.TRANSPORTING)
                //            && row[colORDERS_CID].ToString() == cInfo[colCINFO_CID].ToString();
                //    }
                //}).ToArray();

                //List<DataRow> listPnts = new List<DataRow>();// transOrders.Length + 1);
                //listPnts.Add(cInfo);
                //listPnts.AddRange(transOrders);

                //TspTour transTour = TspDoTsp(listPnts, false, false);

                //DataRow lastOinfo = null;
                //foreach (DataRow oInfo in transTour.Cycle().Select<TspStop, DataRow>(stop => stop.City.CityInfo).Skip(1))
                //{
                //    lock (oInfo)
                //    {
                //        routeList.AddLast(new LinkedListNode<string>(oInfo[colORDERS_OID].ToString()));
                //        lastOinfo = oInfo;
                //    }
                //}

                //if (lastOinfo != null)
                //{
                //    lock (lastOinfo)
                //    {
                //        routeList.AddLast(new LinkedListNode<string>($"{lastOinfo[colORDERS_RID].ToString()}[]"));
                //    }
                //}

                //DataRow[] 
                var planOrders = deliveryPlan.Tables[tblOINFO].Select().Where(row =>
                {
                    lock (row)
                    {
                        return (((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.COOKING
                                || ((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.READY
                                || ((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.TRANSPORTING
                                || (((OINFO_STATE)row[colOINFO_STATE]) == OINFO_STATE.READY && bgnnOrders.Contains(row)))
                                && row[colOINFO_CID].ToString() == cInfo[colCINFO_CID].ToString();
                    }
                }).OrderBy<DataRow, DateTime>(row =>
                 {
                     lock (row)
                     {
                         return (DateTime)row[colOINFO_TR];// row[colORDERS_RID].ToString();
                     }
                 });//.ToArray();

                Dictionary<string, List<DataRow>> ordsPackets = new Dictionary<string, List<DataRow>>();

                string ridTRANSTATE = String.Empty;
                foreach (DataRow oInfo in planOrders)
                {
                    lock (oInfo)
                    {
                        string RID = oInfo[colOINFO_RID].ToString();

                        if (((OINFO_STATE)oInfo[colOINFO_STATE]) == OINFO_STATE.TRANSPORTING)
                        {
                            ridTRANSTATE = RID;
                        }

                        if (!ordsPackets.ContainsKey(RID))
                        {
                            //routeList.AddLast(new LinkedListNode<string>($"{oInfo[colORDERS_RID].ToString()}[{oInfo[colORDERS_OID].ToString()}]"));
                            //routeList.AddLast(new LinkedListNode<string>(oInfo[colORDERS_OID].ToString()));

                            ordsPackets.Add(RID, new List<DataRow>() { oInfo });
                        }
                        else
                        {
                            ordsPackets[RID].Add(oInfo);
                        }
                    }
                }

                List<string> ordsPacketsKeys = new List<string>(ordsPackets.Keys.Count + 1);

                {
                    List<DataRow> listPnts = new List<DataRow>();// transOrders.Length + 1);
                    if (String.IsNullOrEmpty(ridTRANSTATE))
                    {
                        listPnts.Add(cInfo);
                    }
                    else
                    {
                        listPnts.Add(deliveryPlan.Tables[tblRINFO].Rows.Find(ridTRANSTATE));
                    }
                    foreach (string rid in ordsPackets.Keys)
                    {
                        listPnts.Add(deliveryPlan.Tables[tblRINFO].Rows.Find(rid));
                    }

                    TspTour transTour = TspDoTsp(listPnts, false, false);

                    foreach (DataRow rInfo in transTour.Cycle().Select<TspStop, DataRow>(stop => stop.City.CityInfo).Skip(1))
                    {
                        lock (rInfo)
                        {
                            ordsPacketsKeys.Add(rInfo[colRINFO_RID].ToString());
                        }
                    }
                }

                //ordsPacketsKeys =  ordsPacketsKeys.OrderBy(rid=>)

                if (!String.IsNullOrEmpty(ridTRANSTATE))
                {
                    List<DataRow> routeRID = ordsPackets[ridTRANSTATE];
                    //List<DataRow> routeRID = ordsPackets[ordsPacketsKeys[0]];// rid];

                    if (routeRID.Count > MAX_ORDERS_FOR_COURIERS) return false;

                    List<DataRow> listPnts = new List<DataRow>(routeRID.Count + 1);
                    listPnts.Add(deliveryPlan.Tables[tblRINFO].Rows.Find(ridTRANSTATE));
                    listPnts.AddRange(routeRID);

                    TspTour transTour = TspDoTsp(listPnts, false, false);

                    var transTourOrders = transTour.Cycle().Select<TspStop, DataRow>(stop => stop.City.CityInfo).Skip(1);

                    foreach (DataRow oInfo in transTourOrders)
                    {
                        lock (oInfo)
                        {
                            if (((OINFO_STATE)oInfo[colOINFO_STATE]) != OINFO_STATE.TRANSPORTING)
                            {
                                routeList.AddLast(new LinkedListNode<string>($"{ridTRANSTATE}[{oInfo[colOINFO_OID].ToString()}]"));
                            }
                        }
                    }
                    foreach (DataRow oInfo in transTourOrders)
                    {
                        lock (oInfo)
                        {
                            routeList.AddLast(new LinkedListNode<string>(oInfo[colOINFO_OID].ToString()));
                        }
                    }
                    routeList.AddLast(new LinkedListNode<string>($"{ridTRANSTATE}[]"));
                }

                //foreach (string rid in ordsPackets.Keys)
                foreach (string rid in ordsPacketsKeys)
                {
                    if (rid == ridTRANSTATE) continue;

                    List<DataRow> routeRID = ordsPackets[rid];
                    //List<DataRow> routeRID = ordsPackets[ordsPacketsKeys[0]];// rid];

                    if (routeRID.Count > MAX_ORDERS_FOR_COURIERS) return false;

                    List<DataRow> listPnts = new List<DataRow>(routeRID.Count + 1);
                    listPnts.Add(deliveryPlan.Tables[tblRINFO].Rows.Find(rid));
                    listPnts.AddRange(routeRID);

                    TspTour transTour = TspDoTsp(listPnts, false, false);

                    var transTourOrders = transTour.Cycle().Select<TspStop, DataRow>(stop => stop.City.CityInfo).Skip(1);

                    foreach (DataRow oInfo in transTourOrders)
                    {
                        lock (oInfo)
                        {
                            if (((OINFO_STATE)oInfo[colOINFO_STATE]) != OINFO_STATE.TRANSPORTING)
                            {
                                routeList.AddLast(new LinkedListNode<string>($"{rid}[{oInfo[colOINFO_OID].ToString()}]"));
                            }
                        }
                    }
                    foreach (DataRow oInfo in transTourOrders)
                    {
                        lock (oInfo)
                        {
                            routeList.AddLast(new LinkedListNode<string>(oInfo[colOINFO_OID].ToString()));
                        }
                    }
                    routeList.AddLast(new LinkedListNode<string>($"{rid}[]"));
                }

                string route = routeList.Aggregate<string, string, string>("", (a, b) => String.Concat(a, "-", b), a => a) + "-";

                lock (deliveryPlan)
                {
                    int routeLength = GetRouteInfos(cInfo.Table.Rows.IndexOf(cInfo), deliveryPlan, routeList);
                    //deliveryPlan.AcceptChanges();

                    cInfo[colCINFO_ROUTE] = route;
                    cInfo[colCINFO_ROUTELENGTH] = routeLength;
                }

                return true;
            };
        }

        private static void ClearRouteInformation(DataSet deliveryPlan)
        {
            //

            foreach (DataRow cInfo in deliveryPlan.Tables[tblCINFO].Rows)
            {
                cInfo[colCINFO_ROUTE] = "-";
                cInfo[colCINFO_ROUTELENGTH] = 0;
            }

            deliveryPlan.AcceptChanges();
        }

        private static void PlanningForCartesianOrderStateCookingReady(string dir, int vOrder, DataSet deliveryPlan, DataRowCollection OrdersRows, DataRow[] bgnnOrders)
        {
            WatchPlanningForCartesianOrderStateCookingReady.Start();
            foreach (DataRow c in ResortRowsCinfo(deliveryPlan, deliveryPlan.Tables[tblRINFO].Rows.Find(OrdersRows[vOrder][colOINFO_RID]), deliveryPlan.Tables[tblCINFO].Rows).Take(MAX_COURIERS_FOR_PLANNING))

            {
                OrdersRows[vOrder][colOINFO_CID] = c[colCINFO_CID];
                deliveryPlan.Tables[tblOINFO].AcceptChanges();

                WatchPlanningForCartesianOrderStateCookingReady.Stop();
                PlanningForCartesian(dir + "(" + OrdersRows[vOrder][colOINFO_RID].ToString() + "-" + c[colCINFO_CID].ToString() + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                //PlanningForCartesian(dir + "(" + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                WatchPlanningForCartesianOrderStateCookingReady.Start();
            }
        }

        private static void PlanningForCartesianOrderStateBeginning(string dir, int vOrder, DataSet deliveryPlan, DataRowCollection OrdersRows, DataRow[] bgnnOrders)
        {
            if (bgnnOrders.Contains(OrdersRows[vOrder]))
            {
                WatchPlanningForCartesianOrderStateBeginning.Start();
                foreach (DataRow r in ResortRowsRinfo(deliveryPlan, OrdersRows[vOrder], deliveryPlan.Tables[tblRINFO].Rows).Take(MAX_RESTAURANTS_FOR_PLANNING))
                {
                    foreach (DataRow c in ResortRowsCinfo(deliveryPlan, r, deliveryPlan.Tables[tblCINFO].Rows).Take(MAX_COURIERS_FOR_PLANNING))
                    {
                        OrdersRows[vOrder][colOINFO_RID] = r[colRINFO_RID];
                        OrdersRows[vOrder][colOINFO_CID] = c[colCINFO_CID];
                        deliveryPlan.Tables[tblOINFO].AcceptChanges();

                        WatchPlanningForCartesianOrderStateBeginning.Stop();
                        PlanningForCartesian(dir + "(" + r[colRINFO_RID].ToString() + "-" + c[colCINFO_CID].ToString() + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                        //PlanningForCartesian(dir + "(" + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                        WatchPlanningForCartesianOrderStateBeginning.Start();
                    }
                }
            }
            else
            {
                //PlanningForCartesian(dir + "(" + OrdersRows[vOrder][colORDERS_RID].ToString() + "-" + OrdersRows[vOrder][colORDERS_CID].ToString() + ")", vOrder + 1, deliveryPlan, bgnnOrders);
                PlanningForCartesian(dir + "(" + ")", vOrder + 1, deliveryPlan, bgnnOrders);
            }
        }

        private static IEnumerable<DataRow> ResortRowsCinfo(DataSet deliveryPlan, DataRow dataRow, DataRowCollection rows)
        {
            DateTime buildt = (DateTime)deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"];

            string latD = dataRow["LAT"].ToString();
            string lngD = dataRow["LNG"].ToString();

            var sortRows = rows.OfType<DataRow>().OrderBy<DataRow, DateTime>(row =>
            {
                string latS = row["LAT"].ToString();
                string lngS = row["LNG"].ToString();

                //string r = GetGeoPathTotal2Gis(latS, lngS, latD, lngD);
                //string r = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                GeoRouteData r = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                //JsonValue json = JsonValue.Parse(r);

                //double distBet = GetGeoPathDistance(latS, lngS, latD, lngD);

                //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                //int lll = int.Parse(json["result"][0]["length"].ToString());
                //int ddd = (int)distBet / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());
                //return latD * latD + lngD * lngD;

                DateTime sortDateTime = buildt.AddSeconds(r.Duration);

                //DataRow[] 
                var ordsCinfo = deliveryPlan.Tables[tblOINFO].Select().Where(
                    rowOinfo =>
                    {
                        return ((OINFO_STATE)rowOinfo[colOINFO_STATE] == OINFO_STATE.TRANSPORTING
                        || (OINFO_STATE)rowOinfo[colOINFO_STATE] == OINFO_STATE.PLACING
                        || (OINFO_STATE)rowOinfo[colOINFO_STATE] == OINFO_STATE.ENDED)
                        && rowOinfo[colOINFO_CID].ToString().Contains(row[colCINFO_CID].ToString());
                    });//.ToArray();

                foreach (DataRow ord in ordsCinfo)
                {
                    latS = ord["LAT"].ToString();
                    lngS = ord["LNG"].ToString();

                    //string r = GetGeoPathTotal2Gis(latS, lngS, latD, lngD);
                    //string r_internal = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                    GeoRouteData r_internal = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                    //JsonValue json_internal = JsonValue.Parse(r);

                    //double distBetInternal = GetGeoPathDistance(latS, lngS, latD, lngD);

                    //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                    //int lll = int.Parse(json["result"][0]["length"].ToString());
                    //int ddd_internal = (int)distBetInternal / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                    //int lll_internal = (int)distBetInternal;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                    //int ddd_internal = (int)double.Parse(json["routes"][0]["duration"].ToString());
                    //int lll_internal = (int)double.Parse(json["routes"][0]["distance"].ToString());

                    DateTime sortDateTime_internal = ((DateTime)ord[colOINFO_TE]).AddSeconds(r_internal.Duration);

                    if (sortDateTime_internal > sortDateTime) sortDateTime = sortDateTime_internal;
                }

                return sortDateTime; // + duration for available...
            });

            //DataRow[] rrr = sortRows.ToArray<DataRow>();

            string debugCinfoOrder = String.Join("-", sortRows.Select<DataRow, string>(r => { return r[colCINFO_CID].ToString(); }));

            Console.WriteLine($"ResortRowsCinfo: {dataRow[colRINFO_RID].ToString()} {dataRow.Table.TableName}: {sortRows.Count()/*.Length*/} {debugCinfoOrder}");
            WatchResortRows.Stop();
            //return sortRows.OfType<DataRow>();
            return sortRows;//.Reverse();
        }

        private static IEnumerable<DataRow> ResortRowsRinfo(DataSet deliveryPlan, DataRow dataRow, DataRowCollection rows)
        {
            DateTime buildt = (DateTime)deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"];

            string latD = dataRow["LAT"].ToString();
            string lngD = dataRow["LNG"].ToString();

            var sortRows = rows.OfType<DataRow>().OrderBy<DataRow, DateTime>(row =>
            {
                string latS = row["LAT"].ToString();
                string lngS = row["LNG"].ToString();

                //string r = GetGeoPathTotal2Gis(latS, lngS, latD, lngD);
                //string r = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                GeoRouteData r = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                //JsonValue json = JsonValue.Parse(r);

                //double distBet = GetGeoPathDistance(latS, lngS, latD, lngD);

                //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                //int lll = int.Parse(json["result"][0]["length"].ToString());
                //int ddd = (int)distBet / 14; ;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());
                //return latD * latD + lngD * lngD;
                return buildt.AddSeconds(r.Duration); // + duration of cooking...
            });

            //DataRow[] rrr = sortRows.ToArray<DataRow>();

            string debugRinfoOrder = String.Join("-", sortRows.Select<DataRow, string>(r => { return r[colRINFO_RID].ToString(); }));

            Console.WriteLine($"ResortRowsRinfo: {dataRow[colOINFO_OID].ToString()} {dataRow.Table.TableName}: {sortRows.Count()/*.Length*/} {debugRinfoOrder}");
            WatchResortRows.Stop();
            //return sortRows.OfType<DataRow>();
            return sortRows;//.Reverse();
        }

        // 
        private static IEnumerable<DataRow> ResortRows(DataRow dataRow, DataRowCollection rows)
        {
            WatchResortRows.Start();
            //throw new NotImplementedException();
            double latS = double.Parse(dataRow["LAT"].ToString());
            double lngS = double.Parse(dataRow["LNG"].ToString());

            var sortRows = rows.OfType<DataRow>().OrderBy<DataRow, double>(row =>
             {
                 double latD = double.Parse(row["LAT"].ToString()) - latS;
                 double lngD = double.Parse(row["LNG"].ToString()) - lngS;
                 return latD * latD + lngD * lngD;
             });

            //DataRow[] rrr = sortRows.ToArray<DataRow>();
            Console.WriteLine($"{dataRow.Table.TableName}: {sortRows.Count()/*.Length*/}");
            WatchResortRows.Stop();
            //return sortRows.OfType<DataRow>();
            return sortRows;
        }

        private static void PlanningForCinfo(string dir, int vCinfo, DataSet deliveryPlan, DataRow[] bgnnOrders, long totalRouteLength)
        {
            //Thread.Sleep(5);
            WatchPlanningForCinfo.Start();
            //throw new NotImplementedException();

            // 20190506 if (totalRouteLength > TspRouteLength) return;

            DataTable ORDERS = deliveryPlan.Tables[tblOINFO];
            DataTable CINFO = deliveryPlan.Tables[tblCINFO];

            if (vCinfo >= CINFO.Rows.Count)
            {
                WatchPlanningForCinfo.Stop();
                WriteCurrentDeliveryPlan(dir, deliveryPlan, totalRouteLength);
                WatchPlanningForCinfo.Start();
            }
            else
            {
                DataRow[] ords = ORDERS.Select($"CID = '{CINFO.Rows[vCinfo][colCINFO_CID]}'");

                //string route = ords.Aggregate<DataRow, string, string>("", (a, b) => String.Concat(a, "-", b["RID"].ToString(), "-", b["OID"].ToString()), a => a);
                //deliveryPlan.Tables["CINFO"].Rows[vCinfo]["ROUTE"] = route;
                //deliveryPlan.AcceptChanges();

                //PlanningForCinfo(dir + "(" + route + ")", vCinfo + 1, deliveryPlan);

                WatchPlanningForCinfo.Stop();
                if (ords.Length <= MAX_ORDERS_FOR_COURIERS)
                {
                    PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, 0, new LinkedList<string>(), totalRouteLength);
                }
                WatchPlanningForCinfo.Start();
            }
        }

        private static void WriteCurrentDeliveryPlan(string dir, DataSet deliveryPlan, long totalRouteLength)
        {
            WatchWriteCurrentDeliveryPlan.Start();
            deliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALENGTH"] = totalRouteLength;

            CalculateStatistics(deliveryPlan);
            UpdateMedDiv(deliveryPlan);

            deliveryPlan.AcceptChanges();

            TimeSpan med = (TimeSpan)deliveryPlan.Tables["SUMMARY"].Rows[0]["MED"];
            TimeSpan div = (TimeSpan)deliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"];

            string[] paths = dir.Split(Path.DirectorySeparatorChar);
            //long totalLength = (long)CINFO.Compute("SUM(ROUTELENGTH)", "");

            //if (totalLength > TspRouteLength) return;

            //string dirName = Path.GetDirectoryName(dir);
            //string fileName = Path.GetFileName(dir);

            //string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), paths[0], String.Join("-", totalRouteLength.ToString("D8"), med.ToString("hhmmss"), div.ToString("hhmmss")) + paths[1] + ".xml");
            //string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dirName, String.Join("-", totalRouteLength.ToString("D8"), med.ToString("hhmmss"), div.ToString("hhmmss")) + fileName + ".xml");
            //string htmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dirName, String.Join("-", totalRouteLength.ToString("D8"), med.ToString("hhmmss"), div.ToString("hhmmss")) + fileName + ".html");

            //Path.GetRandomFileName());                                                                                                                                                                                                                
            //string xmlFileName = Path.Combine(Directory.GetCurrentDirectory(), dir, "(" + deliveryPlan.Tables["ORDERS"].Rows. + ")(" + deliveryPlan.Tables["ORDERS"].Rows[v]["CID"] + ").xml");                                                                                                                                                                                                                
            //string xmlFileName = dir + ".xml";

            //if (File.Exists(xmlFileName))
            {
                //    xmlFileName += "(" + Path.GetRandomFileName() + ")";
            }

            //Console.WriteLine($"write DP with total length {totalRouteLength}");
            Console.Write(".");
            //deliveryPlan.WriteXml(xmlFileName);
            //WriteHtmlVersionTheBestDeliveryPlan(deliveryPlan, htmlFileName);

            string fnBUILDT = ((DateTime)deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"]).ToString("yyyy-MM-dd-HH-mm");
            string fnBUILDO = deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDO"].ToString();
            string fnTOTALENGTH = deliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALENGTH"].ToString();
            string filenameTBDP = $"CODP-{fnBUILDT}-{fnBUILDO}-{fnTOTALENGTH}";

            //deliveryPlan.WriteXml(Path.Combine(BaseDirectoryForCDP,  $"{filenameTBDP}-{Path.GetRandomFileName()}.xml"));

            if (TheBestDeliveryPlan == null)
            {
                TheBestDeliveryPlan = deliveryPlan.Copy();
                Console.WriteLine($"new TBDP");
            }
            else
            {
                if (CompareMedAndDiv(TheBestDeliveryPlan, deliveryPlan))
                {
                    TheBestDeliveryPlan = deliveryPlan.Copy();
                    Console.WriteLine($"new TBDP");
                }
            }

            if (TheFastDeliveryPlan == null)
            {
                TheFastDeliveryPlan = deliveryPlan.Copy();
                Console.WriteLine($"new TFDP");
            }
            else
            {
                if (CompareTotalDuration(TheFastDeliveryPlan, deliveryPlan))
                {
                    TheFastDeliveryPlan = deliveryPlan.Copy();
                    Console.WriteLine($"new TFDP");
                }
            }

            if (TheShortDeliveryPlan == null)
            {
                TheShortDeliveryPlan = deliveryPlan.Copy();
                Console.WriteLine($"new TSDP");
            }
            else
            {
                if ((totalRouteLength > 0) && CompareTotalength(TheShortDeliveryPlan, totalRouteLength))// || 
                {
                    TheShortDeliveryPlan = deliveryPlan.Copy();
                    Console.WriteLine($"new TSDP");
                }
            }

            TheWorkDeliveryPlan = TheBestDeliveryPlan;

            WatchWriteCurrentDeliveryPlan.Stop();
        }

        private static bool CompareTotalDuration(DataSet theBestDeliveryPlan, DataSet deliveryPlan)
        {
            TimeSpan theBestDur = (TimeSpan)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALDURATION"];
            TimeSpan thePlanDur = (TimeSpan)deliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALDURATION"];

            return thePlanDur < theBestDur;
        }

        private static bool CompareTotalength(DataSet theBestDeliveryPlan, long totalRouteLength)
        {
            return totalRouteLength < ((int)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALENGTH"]);
        }

        private static bool CompareMedAndDiv(DataSet theBestDeliveryPlan, DataSet deliveryPlan)
        {
            TimeSpan theBestMed = (TimeSpan)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["MED"];
            TimeSpan theBestDiv = (TimeSpan)theBestDeliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"];

            TimeSpan planMed = (TimeSpan)deliveryPlan.Tables["SUMMARY"].Rows[0]["MED"];
            TimeSpan planDiv = (TimeSpan)deliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"];

            return ((planMed <= theBestMed) && (planDiv < theBestDiv)) || ((planMed < theBestMed) && (planDiv <= theBestDiv));
        }

        private static void UpdateMedDiv(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();

            TimeSpan med = TimeSpan.FromSeconds(0);
            TimeSpan div = TimeSpan.FromSeconds(0);
            int rowsCount = 0;//deliveryPlan.Tables[tblORDERS].Rows.Count;

            deliveryPlan.Tables[tblOINFO].Select().All<DataRow>(dr =>
            {
                if ((TimeSpan)dr[colOINFO_TD] > TimeSpan.FromTicks(0))
                {
                    //med += TimeSpan.FromTicks(((TimeSpan)dr[colORDERS_TD]).Ticks / rowsCount);
                    med += ((DateTime)dr[colOINFO_TP]) - ((DateTime)dr[colOINFO_TR]);  //(TimeSpan)dr[colOINFO_TD];
                    rowsCount++;
                }
                return true;
            });

            if (rowsCount != 0)
            {
                deliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALDURATION"] = med;

                med = TimeSpan.FromTicks(med.Ticks / rowsCount);

                deliveryPlan.Tables[tblOINFO].Select().All<DataRow>(dr =>
                {
                    if ((TimeSpan)dr[colOINFO_TD] > TimeSpan.FromTicks(0))
                    {
                        //long tk = med.Ticks;
                        div += TimeSpan.FromTicks(Math.Abs((med.Ticks - ((TimeSpan)dr[colOINFO_TD]).Ticks)) / rowsCount);
                    }
                    return true;
                });

                deliveryPlan.Tables["SUMMARY"].Rows[0]["MED"] = med;
                deliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"] = div;
            }
            else
            {
                deliveryPlan.Tables["SUMMARY"].Rows[0]["MED"] = TimeSpan.FromTicks(0);
                deliveryPlan.Tables["SUMMARY"].Rows[0]["DIV"] = TimeSpan.FromTicks(0);
                deliveryPlan.Tables["SUMMARY"].Rows[0]["TOTALDURATION"] = TimeSpan.FromTicks(0);
            }

            deliveryPlan.AcceptChanges();
        }

        private static void CalculateStatistics(DataSet deliveryPlan)
        {
            //throw new NotImplementedException();

            deliveryPlan.Tables[tblOINFO].Select().All<DataRow>(dr =>
            {
                DateTime TOT = (DateTime)dr[colOINFO_TOT];
                DateTime TP = (DateTime)dr[colOINFO_TP];

                dr[colOINFO_TD] = TP - TOT;
                return true;
            });
            deliveryPlan.AcceptChanges();
        }

        private static void PlanningForRoutes(string dir, int vCinfo, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow[] ords, int vDpair, LinkedList<string> listRoute, long totalRouteLength)
        {
            WatchPlanningForRoutes.Start();
            //throw new NotImplementedException();

            if (vDpair >= ords.Length)
            {
                string route = listRoute.Aggregate<string, string, string>("", (a, b) => String.Concat(a, "-", b), a => a) + "-";

                int routeLength = GetRouteInfos(vCinfo, deliveryPlan, listRoute);

                DataRowCollection CinfoRows = deliveryPlan.Tables[tblCINFO].Rows;
                CinfoRows[vCinfo][colCINFO_ROUTELENGTH] = routeLength;
                CinfoRows[vCinfo][colCINFO_ROUTE] = route;
                deliveryPlan.AcceptChanges();

                //PlanningForCinfo(dir + "(" + route + ")", vCinfo + 1, deliveryPlan, totalRouteLength + routeLength);
                WatchPlanningForRoutes.Stop();
                PlanningForCinfo(dir, vCinfo + 1, deliveryPlan, bgnnOrders, totalRouteLength + routeLength);
                WatchPlanningForRoutes.Start();
            }
            else
            {
                LinkedList<string> newRoute = new LinkedList<string>(listRoute);
                //int firstR = 0;
                LinkedListNode<string> firstRlinked = newRoute.First;

                if (firstRlinked != null)
                {
                    //firstR = 0;
                    string[] pnt;
                    do
                    {
                        //pnt = listRoute[firstR].Split("[]".ToCharArray());
                        pnt = firstRlinked.Value.Split("[]".ToCharArray());
                        if (pnt.Length > 1) break;
                        //if (++firstR >= listRoute.Count) break;
                        firstRlinked = firstRlinked.Next;
                        if (firstRlinked == null) break;
                    }
                    while (false);
                    //firstR--;
                    //firstRlinked = firstRlinked.Previous;
                }

                switch ((OINFO_STATE)ords[vDpair][colOINFO_STATE])
                {
                    case OINFO_STATE.BEGINNING:
                        WatchPlanningForRoutes.Stop();
                        if (!bgnnOrders.Contains(ords[vDpair]))
                        {
                            PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
                        }
                        else
                        {
                            PlanningForRoutesOrderStateBeginningCookingReady(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair, totalRouteLength, newRoute, firstRlinked);
                        }
                        WatchPlanningForRoutes.Start();
                        break;

                    case OINFO_STATE.COOKING:
                    case OINFO_STATE.READY:

                        WatchPlanningForRoutes.Stop();
                        PlanningForRoutesOrderStateBeginningCookingReady(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair, totalRouteLength, newRoute, firstRlinked);
                        WatchPlanningForRoutes.Start();
                        break;

                    case OINFO_STATE.TRANSPORTING:

                        WatchPlanningForRoutes.Stop();
                        PlanningForRoutesOrderStateBeginningTransporting(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair, totalRouteLength, newRoute);
                        WatchPlanningForRoutes.Start();
                        break;

                    case OINFO_STATE.PLACING:
                    case OINFO_STATE.ENDED:

                        {
                            //LinkedList<string> newRoute = new LinkedList<string>(listRoute);
                            WatchPlanningForRoutes.Stop();
                            PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
                            WatchPlanningForRoutes.Start();
                        }

                        break;

                    default:

                        break;

                }
            }
        }

        private static void PlanningForRoutesOrderStateBeginningTransporting(string dir, int vCinfo, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow[] ords, int vDpair, long totalRouteLength, LinkedList<string> newRoute)
        {
            string OID = ords[vDpair][colOINFO_OID].ToString();

            if (newRoute.First == null)
            {
                newRoute.AddFirst(OID);

                PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
            }
            else
            {
                LinkedListNode<string> stopNode = null;
                //if (firstRlinked != null) stopNode = firstRlinked.Next;
                //for (int Opos = 0; Opos <= firstR; Opos++)
                for (LinkedListNode<string> OposLinked = newRoute.First; OposLinked != null; OposLinked = OposLinked.Next)
                {
                    //LinkedList<string> newRoute = new LinkedList<string>(listRoute);
                    //string OID = ords[vDpair][colORDERS_OID].ToString();

                    LinkedListNode<string> oidNode = newRoute.AddBefore(OposLinked, OID);

                    PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);

                    newRoute.Remove(oidNode);

                    string[] pnt = OposLinked.Value.Split("[]".ToCharArray());
                    if (pnt.Length > 1)
                    {
                        stopNode = OposLinked;
                        break;
                    }

                }
                if (stopNode == null)
                {
                    //string OID = ords[vDpair][colORDERS_OID].ToString();

                    newRoute.AddLast(OID);

                    PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
                }
            }
        }

        private static void PlanningForRoutesOrderStateBeginningCookingReady(string dir, int vCinfo, DataSet deliveryPlan, DataRow[] bgnnOrders, DataRow[] ords, int vDpair, long totalRouteLength, LinkedList<string> newRoute, LinkedListNode<string> firstRlinked)
        {
            string RID = ords[vDpair][colOINFO_RID].ToString();
            string OID = ords[vDpair][colOINFO_OID].ToString();

            if (newRoute.First == null)
            {
                //string RID = ords[vDpair][colORDERS_RID].ToString();
                //LinkedList<string> newRoute = new LinkedList<string>(listRoute);
                //string OID = ords[vDpair][colORDERS_OID].ToString();

                //newRoute.Insert(Opos, OID);
                //newRoute.Insert(Rpos, RID + "[" + OID + "]");
                newRoute.AddFirst(OID);
                newRoute.AddFirst(RID + "[" + OID + "]");

                PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
            }
            else
            {
                //for (int Opos = firstR; Opos <= listRoute.Count; Opos++)

                if (firstRlinked == null)
                {
                    //string RID = ords[vDpair][colORDERS_RID].ToString();
                    //string OID = ords[vDpair][colORDERS_OID].ToString();

                    newRoute.AddLast(RID + "[" + OID + "]");
                    newRoute.AddLast(OID);

                    PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
                }
                else
                {
                    //string RID = ords[vDpair][colORDERS_RID].ToString();

                    for (LinkedListNode<string> OposLinked = firstRlinked; OposLinked != null; OposLinked = OposLinked.Next)
                    {
                        //for (int Opos = 0; Opos <= 0; Opos++)
                        //for (int Rpos = firstR; Rpos <= Opos; Rpos++)
                        for (LinkedListNode<string> RposLinked = firstRlinked; RposLinked != OposLinked.Next; RposLinked = RposLinked.Next)
                        {
                            if (newRoute.First != null)
                            {
                                if (RposLinked != newRoute.First)
                                {
                                    //string[] pnt = listRoute[Rpos - 1].Split("[]".ToCharArray());
                                    string[] pnt = RposLinked.Value.Split("[]".ToCharArray());
                                    if (pnt.Length > 1)
                                    {
                                        if (pnt[0] != RID)
                                        {
                                            continue;
                                        }
                                    }
                                }

                                if (RposLinked != OposLinked)
                                {
                                    //string[] pnt = listRoute[Rpos].Split("[]".ToCharArray());
                                    string[] pnt = RposLinked.Value.Split("[]".ToCharArray());
                                    if (pnt.Length > 1)
                                    {
                                        //if (pnt[0] != RID)
                                        {
                                            continue;
                                        }
                                    }
                                }
                            }

                            //LinkedList<string> newRoute = new LinkedList<string>(listRoute);
                            //string OID = ords[vDpair][colORDERS_OID].ToString();

                            //newRoute.Insert(Opos, OID);
                            //newRoute.Insert(Rpos, RID + "[" + OID + "]");
                            LinkedListNode<string> ridNode = newRoute.AddBefore(RposLinked, RID + "[" + OID + "]");
                            LinkedListNode<string> oidNode = newRoute.AddBefore(OposLinked, OID);

                            PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);

                            newRoute.Remove(ridNode);
                            newRoute.Remove(oidNode);
                        }
                    }
                    {
                        //string RID = ords[vDpair][colORDERS_RID].ToString();
                        //string OID = ords[vDpair][colORDERS_OID].ToString();

                        newRoute.AddLast(RID + "[" + OID + "]");
                        newRoute.AddLast(OID);

                        PlanningForRoutes(dir, vCinfo, deliveryPlan, bgnnOrders, ords, vDpair + 1, newRoute, totalRouteLength);
                    }
                }
            }
        }

        private static int GetRouteInfos(int vCinfo, DataSet deliveryPlan, LinkedList<string> listRoute)
        {
            WatchGetRouteInfos.Start();
            //throw new NotImplementedException();

            DateTime buildt = (DateTime)deliveryPlan.Tables["SUMMARY"].Rows[0]["BUILDT"];

            DataTable OINFO = deliveryPlan.Tables[tblOINFO];
            DataTable RINFO = deliveryPlan.Tables[tblRINFO];
            DataTable CINFO = deliveryPlan.Tables[tblCINFO];

            int len = 0;

            string latS, lngS, latD, lngD;

            DateTime TOS = (DateTime)CINFO.Rows[vCinfo][colCINFO_TOS];
            latS = (string)CINFO.Rows[vCinfo][colCINFO_LAT];
            lngS = (string)CINFO.Rows[vCinfo][colCINFO_LNG];

            //for (int idx = 0; idx < listRoute.Count; idx++)
            for (LinkedListNode<string> idxLinked = listRoute.First; idxLinked != null; idxLinked = idxLinked.Next)
            {
                //string[] src = listRoute[idx - 1].Split("[]".ToCharArray());

                //if (src.Length > 1)
                //{
                //    latS = deliveryPlan.Tables["RINFO"].Select($"RID = '{src[0]}'")[0]["LAT"].ToString();
                //    lngS = deliveryPlan.Tables["RINFO"].Select($"RID = '{src[0]}'")[0]["LNG"].ToString();
                //}
                //else
                //{
                //    latS = deliveryPlan.Tables["ORDERS"].Select($"OID = '{src[0]}'")[0]["LAT"].ToString();
                //    lngS = deliveryPlan.Tables["ORDERS"].Select($"OID = '{src[0]}'")[0]["LNG"].ToString();
                //}

                string OID;
                //string[] dst = listRoute[idx].Split("[]".ToCharArray());
                string[] dst = idxLinked.Value.Split("[]".ToCharArray());
                if (dst.Length > 1)
                {
                    //latD = RINFO.Select($"RID = '{dst[0]}'")[0]["LAT"].ToString();
                    //lngD = RINFO.Select($"RID = '{dst[0]}'")[0]["LNG"].ToString();
                    DataRow RinfoRow = RINFO.Rows.Find(dst[0]);
                    latD = RinfoRow[colRINFO_LAT].ToString();
                    lngD = RinfoRow[colRINFO_LNG].ToString();
                    OID = dst[1];
                }
                else
                {
                    //latD = ORDERS.Select($"OID = '{dst[0]}'")[0]["LAT"].ToString();
                    //lngD = ORDERS.Select($"OID = '{dst[0]}'")[0]["LNG"].ToString();
                    DataRow OrdersRow = OINFO.Rows.Find(dst[0]);
                    latD = OrdersRow[colOINFO_LAT].ToString();
                    lngD = OrdersRow[colOINFO_LNG].ToString();
                    OID = dst[0];
                }

                //string r = GetGeoPathTotal2Gis(latS, lngS, latD, lngD);
                //string r = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                GeoRouteData r = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                //JsonValue json = JsonValue.Parse(r);

                //double distBet = GetGeoPathDistance(latS, lngS, latD, lngD);

                //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                //int lll = int.Parse(json["result"][0]["length"].ToString());
                //int ddd = (int)distBet / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

                len += r.Distance;

                TOS = TOS.AddSeconds(r.Duration);
                DataRow dr = OINFO.Rows.Find(OID);// ORDERS.Select($"OID = '{OID}'")[0];
                if (dr != null)
                {
                    if (dst.Length > 1)
                    {
                        DateTime TR = (DateTime)dr["TR"];

                        if (TR > TOS) TOS = TR;

                        TOS = TOS.AddMinutes(5);
                        lock (dr)
                        {
                            dr[colOINFO_TT] = TOS;
                        }
                    }
                    else
                    {
                        lock (dr)
                        {
                            dr[colOINFO_TP] = TOS;
                            TOS = TOS.AddMinutes(5);
                            dr[colOINFO_TE] = TOS;
                        }
                    }
                }

                latS = latD;
                lngS = lngD;
            }

            {
                string RID4S = (string)CINFO.Rows[vCinfo][colCINFO_RID4S];
                DataRow rinfoRow = RINFO.Rows.Find(RID4S);
                latD = (string)rinfoRow[colRINFO_LAT];
                lngD = (string)rinfoRow[colRINFO_LNG];

                //string r = GetGeoPathTotal2Gis(latS, lngS, latD, lngD);
                //string r = GetGeoPathTotalYandex(buildt, latS, lngS, latD, lngD);
                GeoRouteData r = GetGeoPathTotalOSRM(buildt, latS, lngS, latD, lngD);
                //JsonValue json = JsonValue.Parse(r);

                //double distBet = GetGeoPathDistance(latS, lngS, latD, lngD);

                //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                //int lll = int.Parse(json["result"][0]["length"].ToString());
                //int ddd = (int)distBet / 14;// int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                //int lll = (int)distBet;// int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

                //len += lll;
            }

            WatchGetRouteInfos.Stop();
            return len;
        }

        /// <summary>
        /// Reads the plan.
        /// </summary>
        private static DataSet ReadTestPlan()
        {
            //DataTable tbl = new DataTable("ORDERS");

            //tbl.Columns.Add("STATE", typeof(ORDER_STATE));
            //tbl.Columns.Add("OID", typeof(string));
            //tbl.Columns.Add("ADDRESS", typeof(string));
            //tbl.Columns.Add("LAT", typeof(string));
            //tbl.Columns.Add("LNG", typeof(string));
            //tbl.Columns.Add("TB", typeof(DateTime));
            //tbl.Columns.Add("TC", typeof(DateTime));
            //tbl.Columns.Add("TR", typeof(DateTime));
            //tbl.Columns.Add("TT", typeof(DateTime));
            //tbl.Columns.Add("TP", typeof(DateTime));
            //tbl.Columns.Add("TE", typeof(DateTime));
            //tbl.Columns.Add("TOT", typeof(DateTime));
            //tbl.Columns.Add("RID", typeof(string));
            //tbl.Columns.Add("CID", typeof(string));
            ////tbl.Columns.Add("MAPATH", typeof(string));
            //tbl.Columns.Add("TD", typeof(TimeSpan));

            //tbl.ReadXml("testwritexml.xml");

            DataSet set = new DataSet("DLVR");
            set.ReadXmlSchema("testxmlschema.xml");
            set.ReadXml("testwriteset.xml");

            return set;
        }

        private static void CreateSchemaForDeliveryPlan(string[] args)
        {
            //string[] lines = File.ReadAllLines(args[0]);

            //foreach (string str in lines)
            {
                //string[] row = str.Split('\t');
            }

            DataTable summary = new DataTable("SUMMARY");

            summary.Columns.Add("BUILDT", typeof(DateTime));
            summary.Columns.Add("BUILDS", typeof(OINFO_STATE));
            summary.Columns.Add("BUILDO", typeof(string));
            summary.Columns.Add("MED", typeof(TimeSpan));
            summary.Columns.Add("DIV", typeof(TimeSpan));
            summary.Columns.Add("TOTALENGTH", typeof(int));
            summary.Columns.Add("TOTALDURATION", typeof(TimeSpan));

            summary.AcceptChanges();

            DataTable Rinfo = new DataTable("RINFO");

            Rinfo.Columns.Add("RSTATE", typeof(RINFO_STATE));
            Rinfo.Columns.Add("RID", typeof(string));
            Rinfo.Columns.Add("ADDRESS", typeof(string));
            Rinfo.Columns.Add("LAT", typeof(string));
            Rinfo.Columns.Add("LNG", typeof(string));
            Rinfo.PrimaryKey = new DataColumn[] { Rinfo.Columns["RID"] };

            {
                DataRow row = Rinfo.NewRow();
                row["RID"] = "ТМ3";
                row["ADDRESS"] = "г.Тула, Ложевая ул, 125";
                row["LAT"] = "54.203069";
                row["LNG"] = "37.642916";
                Rinfo.Rows.Add(row);

                row = Rinfo.NewRow();
                row["RID"] = "ТМ18";
                row["ADDRESS"] = "г.Тула, Вильямса ул, 38В";
                row["LAT"] = "54.209708";
                row["LNG"] = "37.690273";
                Rinfo.Rows.Add(row);
            }
            Rinfo.AcceptChanges();

            DataTable Cinfo = new DataTable("CINFO");

            Cinfo.Columns.Add("CSTATE", typeof(CINFO_STATE));
            Cinfo.Columns.Add("CID", typeof(string));
            Cinfo.Columns.Add("ADDRESS", typeof(string));
            Cinfo.Columns.Add("LAT", typeof(string));
            Cinfo.Columns.Add("LNG", typeof(string));
            Cinfo.Columns.Add("TOS", typeof(DateTime));
            Cinfo.Columns.Add("ROUTE", typeof(string));
            Cinfo.Columns.Add("ROUTELENGTH", typeof(int));
            Cinfo.Columns.Add("RID4S", typeof(string));
            Cinfo.PrimaryKey = new DataColumn[] { Cinfo.Columns["CID"] };

            {
                DataRow row = Cinfo.NewRow();
                row["CID"] = "K3";
                row["ADDRESS"] = "г.Тула, Ложевая ул, 125";
                row["LAT"] = "54.203069";
                row["LNG"] = "37.642916";
                row["TOS"] = DateTime.Now;
                Cinfo.Rows.Add(row);

                row = Cinfo.NewRow();
                row["CID"] = "K181";
                row["ADDRESS"] = "г.Тула, Вильямса ул, 38В";
                row["LAT"] = "54.209708";
                row["LNG"] = "37.690273";
                row["TOS"] = DateTime.Now;
                Cinfo.Rows.Add(row);

                //row = Cinfo.NewRow();
                //row["CID"] = "K182";
                //row["ADDRESS"] = "г.Тула, Вильямса ул, 38В";
                //row["TOS"] = DateTime.Now;
                //Cinfo.Rows.Add(row);

                //row = Cinfo.NewRow();
                //row["CID"] = "K183";
                //row["ADDRESS"] = "г.Тула, Вильямса ул, 38В";
                //row["TOS"] = DateTime.Now;
                //Cinfo.Rows.Add(row);
            }
            Cinfo.AcceptChanges();

            DataTable tbl = new DataTable("OINFO");
            //tbl.ReadXml("");

            tbl.Columns.Add("OSTATE", typeof(OINFO_STATE));
            tbl.Columns.Add("OID", typeof(string));
            tbl.Columns.Add("ADDRESS", typeof(string));
            tbl.Columns.Add("LAT", typeof(string));
            tbl.Columns.Add("LNG", typeof(string));
            tbl.Columns.Add("TB", typeof(DateTime));
            tbl.Columns.Add("TC", typeof(DateTime));
            tbl.Columns.Add("TR", typeof(DateTime));
            tbl.Columns.Add("TT", typeof(DateTime));
            tbl.Columns.Add("TP", typeof(DateTime));
            tbl.Columns.Add("TE", typeof(DateTime));
            tbl.Columns.Add("TOT", typeof(DateTime));
            tbl.Columns.Add("RID", typeof(string));
            tbl.Columns.Add("CID", typeof(string));
            //tbl.Columns.Add("MAPATH", typeof(string));
            tbl.Columns.Add("TD", typeof(TimeSpan));
            tbl.PrimaryKey = new DataColumn[] { tbl.Columns["OID"] };

            {
                DataRow row = tbl.NewRow();
                row["OSTATE"] = OINFO_STATE.BEGINNING;
                row["OID"] = "695354";
                row["ADDRESS"] = "г. Тула, Металлургов ул, д. 86";
                row["LAT"] = "54.196351";
                row["LNG"] = "37.677683";
                row["TB"] = DateTime.Now;
                row["TC"] = DateTime.Now;
                row["TR"] = DateTime.Now.AddMinutes(30);
                row["TT"] = DateTime.Now;
                row["TP"] = DateTime.Now;
                row["TE"] = DateTime.Now;
                row["TOT"] = row["TB"];
                row["RID"] = "ТМ18";
                row["CID"] = "К18";
                //row["MAPATH"] = "ТМ18-695354";
                row["TD"] = TimeSpan.FromMinutes(5);
                tbl.Rows.Add(row);

                row = tbl.NewRow();
                row["OSTATE"] = OINFO_STATE.BEGINNING;
                row["OID"] = "695360";
                row["ADDRESS"] = "г. Тула, Чапаева ул, д. 42";
                row["LAT"] = "54.197747";
                row["LNG"] = "37.644064";
                row["TB"] = DateTime.Now;
                row["TC"] = DateTime.Now;
                row["TR"] = DateTime.Now.AddMinutes(40);
                row["TT"] = DateTime.Now;
                row["TP"] = DateTime.Now;
                row["TE"] = DateTime.Now;
                row["TOT"] = row["TB"];
                row["RID"] = "ТМ3";
                row["CID"] = "К3";
                //row["MAPATH"] = "ТМ3-695360";
                row["TD"] = TimeSpan.FromMinutes(5);
                tbl.Rows.Add(row);

                //row = tbl.NewRow();
                //row["STATE"] = ORDER_STATE.BEGINNING;
                //row["OID"] = "695361";
                //row["ADDRESS"] = "г. Тула, Чапаева ул, д. 43";
                //row["TB"] = DateTime.Now;
                //row["TC"] = DateTime.Now;
                //row["TR"] = DateTime.Now;
                //row["TT"] = DateTime.Now;
                //row["TP"] = DateTime.Now;
                //row["TE"] = DateTime.Now;
                //row["TOT"] = DateTime.Now;
                //row["RID"] = "ТМ3";
                //row["CID"] = "К3";
                ////row["MAPATH"] = "ТМ3-695361";
                //row["TD"] = TimeSpan.FromMinutes(5);
                //tbl.Rows.Add(row);
            }

            tbl.AcceptChanges();


            tbl.WriteXml(@"testwritexml.xml");

            DataSet set = new DataSet("DLVR");
            set.Tables.Add(summary);
            set.Tables.Add(tbl);
            set.Tables.Add(Rinfo);
            set.Tables.Add(Cinfo);

            set.AcceptChanges();

            set.WriteXml(@"testwriteset.xml");
            set.WriteXmlSchema(@"testxmlschema.xml");
        }

        private static string GetTspRoute2Gis(string postData)
        {
            var request = (HttpWebRequest)WebRequest.Create(@"http://catalog.api.2gis.ru//get_tsp/?key=rueejy0019");
            //request.Headers[]=;

            //var postData = String.Concat("{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": ", lngS, ", \"y\": ", latS, " }, { \"type\": \"pedo\", \"x\": ", lngD, ", \"y\": ", latD, " } ], \"type\": \"jam\", \"output\":\"simple\"}");

            string geoInfo = String.Empty;

            var data = Encoding.ASCII.GetBytes(postData);

            request.Method = "POST";
            request.ContentType = "application/x-www-form-urlencoded";
            request.ContentLength = data.Length;

            using (var stream = request.GetRequestStream())
            {
                stream.Write(data, 0, data.Length);
            }

            var response = (HttpWebResponse)request.GetResponse();

            var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
            geoInfo = responseString.ToString();
            response.Close();
            return geoInfo;
        }

        private class GeoRouteData
        {
            public DateTime TimeMark;
            private string privGeoResponse;
            public string GeoResponse
            {
                get { return privGeoResponse; }
                set
                {
                    privGeoResponse = value;
                    JsonValue json = JsonValue.Parse(value);
                    try
                    {
                        Duration = (int)double.Parse(json["routes"][0]["duration"].ToString());
                        Distance = (int)double.Parse(json["routes"][0]["distance"].ToString());
                    }
                    catch
                    {
                        Duration = 0;
                        Distance = 0;
                    }
                }
            }
            public int Duration { get; private set; }
            public int Distance { get; private set; }
        }

        private static Dictionary<string, GeoRouteData> GeoRouteInfo = new Dictionary<string, GeoRouteData>();

        private static string GetGeoPathTotal2Gis(string latS, string lngS, string latD, string lngD)
        {
            lock (lockObj)
            {
                var request = (HttpWebRequest)WebRequest.Create(@"http://catalog.api.2gis.ru//carrouting/4.0.0/tula/?key=rueejy0019");
                //request.Headers[]=;

                var postData = String.Concat("{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": ", lngS, ", \"y\": ", latS, " }, { \"type\": \"pedo\", \"x\": ", lngD, ", \"y\": ", latD, " } ], \"type\": \"jam\", \"output\":\"simple\"}");

                GeoRouteData geoInfo = null;// String.Empty;
                if (!GeoRouteInfo.TryGetValue(postData, out geoInfo))
                {

                    var data = Encoding.ASCII.GetBytes(postData);

                    request.Method = "POST";
                    request.ContentType = "application/x-www-form-urlencoded";
                    request.ContentLength = data.Length;

                    using (var stream = request.GetRequestStream())
                    {
                        stream.Write(data, 0, data.Length);
                    }

                    var response = (HttpWebResponse)request.GetResponse();

                    var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                    geoInfo = new GeoRouteData() { TimeMark = DateTime.MinValue, GeoResponse = responseString.ToString() };
                    GeoRouteInfo.Add(postData, geoInfo);
                    response.Close();
                }
                return geoInfo.GeoResponse;
            }
        }

        private static Object lockObj = new Object();

        private static string GetGeoPathTotalYandex(DateTime timeMark, string latS, string lngS, string latD, string lngD)
        {
            lock (lockObj)
            {
                //string latlngForKey = String.Join(",", latS, lngS, latD, lngD);

                string requestString = String.Concat("https://api.routing.yandex.net/v1.0.0/distancematrix?origins=", latS, ",", lngS, "&destinations=", latD, ",", lngD, "&apikey=a1d0badd-df1d-4814-8d43-eab723c50133");

                var request = (HttpWebRequest)WebRequest.Create(requestString);
                request.Method = "GET";

                //request.Headers[]=;

                //var postData = String.Concat("{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": ", lngS, ", \"y\": ", latS, " }, { \"type\": \"pedo\", \"x\": ", lngD, ", \"y\": ", latD, " } ], \"type\": \"jam\", \"output\":\"simple\"}");

                GeoRouteData geoInfo = null;// String.Empty;
                //do
                {
                    if (!GeoRouteInfo.TryGetValue(requestString, out geoInfo))
                    {

                        //var data = Encoding.ASCII.GetBytes(postData);

                        //request.Method = "GET";
                        //request.ContentType = "application/x-www-form-urlencoded";
                        //request.ContentLength = data.Length;

                        //using (var stream = request.GetRequestStream())
                        //{
                        //    stream.Write(data, 0, data.Length);
                        //}

                        bool isRepeat = false;
                        WebResponse response = null;
                        do
                        {
                            if (isRepeat)
                            {
                                Thread.Sleep(3000);
                                isRepeat = false;
                            }

                            try
                            {
                                response = (HttpWebResponse)request.GetResponse();
                            }
                            catch (Exception e)
                            {
                                isRepeat = true;
                            }
                        }
                        while (isRepeat);


                        var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                        geoInfo = new GeoRouteData() { TimeMark = timeMark, GeoResponse = responseString.ToString() };
                        GeoRouteInfo.Add(requestString, geoInfo);
                        response.Close();
                    }
                    else
                    {
                        if ((timeMark - geoInfo.TimeMark) > TimeSpan.FromDays(2))//.FromMinutes(30))
                        {
                            bool isRepeat = false;
                            WebResponse response = null;
                            do
                            {
                                if (isRepeat)
                                {
                                    Thread.Sleep(3000);
                                    isRepeat = false;
                                }

                                try
                                {
                                    response = (HttpWebResponse)request.GetResponse();
                                }
                                catch (Exception e)
                                {
                                    isRepeat = true;
                                }
                            }
                            while (isRepeat);


                            var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                            geoInfo = new GeoRouteData() { TimeMark = timeMark, GeoResponse = responseString.ToString() };
                            GeoRouteInfo[requestString] = geoInfo;
                            response.Close();
                        }
                    }
                }
                //while ((timeMark - geoInfo.TimeMark) > TimeSpan.FromMinutes(30));

                return geoInfo.GeoResponse;
            }
        }

        private static GeoRouteData GetGeoPathTotalOSRM(DateTime timeMark, string latS, string lngS, string latD, string lngD)
        {
            lock (lockObj)
            {
                //string latlngForKey = String.Join(",", latS, lngS, latD, lngD);

                string requestString = String.Concat("http://router.project-osrm.org/route/v1/driving/", lngS, ",", latS, ";", lngD, ",", latD, "?overview=false");

                HttpWebRequest request = null;// = (HttpWebRequest)WebRequest.Create(requestString);
                //request.Method = "GET";

                //request.Headers[]=;

                //var postData = String.Concat("{ \"locale\": \"ru\", \"point_a_name\": \"point A\", \"point_b_name\": \"point B\", \"points\": [ { \"type\": \"pedo\", \"x\": ", lngS, ", \"y\": ", latS, " }, { \"type\": \"pedo\", \"x\": ", lngD, ", \"y\": ", latD, " } ], \"type\": \"jam\", \"output\":\"simple\"}");

                GeoRouteData geoInfo = null;// String.Empty;
                //do
                {
                    if (!GeoRouteInfo.TryGetValue(requestString, out geoInfo))
                    {

                        //var data = Encoding.ASCII.GetBytes(postData);

                        //request.Method = "GET";
                        //request.ContentType = "application/x-www-form-urlencoded";
                        //request.ContentLength = data.Length;

                        //using (var stream = request.GetRequestStream())
                        //{
                        //    stream.Write(data, 0, data.Length);
                        //}

                        request = (HttpWebRequest)WebRequest.Create(requestString);
                        request.Method = "GET";
                        bool isRepeat = false;
                        WebResponse response = null;
                        do
                        {
                            if (isRepeat)
                            {
                                Thread.Sleep(6000);
                                isRepeat = false;
                            }

                            try
                            {
                                response = (HttpWebResponse)request.GetResponse();
                            }
                            catch (Exception e)
                            {
                                isRepeat = true;
                                request = (HttpWebRequest)WebRequest.Create(requestString);
                                request.Method = "GET";
                            }
                        }
                        while (isRepeat);


                        var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                        geoInfo = new GeoRouteData() { TimeMark = timeMark, GeoResponse = responseString.ToString() };
                        GeoRouteInfo.Add(requestString, geoInfo);
                        response.Close();
                    }
                    else
                    {
                        if ((timeMark - geoInfo.TimeMark) > TimeSpan.FromDays(2))//.FromMinutes(30))
                        {
                            request = (HttpWebRequest)WebRequest.Create(requestString);
                            request.Method = "GET";
                            bool isRepeat = false;
                            WebResponse response = null;
                            do
                            {
                                if (isRepeat)
                                {
                                    Thread.Sleep(3000);
                                    isRepeat = false;
                                }

                                try
                                {
                                    response = (HttpWebResponse)request.GetResponse();
                                }
                                catch (Exception e)
                                {
                                    isRepeat = true;
                                    request = (HttpWebRequest)WebRequest.Create(requestString);
                                    request.Method = "GET";
                                }
                            }
                            while (isRepeat);


                            var responseString = new StreamReader(response.GetResponseStream()).ReadToEnd();
                            geoInfo = new GeoRouteData() { TimeMark = timeMark, GeoResponse = responseString.ToString() };
                            GeoRouteInfo[requestString] = geoInfo;
                            response.Close();
                        }
                    }
                }
                //while ((timeMark - geoInfo.TimeMark) > TimeSpan.FromMinutes(30));

                return geoInfo;//.GeoResponse;
            }
        }


        public static void TspDoTest(string[] args)
        {
            //create an initial tour out of nearest neighbors
            var stops = Enumerable.Range(1, 50)
                                  .Select(i => new TspStop(new TspCity(i)))
                                  .NearestNeighbors()
                                  .ToList();

            //create next pointers between them
            stops.Connect(true);

            //wrap in a tour object
            TspTour startingTour = new TspTour(stops);

            //the actual algorithm
            while (true)
            {
                Console.WriteLine(startingTour);
                var newTour = startingTour.GenerateMutations()
                                          .MinBy(tour => tour.Cost());
                if (newTour.Cost() < startingTour.Cost()) startingTour = newTour;
                else break;
            }

            Console.ReadLine();
        }

        public static TspTour TspDoTsp(List<DataRow> listPnts, bool isLoop, bool isLog = false)
        {
            //create an initial tour out of nearest neighbors
            var stops = Enumerable.Range(1, listPnts.Count)
                                  .Select(i => new TspStop(new TspCity(listPnts[i - 1])))
                                  .NearestNeighbors()
                                  .ToList();

            //create next pointers between them
            stops.Connect(isLoop);

            //wrap in a tour object
            TspTour startingTour = new TspTour(stops);

            //the actual algorithm
            if (listPnts.Count > 2)
                while (true)
                {
                    if (isLog)
                    {
                        appLog.WriteLine(startingTour);
                        appLog.Flush();
                    }
                    var newTour = startingTour.GenerateMutations()
                                              .MinBy(tour => tour.Cost());
                    if (newTour.Cost() < startingTour.Cost()) startingTour = newTour;
                    else break;
                }

            //Console.ReadLine();
            return startingTour;
        }

        public class TspCity
        {
            private static Random rand = new Random();


            public TspCity(int cityName)
            {
                X = rand.NextDouble() * 100;
                Y = rand.NextDouble() * 100;
                CityName = cityName;
            }

            public TspCity(DataRow cityInfo)
            {
                CityInfo = cityInfo;
                //X = rand.NextDouble() * 100;
                //Y = rand.NextDouble() * 100;
                //CityName = cityName;
                LAT = cityInfo["LAT"].ToString();
                LNG = cityInfo["LNG"].ToString();

                int i = cityInfo.Table.Columns.IndexOf("OSTATE");
                if (i > -1)
                {
                    CityID = cityInfo["OID"].ToString();
                }
                else
                {
                    i = cityInfo.Table.Columns.IndexOf("RID");
                    if (i > -1)
                    {
                        CityID = cityInfo["RID"].ToString();
                    }
                    i = cityInfo.Table.Columns.IndexOf("CID");
                    if (i > -1)
                    {
                        CityID = cityInfo["CID"].ToString();
                    }
                }
            }

            public double X { get; private set; }

            public double Y { get; private set; }

            public int CityName { get; private set; }

            public string CityID { get; private set; }
            public string LAT { get; private set; }
            public string LNG { get; private set; }
            public DataRow CityInfo { get; private set; }
        }


        public class TspStop
        {
            public TspStop(TspCity city)
            {
                City = city;
            }


            public TspStop Next { get; set; }

            public TspCity City { get; set; }


            public TspStop Clone()
            {
                return new TspStop(City);
            }


            public static double Distance(TspStop first, TspStop other)
            {
                if ((first == null) || (other == null)) return 0;

                //string r = GetGeoPathTotal2Gis(first.City.LAT, first.City.LNG, other.City.LAT, other.City.LNG);
                //string r = GetGeoPathTotalYandex(DateTime.MinValue, first.City.LAT, first.City.LNG, other.City.LAT, other.City.LNG);
                GeoRouteData r = GetGeoPathTotalOSRM(DateTime.MinValue, first.City.LAT, first.City.LNG, other.City.LAT, other.City.LNG);
                //JsonValue json = JsonValue.Parse(r);

                //double distanceBetween = MainClass.GetGeoPathDistance(first.City.LAT, first.City.LNG, other.City.LAT, other.City.LNG);

                //int ddd = int.Parse(json["result"][0]["duration"].ToString());
                //int lll = int.Parse(json["result"][0]["length"].ToString());
                //int ddd = (int)distanceBetween / 14; //int.Parse(json["rows"][0]["elements"][0]["duration"]["value"].ToString());
                //int lll = (int)distanceBetween; //int.Parse(json["rows"][0]["elements"][0]["distance"]["value"].ToString());
                //int ddd = (int)double.Parse(json["routes"][0]["duration"].ToString());
                //int lll = (int)double.Parse(json["routes"][0]["distance"].ToString());

                return r.Distance;

                //return Math.Sqrt(
                //    Math.Pow(first.City.X - other.City.X, 2) +
                //    Math.Pow(first.City.Y - other.City.Y, 2));
            }


            //list of nodes, including this one, that we can get to
            public IEnumerable<TspStop> CanGetTo()
            {
                var current = this;
                while (true)
                {
                    yield return current;
                    current = current.Next;
                    if ((current == this) || (current == null)) break;
                }
            }


            public override bool Equals(object obj)
            {
                return City == ((TspStop)obj).City;
            }


            public override int GetHashCode()
            {
                return City.GetHashCode();
            }


            public override string ToString()
            {
                //return City.CityName.ToString();
                return City.CityID;
            }
        }

        private static double GetGeoPathDistance(string lAT1, string lNG1, string lAT2, string lNG2)
        {
            var sCoord = new GeoCoordinate(double.Parse(lAT1), double.Parse(lNG1));
            var eCoord = new GeoCoordinate(double.Parse(lAT2), double.Parse(lNG2));

            return sCoord.GetDistanceTo(eCoord);
        }

        //private static double GetOSRMDistance(string osrmResult)
        //{

        //    return sCoord.GetDistanceTo(eCoord);
        //}


        public class TspTour
        {
            public TspTour(IEnumerable<TspStop> stops)
            {
                Anchor = stops.First();
            }


            //the set of tours we can make with 2-opt out of this one
            public IEnumerable<TspTour> GenerateMutations()
            {
                for (TspStop stop = Anchor; (stop.Next != Anchor) && (stop.Next != null); stop = stop.Next)
                {
                    //skip the next one, since you can't swap with that
                    TspStop current = stop.Next.Next;
                    while ((current != Anchor) && (current != null))
                    {
                        yield return CloneWithSwap(stop.City, current.City);
                        current = current.Next;
                    }
                }
            }


            public TspStop Anchor { get; set; }


            public TspTour CloneWithSwap(TspCity firstCity, TspCity secondCity)
            {
                TspStop firstFrom = null, secondFrom = null;
                var stops = UnconnectedClones();
                stops.Connect(true);

                foreach (TspStop stop in stops)
                {
                    if (stop.City == firstCity) firstFrom = stop;

                    if (stop.City == secondCity) secondFrom = stop;
                }

                //the swap part
                var firstTo = firstFrom.Next;
                var secondTo = secondFrom.Next;

                //reverse all of the links between the swaps
                firstTo.CanGetTo()
                       .TakeWhile(stop => stop != secondTo)
                       .Reverse()
                       .Connect(false);

                firstTo.Next = secondTo;
                firstFrom.Next = secondFrom;

                var tour = new TspTour(stops);
                return tour;
            }


            public IList<TspStop> UnconnectedClones()
            {
                return Cycle().Select(stop => stop.Clone()).ToList();
            }


            public double Cost()
            {
                return Cycle().Aggregate(
                    0.0,
                    (sum, stop) =>
                    sum + TspStop.Distance(stop, stop.Next));
            }


            public IEnumerable<TspStop> Cycle()
            {
                return Anchor.CanGetTo();
            }


            public override string ToString()
            {
                string path = String.Join(
                    "->",
                    Cycle().Select(stop => stop == null ? String.Empty : stop.ToString()).ToArray());
                return String.Format("Cost: {0}, Path:{1}", Cost(), path);
            }

        }


        //take an ordered list of nodes and set their next properties
        public static void Connect(this IEnumerable<TspStop> stops, bool loop)
        {
            TspStop prev = null, first = null;
            foreach (var stop in stops)
            {
                if (first == null) first = stop;
                if (prev != null) prev.Next = stop;
                prev = stop;
            }

            if (loop)
            {
                prev.Next = first;
            }
        }


        //T with the smallest func(T)
        public static T MinBy<T, TComparable>(
            this IEnumerable<T> xs,
            Func<T, TComparable> func)
            where TComparable : IComparable<TComparable>
        {
            return xs.DefaultIfEmpty().Aggregate(
                (maxSoFar, elem) =>
                func(elem).CompareTo(func(maxSoFar)) > 0 ? maxSoFar : elem);
        }


        //return an ordered nearest neighbor set
        public static IEnumerable<TspStop> NearestNeighbors(this IEnumerable<TspStop> stops)
        {
            var stopsLeft = stops.ToList();
            for (var stop = stopsLeft.First();
                 stop != null;
                 stop = stopsLeft.MinBy(s => TspStop.Distance(stop, s)))
            {
                stopsLeft.Remove(stop);
                yield return stop;
            }
        }
    }
}

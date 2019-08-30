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

            OleDbConnection dbConn = new OleDbConnection(@"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\Users\home91\Downloads\fias_dbf;Extended Properties=dBASE IV;");

            dbConn.Open();

            foreach(string argv in args)
            {
                OleDbCommand dbCmd = new OleDbCommand($"select * from {argv}", dbConn);

                OleDbDataReader dbRdr = dbCmd.ExecuteReader();

                while (dbRdr.Read())
                {
                    //Console.WriteLine(dbRdr[0].ToString());

                }
                dbRdr.Close();
            }

            dbConn.Close();
        }
    }
}

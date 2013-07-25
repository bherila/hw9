using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Windows.Forms;
using System.Net;
using System.Threading.Tasks;
using System.Globalization;

namespace hw9
{
    static class Program
    {

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main() {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            WebClient wc = new WebClient();

            /* Load the 2006 S&P 100 from the given text file */
            const string sp100_2006_url = "http://www.econ.brown.edu/students/Tomislav_Ladika/teaching_files/sp100.txt";
            
            var sp100_2006 = from line in downloadLines(sp100_2006_url).Skip(1)
                             let split = line.Split(',', '\t')
                             select new { gvkey = int.Parse(split[0]), ticker = split[1] };

            /* URL to yahoo chart API */
            const string YAHOO = @"http://ichart.finance.yahoo.com/table.csv?s={0}&a=03&b=12&c=1900&d={1:MM}&e={1:dd}&f={1:yyyy}&g=d&ignore=.csv";

            var outputStream = File.CreateText("out.csv");

            List<string> delisted_tickers = new List<string>();
            Parallel.ForEach(sp100_2006, sp => {
                try {
                    Console.WriteLine("Downloading " + sp.ticker);
                    var ydata = downloadLines(String.Format(YAHOO, sp.ticker, DateTime.Now));
                    Console.WriteLine(" ==> " + ydata.Count);

                    /* Parse all the lines */
                    var data = (from l in ydata.Skip(1)
                                let cols = l.Split(',')
                                let result = new RecordFormat() {
                                    Ticker = sp.ticker,
                                    Date = DateTime.Parse(cols[0]),
                                    Open = double.Parse(cols[1]),
                                    Close = double.Parse(cols[4]),
                                    AdjClose = double.Parse(cols[6])
                                }
                                select result);

                    lock (outputStream) {
                        dumpData(outputStream, data);
                    }
                    
                }
                catch (WebException wx) {
                    lock (delisted_tickers) { delisted_tickers.Add(sp.gvkey.ToString()); }
                    Console.WriteLine(" ==> DELISTED!");
                }
            });

            // Faster to do this all at once vs. a zillion queries for some reason. 
            // select gvkey, datadate, prccd, prcod, ajexdi into outfile '/home/bherila/sqlresult.csv' fields terminated by ',' lines terminated by '\n' from sec_dprc where gvkey in (1478,4739,16243,30128,9459,157858,15208);
            var gvkey_lookup = sp100_2006.ToDictionary(a=> a.gvkey, b=> b.ticker);
            var delisted_data = from line in splitCleanLines(File.ReadAllText("sqlresult.csv"))
                                let split = line.Split(',')
                                //select new { gvkey = int.Parse(split[0]), datadate = split[1], adjclose = dou.Parse(split[2]) / double.Parse(split[4]) };
                                select new RecordFormat() {
                                    Ticker = gvkey_lookup[int.Parse(split[0])],
                                    AdjClose = double.Parse(split[2] == @"\N" ? "-1" : split[2]) / double.Parse(split[4]),
                                    Close = double.Parse(split[2] == @"\N" ? "-1" : split[2]),
                                    Date = DateTime.ParseExact(split[1], "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                    Open = double.Parse(split[3] == @"\N" ? "-1" : split[3])
                                };
            dumpData(outputStream, delisted_data);

            outputStream.Close();
            
        }



        struct RecordFormat
        {
            public string Ticker;
            public DateTime Date;
            public double Open, Close, AdjClose;
        }

        /// <summary>
        /// Pasted from previous assignment with slight modification.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="symbol"></param>
        /// <param name="data"></param>
        static void dumpData(StreamWriter file, IEnumerable<RecordFormat> data) {
            var prev = data.First();
            double RoR;
            double cRoR = 1;

            file.WriteLine(String.Join("\t", "ticker", "date", "price", "return", "cumratereturn"));
            foreach (var dataItem in data) {
                var cur = dataItem;

                /* compute rate of return and cumulative rate of return */
                RoR = (cur.AdjClose - prev.AdjClose) / prev.AdjClose;
                cRoR *= 1.0 + RoR;

                /* write result line to file */
                file.WriteLine(String.Join(
                    "\t", dataItem.Ticker,
                    dataItem.Date.ToString("yyyy-MM-dd"),
                    dataItem.AdjClose.ToString("00.0000"),
                    RoR, (cRoR - 1)));
                prev = cur;
            }
        }

        static List<string> downloadLines(string url) {
            string data;
            string file = url.GetHashCode().ToString() + ".cache";
            if (!File.Exists(file)) {
                using (WebClient wc = new WebClient()) {
                    data = wc.DownloadString(url);
                    File.WriteAllText(file, data);
                }
            }
            else
                data = File.ReadAllText(file);
            return splitCleanLines(data);
        }

        static List<string> splitCleanLines(string input) {
            List<string> lines = new List<string>();
            using (StringReader sr = new StringReader(input)) {
                string buf;
                while ((buf = sr.ReadLine()) != null)
                    lines.Add(buf.Trim());
            }
            return lines;
        }


    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.IO;
using Google.Apis.Sheets.v4;
using Google.Apis.Services;
using Google.Apis.Sheets.v4.Data;
using System.Security.Cryptography.X509Certificates;
using Google.Apis.Auth.OAuth2;
using System.Configuration;

namespace konvertor
{

    class DataConvertor
    {

        public static string[] requiredDbfFiles = { "DBFODB.DBF", "DBFDOD.DBF", "DBFPOHL.DBF" };

        private readonly string[] Scopes = { SheetsService.Scope.Spreadsheets };
        
        private OdbcConnection dbf;
        private SqlConnection sql;
        private string dbfPath;
        private Boolean isTest;
        private StreamWriter logStream;
        private readonly string appName = "ENP";
        private string sheetId;
        private string sheetName;


        public DataConvertor(OdbcConnection dbf, SqlConnection sql, StreamWriter logStream, string sheetId, string sheetName, Boolean isTest = false)
        {
            this.dbf = dbf;
            this.sql = sql;
            this.isTest = isTest;
            this.logStream = logStream;
            this.dbfPath = ConfigurationManager.AppSettings["DbfPath"];
            this.sheetId = sheetId;
            this.sheetName = sheetName;

            if (dbf.State != System.Data.ConnectionState.Open)
            {
                throw new ArgumentException("Není otevřeno DBF spojení.");
            }

            if (sql.State != System.Data.ConnectionState.Open)
            {
                throw new ArgumentException("Není otevřeno SQL spojení.");
            }
        }

        private string columnPartner(string type)
        {
            return type == "ODB" ? "P2" : "P1";
        }

        private bool partnerExists(string id, string type)
        {
            using (var command = sql.CreateCommand())
            {
                command.CommandText = String.Format("SELECT 1 as result FROM AD WHERE Smlouva=@id AND {0}=1", columnPartner(type));
                command.Parameters.AddWithValue("@id", id);
                using (var reader = command.ExecuteReader())
                {
                    return reader.HasRows;
                }
            }
        }

        private string commaToDotInDecimalValue(string value)
        {
            // if is decimal number, replace comma with dot
            string[] vals = value.Split(',');
            if (vals.Length == 2)
            {
                int i;
                if (int.TryParse(vals[0], out i) && int.TryParse(vals[1], out i))
                {
                    return value.Replace(",", ".");
                }
            }
            return value;
        }

        private void commaToDotInDecimalValues(string[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = commaToDotInDecimalValue(values[i]);
            }
        }

        private string escapeValue(string value)
        {
            value = value.Trim();
            return value.Replace("'", "''");
        }

        private void escapeValues(string[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = escapeValue(values[i]);
            }
        }

        private void updatePartner(SqlCommand sqlCmd, string[] columns, string[] values, string type)
        {
            var output = new StringBuilder();
            escapeValues(values);
            commaToDotInDecimalValues(values);
            for (int i = 0; i < columns.Length; i++)
            {
                output.AppendFormat("{0}='{1}',", columns[i], values[i]);
            }
            output.Remove(output.Length - 1, 1);
            sqlCmd.CommandText = String.Format("UPDATE AD SET {0} WHERE Smlouva=@id AND {1}=1", output, columnPartner(type));
            sqlCmd.Parameters.AddWithValue("@id", values[0]);
            sqlCmd.ExecuteNonQuery();
        }

        private void insertPartner(SqlCommand sqlCmd, string[] columns, string[] values)
        {
            escapeValues(values);
            commaToDotInDecimalValues(values);
            sqlCmd.CommandText = String.Format("INSERT INTO AD ({0}) VALUES ('{1}')",
                String.Join(", ", columns), String.Join("', '", values));
            sqlCmd.ExecuteNonQuery();
        }

        private string getCostumersQuery()
        {
            return String.Format("SELECT {0} {1} FROM {2} ORDER BY CisloOdber",
                isTest ? "TOP 20" : "",
                "CisloOdber as Smlouva" +
                ",IIF(NazevOdb2 <> '', NazevOdber+' '+NazevOdb2, NazevOdber) AS Firma" +
                ",Ulice" +
                ",Mesto AS Obec" +
                ",PSC" +
                ",ICO" +
                ",DIC1 + DIC2 AS DIC" +
                ",Zastoupeny AS Jmeno" +
                ",Telefon AS Tel" +
                ",Mail AS Email" +
                ",NazevPrije as Firma2" +
                ",UlicePrije as Ulice2" +
                ",MestoPrije AS Obec2" +
                ",PSCPrijemc AS PSC2" +
                ",KodOdb as CisloZAK" +
                ",Splatnost AS ADSplat" +
                ",IIF(NahradniPl = 1, 1, 0) AS RefStr" +
                ",IIF(RabatO <> 0, RabatO * (-1), NULL) AS CenyIDS" +
                ",IIF(KodSumFa <> '', 2, IIF(SumFa = 1, 1, 0)) AS RefCin" +
                ",KupniSmlou AS P3" +
                ",1 AS P2", // ODB
                dbfPath + "\\DBFODB.DBF");
        }

        private void convertCostumers()
        {
            Log("Konvertuji odběratele.");
            using (var odbcCmd = dbf.CreateCommand())
            {
                odbcCmd.CommandText = getCostumersQuery();
                int[] counters = { 0, 0, 0 };

                using (OdbcDataReader reader = odbcCmd.ExecuteReader())
                {
                    int fieldCount = reader.FieldCount;
                    string[] columns = new string[fieldCount];
                    string[] values = new string[fieldCount];
                    while (reader.Read())
                    {
                        for (int i = 0; i < fieldCount; i++)
                        {
                            columns[i] = reader.GetName(i);
                            values[i] = reader.GetValue(i).ToString();
                        }
                        counters[2]++;

                        // remove leading zeros of identifier
                        values[0] = values[0].TrimStart('0');
                        var id = values[0];

                        if (partnerExists(id, "ODB"))
                        {
                            try
                            {
                                using (var sqlCmd = sql.CreateCommand())
                                {
                                    updatePartner(sqlCmd, columns, values, "ODB");
                                    counters[0]++;
                                }
                            }
                            catch (SqlException e)
                            {
                                Log(String.Format("Nepodařilo se aktualizovat odběratele č. {0}. {1}", id, e.Message));
                                Log("Pokračuji...");
                            }
                        }
                        else {
                            try
                            {
                                using (var sqlCmd = sql.CreateCommand())
                                {
                                    insertPartner(sqlCmd, columns, values);
                                    counters[1]++;
                                }
                            }
                            catch (SqlException e)
                            {
                                Log(String.Format("Nepodařilo se vložit odběratele č. {0}. {1}", id, e.Message));
                                Log("Pokračuji...");
                            }
                        }
                    }
                }
                Log(String.Format("Zpracováno {0} odběratelů. Vloženo: {1}, aktualizováno: {2}, chybných: {3}",
                    counters[2], counters[1], counters[0], counters[2] - counters[1] - counters[0]));

            }
        }

        private string getSuppliersQuery()
        {
            return String.Format("SELECT {0} {1} FROM {2} ORDER BY CisloDodav",
                isTest ? "TOP 20" : "",
                "CisloDodav AS Smlouva" +
                ",IIF(NazevDod2 <> '', NazevDodav+' '+NazevDod2, NazevDodav) AS Firma" +
                ",Ulice" +
                ",Mesto AS Obec" +
                ",PSC" +
                ",ICO" +
                ",DIC1 + DIC2 AS DIC" +
                ",Zastoupeny AS Jmeno2" +
                ",Telefon AS Tel" +
                ",Fax" +
                ",Mail AS Email" +
                ",1 AS P1", //DOD
                dbfPath + "\\DBFDOD.DBF");
        }

        private void convertSuppliers()
        {
            Log("Konvertuji dodavatele.");
            using (var odbcCmd = dbf.CreateCommand())
            {
                odbcCmd.CommandText = getSuppliersQuery();
                int[] counters = { 0, 0, 0 };

                using (OdbcDataReader reader = odbcCmd.ExecuteReader())
                {
                    int fieldCount = reader.FieldCount;
                    string[] columns = new string[fieldCount];
                    string[] values = new string[fieldCount];
                    while (reader.Read())
                    {
                        for (int i = 0; i < fieldCount; i++)
                        {
                            columns[i] = reader.GetName(i);
                            values[i] = reader.GetValue(i).ToString();
                        }
                        counters[2]++;

                        // remove leading zeros of identifier
                        values[0] = values[0].TrimStart('0');
                        var id = values[0];

                        if (partnerExists(id, "DOD"))
                        {
                            try
                            {
                                using (var sqlCmd = sql.CreateCommand())
                                {
                                    updatePartner(sqlCmd, columns, values, "DOD");
                                    counters[0]++;
                                }
                            }
                            catch (SqlException e)
                            {
                                Log(String.Format("Nepodařilo se aktualizovat dodavatele č. {0}. {1}", id, e.Message));
                                Log("Pokračuji...");
                            }
                        }
                        else {
                            try
                            {
                                using (var sqlCmd = sql.CreateCommand())
                                {
                                    insertPartner(sqlCmd, columns, values);
                                    counters[1]++;
                                }
                            }
                            catch (SqlException e)
                            {
                                Log(String.Format("Nepodařilo se vložit dodavatele č. {0}. {1}", id, e.Message));
                                Log("Pokračuji...");
                            }
                        }
                    }
                }
                Log(String.Format("Zpracováno {0} dodavatelů. Vloženo: {1}, aktualizováno: {2}, chybných: {3}",
                    counters[2], counters[1], counters[0], counters[2] - counters[1] - counters[0]));
            }
        }

        private string getInvoicesQuery()
        {
            return String.Format("SELECT {0} {1} FROM {2} ORDER BY CisloVydej",
                isTest ? "TOP 20" : "",
                "CisloVydej AS _id" +
                ",CisloOdber as _idOdber" +
                ",DatumVydej AS _datum" +
                ",Sdani AS _sdani" +
                ",Splatnost AS _splatnost" +
                ",IIF(NahradniPl <> 0, 1, 0) AS _nahradni" +
                ",1 AS RelTpFak" +
                ",17 AS RelPK" +
                ",480 AS RelCR" +
                ",CisloObjed AS CisloObj" +
                ",Kc0" +
                ",Kc1" +
                ",Kc2" +
                ",0 AS Kc3" +
                ",KcDPH1" +
                ",KcDPH2" +
                ",0 AS KcDPH3" +
                ",0 AS KcU" +
                ",0 AS KcP" +
                ",0 as KcZaloha" +
                ",0 AS KcKRZaloha" +
                ",KcZaokr" +
                ",KcCelkem" +
                ",KcCelkem AS KcLikv" +
                ",IIF(FormaUhrad = 'P', 1, IIF(FormaUhrad = 'H', 2, IIF(FormaUhrad = 'D', 4, 1))) AS RelForUh" +
                ",IIF(FormaUhrad = 'P', 2, IIF(FormaUhrad = 'H', 1, IIF(FormaUhrad = 'D', 1, 2))) AS RefUcet" +
                ",1 AS ZaokrFV" +
                ",IIF(TiskSum <> '0', 1, 0) AS RefCin" +
                ",KodOdb AS CisloZAK" +
                ",0 AS RelZpVypDPH",
                dbfPath + "\\DBFPOHL.DBF");
        }

        private int getIdFromSql(string table, string column, string value)
        {
            using (var sqlCmd = sql.CreateCommand())
            {
                sqlCmd.CommandText = String.Format("SELECT ID FROM {0} WHERE {1}=@value", table, column);
                sqlCmd.Parameters.AddWithValue("@value", value);
                using (var reader = sqlCmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return reader.GetInt32(0);
                    }
                    throw new MissingFieldException(String.Format(
                        "Nebyl nalezen záznam. Tabulka: {0}, sloupec: {1}, value: {2}.",
                        table, column, value));
                }
            }
        }

        private bool invoiceExists(string id)
        {
            using (var command = sql.CreateCommand())
            {
                command.CommandText = "SELECT 1 as result FROM FA WHERE Cislo=@id";
                command.Parameters.AddWithValue("@id", id);
                using (var reader = command.ExecuteReader())
                {
                    return reader.HasRows;
                }
            }
        }

        private void convertInvoices()
        {
            Log("Konvertuji pohledávky.");
            int UD = getIdFromSql("sDPH", "IDS", "UD");
            int UDdodEU = getIdFromSql("sDPH", "IDS", "UD");
            int NP = getIdFromSql("sSTR", "IDS", "NP");

            using (var odbcCmd = dbf.CreateCommand())
            {
                odbcCmd.CommandText = getInvoicesQuery();
                int[] counters = { 0, 0, 0 };
                using (var reader = odbcCmd.ExecuteReader())
                {
                    int fieldCount = reader.FieldCount;
                    Dictionary<string, string> data = null;
                    while (reader.Read())
                    {
                        counters[2]++;
                        data = new Dictionary<string, string>();
                        for (int i = 0; i < fieldCount; i++)
                        {
                            data.Add(reader.GetName(i), reader.GetValue(i).ToString());
                        }

                        int id = Int32.Parse(data["_id"]);
                        int refAd = Int32.Parse(data["_idOdber"]);
                        DateTime date = DateTime.Parse(data["_datum"]);
                        id += (date.Year % 100) * 10000000 + 100000;

                        string sdani = data["_sdani"];
                        bool nahradni = data["_nahradni"] == "1";
                        DateTime payupDate = date.AddDays(Int32.Parse(data["_splatnost"]));
                        string dateStr = date.ToString("yyyy-MM-dd");
                        string payupDateStr = payupDate.ToString("yyyy-MM-dd");

                        data.Remove("_id");
                        data.Remove("_idOdber");
                        data.Remove("_sdani");
                        data.Remove("_datum");
                        data.Remove("_splatnost");
                        data.Remove("_nahradni");
                        data.Add("Cislo", id.ToString());
                        data.Add("RefAd", refAd.ToString());
                        data.Add("VarSym", id.ToString());
                        data.Add("RefStr", nahradni ? NP.ToString() : "0");
                        data.Add("RelTpDPH", sdani == "V" ? UDdodEU.ToString() : UD.ToString());
                        data.Add("Datum", dateStr);
                        data.Add("DatUCP", dateStr);
                        data.Add("DatZdPln", dateStr);
                        data.Add("DatSplat", payupDateStr);
                        data.Add("SText", data["RefCin"] == "1" ? "Fakturujeme Vám za zboží dle dodacího listu" : "Fakturujeme Vám za zboží dle vaší objednávky");

                        if (invoiceExists(id.ToString()))
                        {
                            // update wrong converted data
                            //using (var command = sql.CreateCommand())
                            //{
                            //    command.CommandText = "UPDATE FA SET DatSplat=@DatSplat WHERE Cislo=@id";
                            //    command.Parameters.AddWithValue("@id", id);
                            //    command.Parameters.AddWithValue("@DatSplat", payupDateStr);
                            //    command.ExecuteNonQuery();

                            //}
                            counters[0]++;
                            continue;
                        }

                        string cmd = "";
                        try
                        {
                            using (var sqlCmd = sql.CreateCommand())
                            {
                                string[] values = data.Values.ToArray();
                                escapeValues(values);
                                commaToDotInDecimalValues(values);
                                cmd = sqlCmd.CommandText = String.Format("INSERT INTO FA ({0}) VALUES ('{1}')",
                                    String.Join(", ", data.Keys), String.Join("', '", values));
                                sqlCmd.ExecuteNonQuery();
                            }

                            using (var sqlCmd = sql.CreateCommand())
                            {
                                cmd = sqlCmd.CommandText = "UPDATE FA" +
                                    " SET FA.RefAd=AD.Id" +
                                    " FROM FA INNER JOIN AD on FA.RefAd=AD.Smlouva" +
                                    " WHERE AD.P2=1 AND FA.Cislo=@id"; // P2 priznak znamena, ze je to odberatel
                                sqlCmd.Parameters.AddWithValue("@id", id);
                                sqlCmd.ExecuteNonQuery();
                            }

                            using (var sqlCmd = sql.CreateCommand())
                            {
                                cmd = sqlCmd.CommandText = "UPDATE FA" +
                                    " SET FA.Firma=AD.Firma, FA.Ulice=AD.Ulice, FA.Obec=AD.Obec, FA.PSC=AD.PSC, FA.ICO=AD.ICO, FA.DIC=AD.DIC" +
                                    " FROM FA INNER JOIN AD on FA.RefAd=AD.Id" +
                                    " WHERE FA.Cislo=@id";
                                sqlCmd.Parameters.AddWithValue("@id", id);
                                sqlCmd.ExecuteNonQuery();
                            }

                            using (var sqlCmd = sql.CreateCommand())
                            {
                                cmd = sqlCmd.CommandText = "INSERT INTO pUD" +
                                    " (sel, tpud, reldruhud, datum, datzdpln, cislo, orderfld, reludag, relagid, sText, umd, ud, kc, parsym, firma, RefAD, ParICO)" +
                                    " SELECT 0, 0, 0, datum, datzdpln, cislo, 0, 2, ID, 'Tržby z prodeje zboží', 311000, 604000, kc0 + kc1 + kc2 + kczaokr, varsym, firma, RefAD, ICO" +
                                    " FROM FA" +
                                    " WHERE FA.Cislo=@id";
                                sqlCmd.Parameters.AddWithValue("@id", id);
                                sqlCmd.ExecuteNonQuery();
                            }

                            using (var sqlCmd = sql.CreateCommand())
                            {
                                cmd = sqlCmd.CommandText = "INSERT INTO pUD" +
                                    " (sel, tpud, reldruhud, datum, datzdpln, cislo, orderfld, reludag, relagid, sText, umd, ud, kc, parsym, firma, RefAD, ParICO)" +
                                    " SELECT 0, 0, 0, datum, datzdpln, cislo, 1, 2, ID, 'DPH 15% - Tržby z prodeje zboží', 311000, 343014, kcdph1, varsym, firma, RefAD, ICO" +
                                    " FROM FA" +
                                    " WHERE FA.Cislo=@id AND KcDPH1 <> 0";
                                sqlCmd.Parameters.AddWithValue("@id", id);
                                sqlCmd.ExecuteNonQuery();
                            }

                            using (var sqlCmd = sql.CreateCommand())
                            {
                                cmd = sqlCmd.CommandText = "INSERT INTO pUD" +
                                    " (sel, tpud, reldruhud, datum, datzdpln, cislo, orderfld, reludag, relagid, sText, umd, ud, kc, parsym, firma, RefAD, ParICO)" +
                                    " SELECT 0, 0, 0, datum, datzdpln, cislo, 1, 2, ID, 'DPH 21% - Tržby z prodeje zboží', 311000, 343020, kcdph2, varsym, firma, RefAD, ICO" +
                                    " FROM FA" +
                                    " WHERE FA.Cislo=@id AND KcDPH2 <> 0";
                                sqlCmd.Parameters.AddWithValue("@id", id);
                                sqlCmd.ExecuteNonQuery();
                            }
                            counters[1]++;

                        }
                        catch (SqlException e)
                        {
                            if (e.Message.Contains("IX_FA_Cislo"))
                            {
                                // IX_FA_Cislo is UNIQUE CONSTRAINT - FA exists
                                counters[0]++;
                                continue;
                            }

                            Log(String.Format("Nepodařilo se vložit pohledávku č. {0}. {1}", id, e.Message));
                            Log(cmd);
                            Log(e.ToString());
                            Log("Pokračuji...");
                        }
                    }
                }

                using (var sqlCmd = sql.CreateCommand())
                {
                    /**
                        updates emails in the invoices where partner has NP in shipping address as Utvar2
                        if partner has in Utvar2 value NP then its email (ADdodaci.Email2) is confirmed as valid email for ENP
                        if email in the invoice is filled, export to ENP is possible
                    */
                    sqlCmd.CommandText = "update FAs" +
                        " set FAs.Email=ADdodaci.Email2" +
                        " from FA FAs" +
                        " join AD on AD.ID=FAs.RefAD" +
                        " join sSTR on FAs.RefStr=sSTR.ID" +
                        " join ADdodaci on ADdodaci.RefAg=AD.ID and ADdodaci.Utvar2='NP'" +
                        " where sSTR.IDS='NP'";
                    sqlCmd.ExecuteNonQuery();
                }
                 
                Log(String.Format("Zpracováno {0} pohledávek. Vloženo: {1}, přeskočeno: {2}, chybných: {3}",
                    counters[2], counters[1], counters[0], counters[2] - counters[1] - counters[0]));
            }
        }

        private string getUnpaidInvoicesQuery()
        {
            return String.Format("SELECT {0} {1} FROM {2} WHERE {3}",
                isTest ? "TOP 20" : "",
                "REPLICATE('0',5-LEN(AD.Smlouva))+AD.Smlouva as CisloOdber" +
                ", FA.Firma as NazevOdber" +
                ", right(FA.Cislo,5)+'/'+format(FA.Datum,'yy') as CisloFakRu" +
                ", FORMAT(FA.Datum, 'yyyy-MM-dd') as DatFa" +
                ", FORMAT(FA.DatSplat, 'yyyy-MM-dd') as DatSpl" +
                ", FA.KcCelkem as Celkem" +
                ", FA.KcLikv as Dluh",
                "FA INNER JOIN AD ON FA.RefAD=AD.Id",
                "FA.KcLikv<>0 AND FA.RelTPFak=1");
        }

        private void convertUnpaidInvoices()
        {
            Log("Konvertuji nezaplacené faktury.");
            try
            {
                using (var odbcCmd = dbf.CreateCommand())
                {
                    odbcCmd.CommandText = String.Format("DELETE FROM {0}",
                        dbfPath + "\\DBFDLUH.DBF");
                    odbcCmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Log(String.Format("Nepodařilo se odstranit data z DBFDLUH. {0}", e.Message));
            }

            using (var sqlCmd = sql.CreateCommand())
            {
                sqlCmd.CommandText = getUnpaidInvoicesQuery();
                int[] counters = { 0, 0 };
                Dictionary<string, string> data = null;
                using (var reader = sqlCmd.ExecuteReader())
                {
                    int fieldCount = reader.FieldCount;
                    while (reader.Read())
                    {
                        counters[0]++;
                        data = new Dictionary<string, string>();
                        for (int i = 0; i < fieldCount; i++)
                        {
                            data.Add(reader.GetName(i), reader.GetValue(i).ToString());
                        }

                        string cmd = "";
                        try
                        {
                            using (var odbcCmd = dbf.CreateCommand())
                            {
                                string[] values = data.Values.ToArray();
                                escapeValues(values);
                                cmd = odbcCmd.CommandText = String.Format("INSERT INTO {0} ({1}) VALUES ('{2}')",
                                    dbfPath + "\\DBFDLUH.DBF",
                                    String.Join(", ", data.Keys), String.Join("', '", values));
                                odbcCmd.ExecuteNonQuery();
                                counters[1]++;
                            }
                        }
                        catch (SqlException e)
                        {
                            Log(String.Format("Nepodařilo se vložit nezaplacenou fakturu č. {0}. {1}", data["CisloFakRu"], e.Message));
                            Log(e.ToString());
                            Log(cmd);
                            Log("Pokračuji...");
                        }
                    }
                }

                Log(String.Format("Zpracováno {0} nezaplacených faktur. Vloženo: {1}, chybných: {2}",
                    counters[0], counters[1], counters[0] - counters[1]));
            }
        }

        private void addNpCostumersToSheet()
        {
            try {
                GoogleCredential credential;
                using (var stream = new FileStream("client_secret.json", FileMode.Open, FileAccess.Read))
                {
                    credential = GoogleCredential.FromStream(stream)
                        .CreateScoped(Scopes);
                }

                // Create Google Sheets API service.
                SheetsService service = new SheetsService(new BaseClientService.Initializer()
                {
                    HttpClientInitializer = credential,
                    ApplicationName = appName,
                });

                var range = $"{sheetName}!A2:A";
                SpreadsheetsResource.ValuesResource.GetRequest request =
                        service.Spreadsheets.Values.Get(sheetId, range);

                var response = request.Execute();
                IList<IList<object>> values = response.Values;

                List<string> ids = new List<string>();

                if (values != null && values.Count > 0)
                {
                    Log(String.Format("V spreadsheetu bylo nalezeno {0} odběratelů.", values.Count));
                    foreach (var row in values)
                    {
                        ids.Add(row[0].ToString());
                    }
                }
                else
                {
                    Log("Žádná data nebyla v spreadsheetu nalezena.");
                }

                using (var sqlCmd = sql.CreateCommand())
                {
                    sqlCmd.CommandText = getNpCostumersQuery(ids);
                    using (var reader = sqlCmd.ExecuteReader())
                    {
                        int count = 0;
                        while (reader.Read())
                        {
                            range = $"{sheetName}!A2:F";
                            var valueRange = new ValueRange();

                            var oblist = new List<object>()
                            {
                                reader.GetValue(0).ToString(),
                                reader.GetValue(1).ToString(),
                                reader.GetValue(2).ToString(),
                                reader.GetValue(3).ToString(),
                                reader.GetValue(4).ToString(),
                                reader.GetValue(5).ToString()
                            };
                            valueRange.Values = new List<IList<object>> { oblist };

                            try {

                                var appendRequest = service.Spreadsheets.Values.Append(valueRange, sheetId, range);
                                appendRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.USERENTERED;
                                var appendReponse = appendRequest.Execute();
                            }
                            catch (Exception e) {
                                Log("Nepodařilo se vložit data do sheetu. " + e.ToString());
                            }
                            count++;
                        }

                        Log(String.Format("Vloženo do spreadsheetu {0} odběratelů.", count));
                    }
                }

            }
            catch (Exception e)
            {
                Log("Nepodařilo se spojit s sheetem. " + e.ToString());
            }
        }

        private string getNpCostumersQuery(List<string> ids)
        {
            return String.Format(
                "select distinct cast(AD.Smlouva as INTEGER) as CisloOdb, AD.ICO as ICO, AD.Firma as Firma, AD.Email as Email, AD.Tel as Telefon, ADdodaci.Email2 as EmailNP" +
                " from AD" +
                " join FA on AD.ID=FA.RefAD" +
                " join sSTR on FA.RefStr=sSTR.ID" +
                " left join ADdodaci on ADdodaci.RefAg=AD.ID and ADdodaci.Utvar2='NP'" +
                " where sSTR.IDS='NP'" +
                (ids.Capacity > 0 ? " and AD.Smlouva not in ({0})" : "") +
                " order by CisloOdb", 
                String.Join(",", ids));
        }

        public void Execute()
        {
            convertCostumers();
            convertSuppliers();
            convertInvoices();
            convertUnpaidInvoices();
            addNpCostumersToSheet();
        }

        private void Log(string message) {
            logStream.WriteLine(message);
        }
    }
}

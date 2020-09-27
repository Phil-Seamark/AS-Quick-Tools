using System;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AnalysisServices.AdomdClient;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;


namespace V4
{
    class Program
    {
        static Server server;
        static string consoleDelimeter = "##################################################################";
        static string appFolder = "c:\\vscode\\v4\\";

        static void Main(string[] args)
        {


            Model model;


            server = new Server();
            if (args.Length > 0)
            {
                string pbiserver = args[0].ToString();
                server.Connect(pbiserver);
                model = server.Databases[0].Model;
            }
            else
            {
                setServer();
                model = setModel();
            }



            string userInput = "";
            while (userInput != "0")
            {
                Console.WriteLine(consoleDelimeter);
                Console.WriteLine($"Server  : {server.ConnectionString}");
                Console.WriteLine($"Database: {model.Database.Name}");
                Console.WriteLine(consoleDelimeter);
                Console.WriteLine($"    0   Exit");
                Console.WriteLine($"");
                Console.WriteLine($"    1   List Tables Storage Modes");
                Console.WriteLine($"    2   List Partition Queries");
                Console.WriteLine($"    3   Delete Tables");
                Console.WriteLine($"    4   List Tables");
                Console.WriteLine($"    5   Run DAX query");
                Console.WriteLine($"    6   Process Table");
                Console.WriteLine($"    7   Set Database");
                Console.WriteLine($"    8   Set Server");
                Console.WriteLine($"    9   List Table Processed state");
                Console.WriteLine($"   10   Get Database as TMSL");
                Console.WriteLine($"   11   Get Table as TMSL");
                Console.WriteLine(consoleDelimeter);

                userInput = Console.ReadLine();
                switch (userInput)
                {
                    case "1":
                        getStorageMode(model);
                        break;
                    case "2":
                        getPartitionQueries(model);
                        break;
                    case "3":
                        fnDeleteAllTablesModel(model);
                        break;
                    case "4":
                        getTables(model);
                        break;
                    case "5":
                        executeDAX(model);
                        break;
                    case "6":
                        processPartitions(model);
                        break;
                    case "7":
                        model = setModel();
                        break;
                    case "8":
                        setServer();
                        model = setModel();
                        break;
                    case "9":
                        getTableProcessedState(model);
                        break;
                    case "10":
                        getModelTMSL(model);
                        break;
                    case "11":
                        getTableTMSL(model);
                        break;

                    default:
                        Console.WriteLine($"You chose option: {userInput}");
                        break;
                }
            }
        }

        static private void getModelTMSL(Model model)
        {

            String tmslDB = Microsoft.AnalysisServices.Tabular.JsonSerializer.SerializeDatabase(server.Databases[0]);
            String tmsl = $"{{\"createOrReplace\": {{\"object\": {{\"database\": \"{model.Database.Name}\"}},\"database\": {tmslDB}}}}}";

            String tmslResultFileName = $"{appFolder}TMSLResult-{Guid.NewGuid()}.tsv";
            File.AppendAllText(tmslResultFileName, tmsl);
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.FileName = "NOTEPAD.EXE";
            startInfo.Arguments = tmslResultFileName;
            Process.Start(startInfo);

        }


        static private void getTableTMSL(Model model)
        {

            int i = 0;
            foreach (Table table in model.Tables)
            {
                Console.WriteLine($"{i,4} - {table.Name,-30}");
                i++;
            }


            String s = Console.ReadLine();
            int tableIndex = int.Parse(s);


            Table selectedTable = model.Tables[tableIndex];

            String tmslTable = Microsoft.AnalysisServices.Tabular.JsonSerializer.SerializeObject(selectedTable);
            String tmsl = $"{{\"createOrReplace\": {{\"object\": {{\"database\": \"{model.Database.Name}\",\"table\": \"{selectedTable.Name}\" }},\"table\": {tmslTable}}}}}";

            String tmslResultFileName = $"{appFolder}TMSLResult-{Guid.NewGuid()}.tsv";
            File.AppendAllText(tmslResultFileName, tmsl);
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.FileName = "NOTEPAD.EXE";
            startInfo.Arguments = tmslResultFileName;
            Process.Start(startInfo);

        }


        static private void getTableProcessedState(Model model)
        {
            foreach (Table table in model.Tables)
            {
                foreach (Partition partition in table.Partitions)
                {
                    Console.WriteLine("{0,-30}{1,-30}{2,-20}",
                        table.Name,
                        partition.Name,
                        partition.State.ToString()
                        );

                }
            }
        }

        static private void saveData(String filename, String line)
        {
            String[] lines = System.IO.File.ReadAllLines(filename);
            bool lineFound = false;
            foreach (string l in lines)
            {
                if (l == line)
                {
                    lineFound = true;
                }
            }

            string[] writeThis = { line };
            if (!lineFound)
            {
                System.IO.File.AppendAllLines(filename, writeThis);
            }
        }

        static private string getData(String filename)
        {

            filename = appFolder + filename;
            Console.WriteLine(consoleDelimeter);
            if (!System.IO.File.Exists(filename))
            {
                String[] x = { "" };
                System.IO.File.AppendAllLines(filename, x);
            }

            String[] lastData = System.IO.File.ReadAllLines(filename);
            int i = 0;
            foreach (string q in lastData)
            {
                Console.WriteLine($"{i}    {q}");
                i++;
            }
            Console.WriteLine(consoleDelimeter);

            String userInput = Console.ReadLine();

            if (userInput.ToUpper().StartsWith("DEL"))
            {
                //TODO: delete code here

                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.FileName = "NOTEPAD.EXE";
                startInfo.Arguments = filename;
                Process.Start(startInfo);
            }
            else
            {
                String[] lines = { userInput };

                if (Int32.TryParse(userInput, out int queryNumber))
                {
                    userInput = lastData[queryNumber];
                }
                else
                {
                    saveData(filename, userInput);
                }
            }
            return userInput;

        }


        static private void setServer()
        {
            string serverName = "";
            Console.Clear();
            Console.WriteLine(consoleDelimeter);
            Console.WriteLine($"Enter or Pick a Server");

            serverName = getData("Servers.dat");


            if (server.Connected)
            {
                server.Disconnect();
            }
            try
            {
                server.Connect(serverName);

                saveData(appFolder + "Servers.dat", serverName);
                Console.Clear();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.InnerException.ToString()}");
            }
        }
        static private Model setModel()
        {
            Model model = null;
            Dictionary<int, string> x = setDBIndex();
            Console.Clear();
            Console.WriteLine(consoleDelimeter);
            Console.WriteLine($"Please pick a database:");
            Console.WriteLine(consoleDelimeter);

            foreach (KeyValuePair<int, string> database in x)
            {
                Console.WriteLine($"\t{database.Key} - {database.Value}");
            }


            String s = Console.ReadLine();
            int dbIndex = int.Parse(s);


            if (x.ContainsKey(int.Parse(s)))
            {
                string db;
                x.TryGetValue(dbIndex, out db);


                model = server.Databases[db].Model;
            }

            Console.Clear();
            return model;

        }
        static private Dictionary<int, string> setDBIndex()
        {

            /***********************************************************
                   Table Mapping Dictionary
                ***********************************************************/

            Dictionary<int, string> databases = new Dictionary<int, string>();
            int dbIndex = 0;

            foreach (Database database in server.Databases)
            {
                string databaseName = database.Name;
                if (!databases.ContainsValue(databaseName))
                {
                    databases.Add(dbIndex, databaseName);
                    dbIndex++;
                }
            }
            return databases;

        }

        static private void fnDeleteAllTablesModel(Model model)
        {
            Console.WriteLine(consoleDelimeter);
            Console.WriteLine($"Please enter table pattern");
            String contains = Console.ReadLine();

            int i = 0;
            foreach (Table mytable in model.Tables)
            {
                if (mytable.Name.ToUpper().Contains(contains.ToUpper().Trim()))
                {
                    Console.WriteLine($"Deleting {mytable.Name}");
                    model.Tables.Remove(mytable.Name);
                    i++;
                }
            }

            model.SaveChanges();
            Console.WriteLine($"Deleted {i} tables ");

        }

        static private void showConsoleHeader(Model model)
        {
            Console.WriteLine(consoleDelimeter);
            Console.WriteLine($"Server  : {server.ConnectionString}");
            Console.WriteLine($"Database: {model.Database.Name}");
            Console.WriteLine(consoleDelimeter);
        }

        static private void executeDAX(Model model)
        {
            //string serverName = ""; // ("asazure://aspaaseastus2.asazure.windows.net/aascdhdatahubeunpd2:rw");
            Console.Clear();
            showConsoleHeader(model);
            Console.WriteLine($"Enter or Pick a Query");

            string query = getData("Query.dat");

            AdomdConnection adomdConnection = new AdomdConnection($"Data Source={model.Database.Parent.ConnectionString};Initial catalog={model.Database.Name}");
            AdomdCommand adomdCommand = new AdomdCommand(query, adomdConnection);
            adomdConnection.Open();
            String queryResultFileName = $"{appFolder}QueryResult-{Guid.NewGuid()}.tsv";
            List<string> list = new List<string>();
            bool hasHeader = false;
            try
            {
                AdomdDataReader reader = adomdCommand.ExecuteReader();
                while (reader.Read())
                {

                    String rowResults = "";
                    /*****************************************************************************
                        Add Header (if needed)
                    ****************************************************************************/
                    if (!hasHeader)
                    {
                        for (
                            int columnNumber = 0;
                            columnNumber < reader.FieldCount;
                            columnNumber++
                            )
                        {
                            if (columnNumber > 0)
                            {
                                rowResults += $"\t";
                            }
                            rowResults += $"{reader.GetName(columnNumber)}";
                        }
                        Console.WriteLine(rowResults);
                        list.Add(rowResults);
                        hasHeader = true;
                    }
                    /*****************************************************************************
                        Add normal line
                    ****************************************************************************/
                    rowResults = "";
                    // Create a loop for every column in the current row
                    for (
                        int columnNumber = 0;
                        columnNumber < reader.FieldCount;
                        columnNumber++
                        )
                    {
                        if (columnNumber > 0)
                        {
                            rowResults += $"\t";
                        }
                        rowResults += $"{reader.GetValue(columnNumber)}";
                    }
                    Console.WriteLine(rowResults);
                    list.Add(rowResults);
                }


                System.IO.File.WriteAllLines(queryResultFileName, list);


                ProcessStartInfo startInfo = new ProcessStartInfo();
                bool excelFound = false;
                if (File.Exists("C:\\Program Files\\Microsoft Office\\root\\Office16\\EXCEL.EXE"))
                {
                    startInfo.FileName = "C:\\Program Files\\Microsoft Office\\root\\Office16\\EXCEL.EXE";
                    excelFound = true;
                }
                else
                {
                    if (File.Exists("C:\\Program Files (x86)\\Microsoft Office\\root\\Office16\\EXCEL.EXE"))
                    {
                        startInfo.FileName = "C:\\Program Files (x86)\\Microsoft Office\\root\\Office16\\EXCEL.EXE";
                        excelFound = true;
                    }
                }

                if (excelFound)
                {
                    startInfo.Arguments = queryResultFileName;
                    Process.Start(startInfo);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.InnerException.ToString());
                Console.ReadKey();
            }
            adomdConnection.Close();

        }

        static private void getStorageMode(Model model)
        {
            foreach (Table table in model.Tables)
            {
                //Console.WriteLine ($"Tablename = {table.Name} - Mode {table.Partitions[0].Mode}");

                Console.WriteLine("    {0,-30}{1,-20}",
                    table.Name,
                    table.Partitions[0].Mode
                    );
            }
        }


        static private void getRelationships(Model model)
        {
            foreach (Relationship relationship in model.Relationships)
            {
                if (relationship.ToTable == model.Tables["AT"])
                {
                    Console.WriteLine($"{relationship.FromTable.Name} at_flag ");
                }
            }
        }
        static private void getPartitionQueries(Model model)
        {
            foreach (Table table in model.Tables)
            {
                //TODO handle different partition types
                QueryPartitionSource queryPartitionSource = (QueryPartitionSource)table.Partitions[0].Source;
                Console.WriteLine($"Table = {table.Name} - Query =  {queryPartitionSource.Query}");
            }
        }

        static private void getTables(Model model)
        {
            foreach (Table table in model.Tables)
            {
                Console.WriteLine($"  Table = {table.Name}");
            }
        }

        static private void setPartitionQueries(Model model)
        {
            foreach (Table table in model.Tables)
            {
                QueryPartitionSource queryPartitionSource = (QueryPartitionSource)table.Partitions[0].Source;
                //queryPartitionSource.Query = queryPartitionSource.Query.Replace("TOP 0","");

                if (queryPartitionSource.Query.ToUpper().Contains("UNION"))
                {
                    Console.WriteLine($"{table.Name} {queryPartitionSource.Query}");
                    queryPartitionSource.Query = queryPartitionSource.Query.Replace("UNION", "--UNION");
                }


            }
            model.SaveChanges();
        }


        static private void processPartitions(Model model)
        {
            Console.WriteLine(consoleDelimeter);
            Console.WriteLine($"Please enter table pattern");
            String contains = Console.ReadLine();

            foreach (Table table in model.Tables)
            {
                if (table.Name.ToUpper().Contains(contains.ToUpper()))

                {

                    //if(table.Partitions[0].State!=ObjectState.Ready)
                    //{
                    Console.WriteLine($"Processing table : {table.Name}");
                    table.RequestRefresh(RefreshType.Full);
                    try
                    {
                        model.SaveChanges();
                        Console.WriteLine($"Completed  table : {table.Name}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{table.Name} : {ex.InnerException.ToString()})");
                    }
                    // }
                    // else{
                    //     Console.WriteLine($"Skipping   table : {table.Name}");
                    // }

                }

            }
        }

        static private void setPartitionMode(Model model, ModeType modeType)
        {
            foreach (Table table in model.Tables)
            {
                foreach (Partition partition in table.Partitions)
                {
                    int i = 0;
                    if (partition.Mode != modeType)
                    {
                        partition.Mode = modeType;
                    }
                    if (modeType == ModeType.DirectQuery && i > 1)
                    {
                        table.Partitions.Remove(partition);
                    }
                    i++;
                }
            }
            model.SaveChanges();
        }
    }
}

using Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utils;

namespace TSVDB
{
    public class TSVFile
    {
        public string HeaderLine { get; set; }
        public Dictionary<string, int> ColIndex { get; set; }
        public BlockingCollection<string[]> Lines {set; get;}
        public string FileName { get; set; }
        public char ColSeparator { set; get; }

        public DTOOut DTO { get; set; }


        Encoding Enc { set; get; }

        public TSVFile(string fileName, char _colSeparator = '\t')
        {
            DTO = new DTOOut();
            FileName = fileName;

            Enc = Encoding.UTF8;

            ColSeparator = _colSeparator;
            ColIndex = new Dictionary<string, int>();
            Lines = new BlockingCollection<string[]>();

        }
        
        public async Task<DTOOut> Split(string fileName, int batchSize)
        {
            DTO = new DTOOut();
            string[] lines = ".".Split('.');
            int start = 0;
            string Header = "";
            string fName = "";

            int batchCount = 1;
            while(lines.Length > 0 )
            {
                try
                { 
                    lines = File.ReadAllLines(fileName).Skip(start).Take(batchSize).ToArray();
                    start += lines.Length;
                    if (lines.Length > 0)
                    {
                        if (Header.IsEmpty())
                            Header = lines[0];

                        fName = fileName.GetNewName($".Split_{batchCount}");
                        await File.WriteAllTextAsync(fName, $"{Header}\r\n");
                        await File.WriteAllLinesAsync(fName, lines);
                        batchCount++;
                    }
                }
                catch(Exception ex)
                {
                    DTO.Result = -1;
                    DTO.Message = "Error reading file";
                    DTO.Ex = ex;
                    break;
                }
            }

            return DTO;
        }


        public void SetHeader(string line, char separator = '\t')
        {
            string[] cols = line.Trim().Split(separator);
            if ( cols.Length > 0 )
            {
                HeaderLine = line;
                ColIndex = new Dictionary<string, int>();
                for( int i=0; i < cols.Length; i++)
                {
                    ColIndex.Add(cols[i].Trim(), i);
                }
            }
        }

        public async Task<DTOOut> Load(string fileName = "", char colSeparator = '\t',  Encoding _enc = null )
        {
            string[] lines = Array.Empty<string>();
            if (fileName.IsEmpty())
                fileName = FileName;

            if (_enc == null)
                _enc = Enc;

            if (fileName.IsNotEmpty() && File.Exists(fileName))
            {
                try
                {
                    lines = await File.ReadAllLinesAsync(fileName, _enc);
                }
                catch(Exception ex)
                {
                    DTO.Result = -1;
                    DTO.Message = "Error reading file";
                    DTO.Ex = ex;
                    return DTO;
                }

                if (lines.Length > 0)
                {
                    SetHeader(lines[0], colSeparator);
                    lines = lines.Skip(1).ToArray();

                    Parallel.ForEach(lines, line =>
                    {
                        string[] cols = line.Split(colSeparator);

                        if (cols.Length < ColIndex.Count)
                            cols = $"{line}{colSeparator}{"".PadLeft(ColIndex.Count - cols.Length, colSeparator)}".Split(colSeparator);
                        Lines.TryAdd(cols);
                    }
                    );
                }
            }
            return DTO;
        }

        public async Task<DTOOut> AddColumnIds(string[] files, string colNames, string additionalCols = "", 
            char colSeparator = '\t')
        {
            foreach(string fileName in files)
            {
                TSVFile tsv = new TSVFile(fileName);
                await tsv.Load();
                DTO = await tsv.AddColumnIds(colNames, additionalCols, colSeparator);
                if (DTO.Result < 0)
                    break;
            }
            return DTO;
        }


        public async Task<DTOOut> AddColumnIds( string colNames, string additionalCols = "", char colSeparator = '\t')
        {
            colNames = colNames.Trim().Replace($"{ColSeparator}", $"Id{colSeparator}");
            colNames += "Id";
            if (additionalCols.IsNotEmpty())
                colNames += $"{colSeparator}{additionalCols}";

            SetHeader(colNames, colSeparator);

            Parallel.ForEach(Lines, line =>
            {
                string addCols = "".PadLeft(ColIndex.Count - line.Length, '\t');
                if ( line.Length < ColIndex.Count)
                    line = $"{string.Join(colSeparator, line)}{colSeparator}{addCols}".Split(colSeparator);
            }
            );
            await Save();
            return DTO;
        }

        public async Task<DTOOut> AddColumns(string[] files, string additionalCols, char colSeparator = '\t')
        {
            foreach (var fileName in files)
            {
                TSVFile tsv = new TSVFile(fileName);
                DTO = await tsv.Load();
                DTO = await tsv.AddColumns(additionalCols, colSeparator);
                if (DTO.Result < 0)
                    break;
            }
            return DTO;
        }

        public async Task<DTOOut> AddColumns(string additionalCols, char colSeparator = '\t')
        {
            SetHeader(additionalCols.Trim(), colSeparator);

            Parallel.ForEach(Lines, line =>
            {
                string l2 = string.Join(ColSeparator, line);
                string addCols = "".PadLeft(ColIndex.Count - line.Length, '\t');
                if (line.Length < ColIndex.Count)
                    line = $"{l2}{ColSeparator}{addCols}".Split(ColSeparator);
            }
            );
            await Save();
            return DTO;
        }


        public async Task<DTOOut> RemoveColumns(string fileName, string colNames, char colSeparator = '\t')
        {
            var cols = colNames.Split(colSeparator);
            StringBuilder sbHeader = new StringBuilder();
            for(int i=0; i <cols.Length; i++)
            {
                cols[i] = cols[i].Trim();
                if (ColIndex.ContainsKey(cols[i]))
                {
                    ColIndex.Remove(cols[i]);
                }
            }
            BlockingCollection<string> newLines = new BlockingCollection<string>();

            foreach (var kvp in ColIndex)
                sbHeader.Append($"{kvp.Key}{ColSeparator}");
            sbHeader.AppendLine();

            Parallel.ForEach(Lines, line =>
            {
                StringBuilder sb = new StringBuilder();
                foreach (var kvp in ColIndex)
                    sb.Append($"{line[kvp.Value]}{ColSeparator}");

                newLines.TryAdd(sb.ToString());
            }
            ) ;

            DTO = new DTOOut();
            try
            {
                await File.WriteAllTextAsync(fileName, sbHeader.ToString());
                await File.AppendAllLinesAsync(fileName, newLines);
            }
            catch(Exception ex)
            {
                DTO.Result = -1;
                DTO.Message = "Error saving file.";
                DTO.Ex = ex;
            }

            return DTO;

        }


        public async Task<DTOOut> SelectColumns(string[] files, string colNames, char colSeparator = '\t')
        {
            foreach (var fileName in files)
            {
                TSVFile tsv = new TSVFile(fileName);
                await tsv.Load();
                DTO = await tsv.SelectColumns(fileName, colNames, colSeparator);
                if (DTO.Result < 0)
                    break;
            }
            return DTO;
        }

        public async Task<DTOOut> SetValues(string[] files, string colNames, string values, char colSeparator = '\t')
        {
            foreach (var fileName in files)
            {
                TSVFile tsv = new TSVFile(fileName);
                await tsv.Load();
                DTO = await tsv.SetValues(FileName, colNames, values, colSeparator);
                if (DTO.Result < 0)
                    break;
            }
            return DTO;
        }


        public async Task<DTOOut> SetValues(string fileName, string colNames, string values, char colSeparator = '\t')
        {
            DTO = new DTOOut();
            colNames = colNames.Trim();
            values = values.Trim();
            string[] vals = values.Split(colSeparator);
            string[] cols = colNames.Split(colSeparator);

            if ( vals.Length != cols.Length)
            {
                DTO.Result = -1;
                DTO.Message = "Columns and values array are different sizes";
                return DTO;
            }

            Parallel.ForEach(Lines, line =>
            {
                for (int i = 0; i < cols.Length; i++)
                {
                    cols[i] = cols[i].Trim();
                    if (ColIndex.ContainsKey(cols[i]))
                    {
                        StringBuilder sb = new StringBuilder();
                        string[] v2 = vals[i].Split('$');
                        if (v2.Length > 0)
                        {
                            sb.Append(v2[0]);
                            for (int j = 1; j < v2.Length; j++)
                            {
                                v2[j] = v2[j].Trim();
                                if (ColIndex.ContainsKey(v2[j]))
                                    sb.Append(v2[j]);
                            }
                        }
                        line[ColIndex[cols[i]]] = sb.ToString();
                    }
                }
            }
            );
            await Save();

            return DTO;
        }

        public async Task<DTOOut> SelectColumns(string fileName, string colNames,char colSeparator = '\t')
        {
            var cols = colNames.Split(colSeparator);
            StringBuilder sbHeader = new StringBuilder();
            Dictionary<string, int> newColIndex = new Dictionary<string, int>();

            if ( !colNames.ToLower().Contains("filename"))
                sbHeader.Append($"FileName{ColSeparator}");

            for (int i = 0; i < cols.Length; i++)
            {
                cols[i] = cols[i].Trim();
                sbHeader.Append($"{cols[i]}{ColSeparator}");
            }
            BlockingCollection<string> newLines = new BlockingCollection<string>();
            sbHeader.AppendLine();

            Parallel.ForEach(Lines, line =>
            {
                StringBuilder sb = new StringBuilder();
                foreach (var col in cols)
                    if ( ColIndex.ContainsKey(col))
                        sb.Append($"{line[ColIndex[col]]}{ColSeparator}");
                    else
                        sb.Append($"{ColSeparator}");

                newLines.TryAdd($"{Path.GetFileName(fileName)}{ColSeparator}{sb}");
            }
            );

            DTO = new DTOOut();
            try
            {
                await File.WriteAllTextAsync(fileName, sbHeader.ToString());
                await File.AppendAllLinesAsync(fileName, newLines);
            }
            catch (Exception ex)
            {
                DTO.Result = -1;
                DTO.Message = "Error saving file.";
                DTO.Ex = ex;
            }

            return DTO;

        }



        public async Task<DTOOut> Save(string fileName = "")
        {
            DTO = new DTOOut();
            if (fileName.IsEmpty())
                fileName = FileName;

            try
            {
                BlockingCollection<string> lines = new BlockingCollection<string>();
                await File.WriteAllTextAsync(fileName, $"{HeaderLine}\r\n");

                Parallel.ForEach(Lines, line =>
                {
                    lines.TryAdd(string.Join(ColSeparator, line));
                }
                );

                if ( lines.Count > 0 )
                    await File.AppendAllLinesAsync(fileName, lines);
            }
            catch(Exception ex)
            {
                DTO.Result = -1;
                DTO.Message = "Error saving file";
                DTO.Ex = ex;
            }


            return DTO;
        }

    }
}

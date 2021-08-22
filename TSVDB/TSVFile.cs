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

        public TSVFile(string fileName, char _colSeparator = '\t', Encoding _enc = null )
        {
            DTO = new DTOOut();
            FileName = fileName;

            if (_enc == null)
                _enc = Encoding.UTF8;
            Enc = _enc;

            ColSeparator = _colSeparator;
            ColIndex = new Dictionary<string, int>();
            Lines = new BlockingCollection<string[]>();

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
                    DTO.Mesage = "Error reading file";
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

        public async Task<DTOOut> AddColumnIds( string colNames, string additionalCols = "", char colSeparator = '\t')
        {
            colNames = colNames.Trim().Replace($"{ColSeparator}", $"Id{ColSeparator}");
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


        public async Task<DTOOut> AddColumns(string additionalCols, char colSeparator = '\t')
        {
            SetHeader(additionalCols.Trim(), colSeparator);

            Parallel.ForEach(Lines, line =>
            {
                string l2 = string.Join(colSeparator, line);
                string addCols = "".PadLeft(ColIndex.Count - line.Length, '\t');
                if (line.Length < ColIndex.Count)
                    line = $"{l2}{colSeparator}{addCols}".Split(colSeparator);
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
                sbHeader.Append($"{kvp.Key}{colSeparator}");
            sbHeader.AppendLine();

            Parallel.ForEach(Lines, line =>
            {
                StringBuilder sb = new StringBuilder();
                foreach (var kvp in ColIndex)
                    sb.Append($"{line[kvp.Value]}{colSeparator}");

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
                DTO.Mesage = "Error saving file.";
                DTO.Ex = ex;
            }

            return DTO;

        }


        //todo: batched version without parallel for big big files
        public async Task<DTOOut> SelectColumns(string fileName, string colNames,
            char colSeparator = '\t')
        {
            var cols = colNames.Split(colSeparator);
            StringBuilder sbHeader = new StringBuilder();
            Dictionary<string, int> newColIndex = new Dictionary<string, int>();
            for (int i = 0; i < cols.Length; i++)
            {
                cols[i] = cols[i].Trim();
                if (ColIndex.ContainsKey(cols[i]))
                {
                    sbHeader.Append($"{cols[i]}{colSeparator}");
                }
            }
            BlockingCollection<string> newLines = new BlockingCollection<string>();
            sbHeader.AppendLine();

            Parallel.ForEach(Lines, line =>
            {
                StringBuilder sb = new StringBuilder();
                foreach (var col in cols)
                    if ( ColIndex.ContainsKey(col))
                        sb.Append($"{line[ColIndex[col]]}{colSeparator}");
                newLines.TryAdd(sb.ToString());
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
                DTO.Mesage = "Error saving file.";
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
                DTO.Mesage = "Error saving file";
                DTO.Ex = ex;
            }


            return DTO;
        }


        //todo: file split

        public async Task<DTOOut> Split(string fileName, int batchSize)
        {
            DTO = new DTOOut();
            string[] lines = ".".Split('.');
            int start = 0;

            int batchCount = 1;
            while(lines.Length > 0 )
            {
                lines = File.ReadAllLines(fileName).Skip(start).Take(batchSize).ToArray();
                start += lines.Length;
                if ( lines.Length > 0 )
                {

                    batchCount++;
                }
                     
            }




            return DTO;
        }

        //todo: not necesary, batched is required onky for splitting
        public async Task<DTOOut> SaveBatched(string fileName = "", int batchSize = 50000 )
        {
            DTO = new DTOOut();
            if (fileName.IsEmpty())
                fileName = FileName;

            try
            {
                BlockingCollection<string> lines = new BlockingCollection<string>();
                await File.WriteAllTextAsync(fileName, $"{HeaderLine}\r\n");

                foreach(var line in Lines)
                {
                    lines.TryAdd(string.Join(ColSeparator, line));
                    if ( lines.Count > batchSize )
                    {
                        File.AppendAllLines(fileName, lines);
                        lines = new BlockingCollection<string>();
                    }
                }

                if (lines.Count > 0)
                    await File.AppendAllLinesAsync(fileName, lines);
            }
            catch (Exception ex)
            {
                DTO.Result = -1;
                DTO.Mesage = "Error saving file";
                DTO.Ex = ex;
            }


            return DTO;
        }


    }
}

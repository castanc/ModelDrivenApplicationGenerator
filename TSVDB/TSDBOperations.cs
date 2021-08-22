using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TSVDB
{
    public static class TSDBOperations
    {
        public static async Task<DTOOut> InnerJoin(this TSVFile left, TSVFile right, string lKeys, string rKeys, 
            string lCols, string rCols)
        {
            var DTO = new DTOOut();

            return DTO;
        }


        public static async Task<DTOOut> Distinct(this TSVFile left, TSVFile right, string lKeys, string rKeys,
            string lCols, string rCols)
        {
            var DTO = new DTOOut();

            return DTO;
        }

        public static async Task<DTOOut> FullDistinct(this TSVFile left, TSVFile right, string lKeys, string rKeys,
            string lCols, string rCols)
        {
            var DTO = new DTOOut();

            return DTO;
        }


        public static async Task<DTOOut> NotIn(this TSVFile left, TSVFile right, string lKeys, string rKeys,
    string lCols, string rCols)
        {
            var DTO = new DTOOut();

            return DTO;
        }

        public static async Task<DTOOut> Duplicates(this TSVFile left, TSVFile right, string lKeys, string rKeys,
    string lCols, string rCols)
        {
            var DTO = new DTOOut();

            return DTO;
        }




    }
}

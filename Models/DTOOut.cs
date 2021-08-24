using System;
using System.Collections.Generic;

namespace Models
{
    public class DTOOut
    {
        public int Result { get; set; }
        public string Message { get; set; }

        public Exception Ex { get; set; }

        public List<string> Results { get; set; }

        public DTOOut()
        {
            Result = 0;
            Message = "";
        }

    }
}

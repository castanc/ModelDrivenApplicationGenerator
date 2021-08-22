using System;
using System.IO;

namespace Utils
{
    public static class Utils
    {
        public static bool IsEmpty(this string text)
        {
            return string.IsNullOrEmpty(text.Trim());
        }

        public static bool IsNotEmpty(this string text)
        {
            return !string.IsNullOrEmpty(text.Trim());
        }

        public static string GetNewName(this string name, string ext = "")
        {
            if (ext.IsEmpty())
                ext = Path.GetExtension(name);

            return $"{Path.GetDirectoryName(name)}\\{Path.GetFileNameWithoutExtension(name)}{ext}";
        }
    }
}

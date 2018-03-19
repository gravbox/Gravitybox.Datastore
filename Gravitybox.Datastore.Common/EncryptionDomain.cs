#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace Gravitybox.Datastore.Common
{
    /// <summary />
    public static class EncryptionDomain
    {
        private const string encryptionIV = "狟⊘㵕ᶷﳰ␍虭";

        static EncryptionDomain()
        {
        }

        private const int SaltSize = 16;
        private static byte[] SaltBytes = new byte[16] { 34, 187, 182, 3, 56, 90, 27, 253, 87, 99, 198, 132, 66, 199, 31, 89 };

        /// <summary />
        public static int Hash(string s)
        {
            if (string.IsNullOrEmpty(s)) return -1;
            var algorithm = SHA1.Create();
            var b = algorithm.ComputeHash(Encoding.UTF8.GetBytes(s));
            return BitConverter.ToInt32(b, 0);
        }

        /// <summary />
        public static long HashFast(string read)
        {
            if (string.IsNullOrEmpty(read)) return -1;
            UInt64 hashedValue = 3074457345618258791ul;
            for (int i = 0; i < read.Length; i++)
            {
                hashedValue += read[i];
                hashedValue *= 3074457345618258799ul;
            }
            return (long)hashedValue;
        }

    }
}
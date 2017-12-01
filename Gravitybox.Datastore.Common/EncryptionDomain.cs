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
        public static byte[] EncryptBytes(byte[] plainBytes, string password)
        {
            if (plainBytes == null) return null;
            if (string.IsNullOrEmpty(password))
                throw new ArgumentNullException("password");

            // Utilizes helper function to generate random 16 byte salt using RNG
            byte[] salt = new byte[SaltSize];

            // create new password derived bytes using password/salt
            byte[] saltedCipherBytes = null;
            using (Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(password, salt))
            {
                using (Aes aes = AesManaged.Create())
                {
                    // Generate key and iv from password/salt and pass to aes
                    aes.Key = pdb.GetBytes(aes.KeySize / 8);
                    aes.IV = pdb.GetBytes(aes.BlockSize / 8);

                    // Open a new memory stream to write the encrypted data to
                    using (MemoryStream ms = new MemoryStream())
                    {
                        // Create a crypto stream to perform encryption
                        using (CryptoStream cs = new CryptoStream(ms, aes.CreateEncryptor(), CryptoStreamMode.Write))
                        {
                            // write encrypted bytes to memory
                            cs.Write(plainBytes, 0, plainBytes.Length);
                        }
                        // get the cipher bytes from memory
                        byte[] cipherBytes = ms.ToArray();
                        // create a new byte array to hold salt + cipher
                        saltedCipherBytes = new byte[salt.Length + cipherBytes.Length];
                        // copy salt + cipher to new array
                        Array.Copy(salt, 0, saltedCipherBytes, 0, salt.Length);
                        Array.Copy(cipherBytes, 0, saltedCipherBytes, salt.Length, cipherBytes.Length);
                        // convert cipher array to base 64 string
                        //cipherText = Convert.ToBase64String(saltedCipherBytes);
                    }
                    aes.Clear();
                }
            }
            return saltedCipherBytes;
        }

        /// <summary />
        public static byte[] DecryptBytes(byte[] saltedCipherBytes, string password)
        {
            if (saltedCipherBytes == null) return null;
            if (string.IsNullOrEmpty(password))
                throw new ArgumentNullException("password");

            // create array to hold salt
            byte[] salt = new byte[SaltSize];
            // create array to hold cipher
            byte[] cipherBytes = new byte[saltedCipherBytes.Length - salt.Length];

            // copy salt/cipher to arrays
            Array.Copy(saltedCipherBytes, 0, salt, 0, salt.Length);
            Array.Copy(saltedCipherBytes, salt.Length, cipherBytes, 0, saltedCipherBytes.Length - salt.Length);

            // create new password derived bytes using password/salt
            byte[] plainBytes = null;
            using (Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(password, salt))
            {
                using (Aes aes = AesManaged.Create())
                {
                    // Generate key and iv from password/salt and pass to aes
                    aes.Key = pdb.GetBytes(aes.KeySize / 8);
                    aes.IV = pdb.GetBytes(aes.BlockSize / 8);

                    // Open a new memory stream to write the encrypted data to
                    using (MemoryStream ms = new MemoryStream())
                    {
                        // Create a crypto stream to perform decryption
                        using (CryptoStream cs = new CryptoStream(ms, aes.CreateDecryptor(), CryptoStreamMode.Write))
                        {
                            // write decrypted data to memory
                            cs.Write(cipherBytes, 0, cipherBytes.Length);
                        }
                        // convert decrypted array to plain text string
                        //plainText = Encoding.Unicode.GetString(ms.ToArray());
                        plainBytes = ms.ToArray();
                    }
                    aes.Clear();
                }
            }
            return plainBytes;
        }

        /// <summary>
        /// Encrypt a string with a specified key
        /// </summary>
        /// <param name="inString">The string to encrypt</param>
        /// <param name="key">The key to use for encryption</param>
        public static string Encrypt(string inString, string key)
        {
            try
            {
                var aesCSP = new AesCryptoServiceProvider();
                aesCSP.Key = Encoding.Unicode.GetBytes(key);
                aesCSP.IV = Encoding.Unicode.GetBytes(encryptionIV);
                var encString = EncryptString(aesCSP, inString);
                return Convert.ToBase64String(encString);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Decrypt a string with a specified key
        /// </summary>
        /// <param name="inString">The string to decrypt</param>
        /// <param name="key">The key to use for decryption</param>
        public static string Decrypt(string inString, string key)
        {
            try
            {
                var aesCSP = new AesCryptoServiceProvider();
                aesCSP.Key = Encoding.Unicode.GetBytes(key);
                aesCSP.IV = Encoding.Unicode.GetBytes(encryptionIV);
                var encBytes = Convert.FromBase64String(inString);
                return DecryptString(aesCSP, encBytes);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static byte[] EncryptString(SymmetricAlgorithm symAlg, string inString)
        {
            var inBlock = Encoding.Unicode.GetBytes(inString);
            var xfrm = symAlg.CreateEncryptor();
            var outBlock = xfrm.TransformFinalBlock(inBlock, 0, inBlock.Length);
            return outBlock;
        }

        private static string DecryptString(SymmetricAlgorithm symAlg, byte[] inBytes)
        {
            var xfrm = symAlg.CreateDecryptor();
            var outBlock = xfrm.TransformFinalBlock(inBytes, 0, inBytes.Length);
            return Encoding.Unicode.GetString(outBlock);
        }

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
#pragma warning disable 0168
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Serialization;

namespace Gravitybox.Datastore.Common
{
    /// <summary>
    /// Security Routines
    /// </summary>
    public static class SecurityHelper
    {
        private static string _cpuInfo = null;

        /// <summary>
        /// Get a has value for a string
        /// </summary>
        public static string GetHash(string v)
        {
            return GetHash(v, false);
        }

        /// <summary>
        /// Get a has value for a string
        /// </summary>
        public static string GetHash(string v, bool friendly)
        {
            if (friendly)
            {
                var arrInput = GetHashRaw(v);
                int i;
                var sOutput = new StringBuilder(arrInput.Length);
                for (i = 0; i < arrInput.Length; i++)
                {
                    sOutput.Append(arrInput[i].ToString("X2"));
                }
                return sOutput.ToString();
            }
            else
            {
                return System.Text.ASCIIEncoding.Default.GetString(GetHashRaw(v));
            }
        }

        /// <summary />
        public static byte[] GetHashRaw(string v)
        {
            const string salt = "atl@!xmf!$Jhg";
            var q = System.Security.Cryptography.SHA1CryptoServiceProvider.Create().ComputeHash(System.Text.Encoding.ASCII.GetBytes(salt + v));
            return q;
        }

        /// <summary>
        /// Get a unique machine ID
        /// </summary>
        public static string GetMachineId()
        {
            if (_cpuInfo == null)
            {
                _cpuInfo = "3335728991726982";
                try
                {
                    var mc = new System.Management.ManagementClass("win32_processor");
                    var moc = mc.GetInstances();

                    foreach (System.Management.ManagementObject mo in moc)
                    {
                        _cpuInfo = mo.Properties["processorID"].Value.ToString();
                        break;
                    }

                }
                catch (Exception ex)
                {
                    //Do Nothing
                }
            }
            return _cpuInfo;
        }

        /// <summary>
        /// Generate a new public\private key pair
        /// </summary>
        public static KeyPair GenerateSymmetricKeys()
        {
            try
            {
                var retval = new KeyPair();
                var cspParams = new CspParameters();
                cspParams.ProviderType = 1; // PROV_RSA_FULL 
                cspParams.Flags = CspProviderFlags.UseArchivableKey;
                cspParams.KeyNumber = (int)KeyNumber.Exchange;
                var rsaProvider = new RSACryptoServiceProvider(cspParams);
                retval.PublicKey = rsaProvider.ToXmlString(false);
                retval.PrivateKey = rsaProvider.ToXmlString(true);
                return retval;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static string Encrypt(string publicKey, string text)
        {
            try
            {
                var cspParams = new CspParameters();
                cspParams.ProviderType = 1; // PROV_RSA_FULL
                var rsaProvider = new RSACryptoServiceProvider(cspParams);
                rsaProvider.FromXmlString(publicKey);
                var plainBytes = Encoding.Unicode.GetBytes(text);
                var encryptedBytes = rsaProvider.Encrypt(plainBytes, false);
                return BitConverter.ToString(encryptedBytes).Replace("-", string.Empty);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static string Decrypt(string privateKey, string cipherText)
        {
            try
            {
                var cspParams = new CspParameters();
                cspParams.ProviderType = 1; // PROV_RSA_FULL
                var rsaProvider = new RSACryptoServiceProvider(cspParams);
                rsaProvider.FromXmlString(privateKey);
                var cipherBytes = ConvertHexStringToByteArray(cipherText);
                var plainBytes = rsaProvider.Decrypt(cipherBytes, false);
                return Encoding.Unicode.GetString(plainBytes);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static bool EncryptObjectToDisk<T>(string fileName, T obj)
        {
            try
            {
                var serializer = new System.Xml.Serialization.XmlSerializer(typeof(KeyPair));
                var memoryStream = new MemoryStream();
                var streamWriter = new StreamWriter(memoryStream, System.Text.Encoding.Unicode);
                serializer.Serialize(streamWriter, obj);
                memoryStream.Seek(0, SeekOrigin.Begin);
                var streamReader = new StreamReader(memoryStream, System.Text.Encoding.Unicode);
                var text = streamReader.ReadToEnd();
                return EncryptToDisk(fileName, text);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static bool EncryptToDisk(string fileName, string data)
        {
            try
            {
                var cryptText = EncryptionDomain.Encrypt(data, SecurityHelper.GetMachineId());
                File.WriteAllText(fileName, cryptText);
                return true;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static T DecryptObjectFromDisk<T>(string fileName)
        {
            try
            {
                var data = DecryptFromDisk(fileName);
                var ser = new XmlSerializer(typeof(T));
                return (T)ser.Deserialize(new StringReader(data));
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary />
        public static string DecryptFromDisk(string fileName)
        {
            try
            {
                var cryptText = File.ReadAllText(fileName);
                return EncryptionDomain.Decrypt(cryptText, GetMachineId());
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private static byte[] ConvertHexStringToByteArray(string hexString)
        {
            if (hexString.Length % 2 != 0)
            {
                throw new ArgumentException("The binary key cannot have an odd number of digits.");
            }

            try
            {
                var hexAsBytes = new byte[hexString.Length / 2];
                for (int index = 0; index < hexAsBytes.Length; index++)
                {
                    var byteValue = hexString.Substring(index * 2, 2);
                    hexAsBytes[index] = byte.Parse(byteValue, NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                }
                return hexAsBytes;
            }
            catch (Exception ex)
            {
                throw new ArgumentException("The value is not a valid hex string");
            }
        }

        /// <summary />
        public static bool IsValidPassword(string password)
        {
            if (string.IsNullOrEmpty(password)) return false;
            if (password.Length < 6) return false;
            return true;
        }

    }

    /// <summary />
    [Serializable]
    public class KeyPair
    {
        /// <summary />
        [DataMember]
        public string PublicKey { get; set; }

        /// <summary />
        [DataMember]
        public string PrivateKey { get; set; }
    }

}
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;

namespace MassaNews.AWS
{
    public class S3
    {
        #region Properts

        private string accessKey { get; set; }
        private string secretKey { get; set; }
        private AmazonS3Config config { get; set; }

        #endregion

        #region Methods

        public S3()
        {
            accessKey = ConfigurationManager.AppSettings["AWSKeyId"];
            secretKey = ConfigurationManager.AppSettings["AWSAccessKey"];

            config = new AmazonS3Config
            {
                RegionEndpoint = RegionEndpoint.USEast1,
            };

            #region Seta configurações de proxy
            if (ConfigurationManager.AppSettings["ProxyAddress"] == null) return;
            config.ProxyHost = ConfigurationManager.AppSettings["ProxyAddress"];
            config.ProxyPort = Convert.ToInt32(ConfigurationManager.AppSettings["ProxyPort"]);
            config.ProxyCredentials = new NetworkCredential(ConfigurationManager.AppSettings["ProxyUser"], ConfigurationManager.AppSettings["ProxyPassword"]);
            #endregion
        }

        private IAmazonS3 GetClient()
        {
            return new AmazonS3Client(accessKey, secretKey, config);
        }

        public IEnumerable<S3Bucket> ListBuckets()
        {
            return GetClient().ListBuckets().Buckets;
        }

        public IEnumerable<S3Object> ListObjectsOfBucket(string bucketName)
        {
            var request = new ListObjectsRequest();

            request.BucketName = bucketName;

            var response = GetClient().ListObjects(request);

            return response.S3Objects;
        }

        public bool CreateNewBucket(string bucketName)
        {
            try
            {
                PutBucketRequest request = new PutBucketRequest
                {
                    BucketName = bucketName
                };

                var result = GetClient().PutBucket(request);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool DeleteBucket(string bucketName)
        {
            try
            {
                var request = new DeleteBucketRequest
                {
                    BucketName = bucketName
                };

                var result = GetClient().DeleteBucket(request);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public bool Exists(string bucketName, string key)
        {
            using (var client = GetClient())
            {
                var request = new ListObjectsRequest
                {
                    BucketName = bucketName,
                    Prefix = key
                };

                return client.ListObjects(request).S3Objects.Any(o => o.Key == key);
            }
        }

        public byte[] GetObjectByStream(string bucketName, string objectKey)
        {
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            };

            var response = GetClient().GetObject(request);

            return ReadToEnd(response.ResponseStream);
        }

        public bool PutObject(string bucketName, string filePath, string newKey, bool tracking = false)
        {
            try
            {
                var fileTransferUtility = new TransferUtility(GetClient());

                var fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    FilePath = filePath,
                    StorageClass = S3StorageClass.Standard,
                    PartSize = 6291456, // 6 MB.
                    CannedACL = S3CannedACL.PublicRead
                };

                if (!string.IsNullOrEmpty(newKey))
                    fileTransferUtilityRequest.Key = newKey.ToLower();

                if (tracking)
                    fileTransferUtilityRequest.UploadProgressEvent += FileTransferUtilityRequest_UploadProgressEvent;

                //Add Cache
                fileTransferUtilityRequest.Headers.CacheControl = "max-age=2592000";

                fileTransferUtility.Upload(fileTransferUtilityRequest);

                Console.WriteLine("Imagem {0} uploaded!", bucketName);


                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro: {0}", ex.Message);
                return false;
            }
        }

        public bool PutObject(string bucketName, Stream fileStream, string newKey, bool tracking = false, IDictionary<string, string> metas = null)
        {
            try
            {
                using (var objMemoryStream = new MemoryStream())
                {
                    fileStream.Seek(0, SeekOrigin.Begin);

                    fileStream.CopyTo(objMemoryStream);

                    var fileTransferUtility = new TransferUtility(GetClient());

                    var fileTransferUtilityRequest = new TransferUtilityUploadRequest
                    {
                        BucketName = bucketName,
                        InputStream = objMemoryStream,
                        StorageClass = S3StorageClass.Standard,
                        PartSize = 6291456, // 6 MB.
                        CannedACL = S3CannedACL.PublicRead,
                    };

                    if (!string.IsNullOrEmpty(newKey))
                        fileTransferUtilityRequest.Key = newKey.ToLower();

                    if (tracking)
                        fileTransferUtilityRequest.UploadProgressEvent += FileTransferUtilityRequest_UploadProgressEvent;

                    //Add custon meta
                    if (metas != null)
                        foreach (var meta in metas)
                            fileTransferUtilityRequest.Metadata.Add($"x-amz-meta-{meta.Key}", meta.Value);

                    //Add Cache
                    fileTransferUtilityRequest.Headers.CacheControl = "max-age=2592000";

                    fileTransferUtility.Upload(fileTransferUtilityRequest);

                    Console.WriteLine("Imagem {0} uploaded!", bucketName);

                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro: {0}", ex.Message);
                return false;
            }
        }

        public bool CopyingObject(string sourceBucket, string objectKey, string destinationBucket, string destObjectKey)
        {
            try
            {
                var request = new CopyObjectRequest
                {
                    SourceBucket = sourceBucket,
                    SourceKey = objectKey,
                    DestinationBucket = destinationBucket,
                    DestinationKey = destObjectKey,
                    CannedACL = S3CannedACL.PublicRead
                };

                var response = GetClient().CopyObject(request);

                return true;
            }
            catch (AmazonS3Exception s3Exception)
            {
                return false;
                //Console.WriteLine(s3Exception.Message, s3Exception.InnerException);
            }
        }

        public bool DeleteBucketObject(string bucketName, string objectKey)
        {
            try
            {
                var request = new DeleteObjectRequest
                {
                    BucketName = bucketName,
                    Key = objectKey
                };

                var response = GetClient().DeleteObjectAsync(request);

                Console.WriteLine(" Object {0} removed!", objectKey);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public void DownloadFile(string bucketName, string path, string key)
        {
            var request = new GetObjectRequest
            {
                BucketName = bucketName,
                Key = "perl_poetry.pdf"
            };

            var response = GetClient().GetObject(request);

            response.WriteResponseStreamToFile("C:\\Users\\larry\\Documents\\perl_poetry.pdf");
        }

        public void CleanBucket(string bucketName)
        {
            var lstObjects = (List<S3Object>)ListObjectsOfBucket(bucketName);

            while (lstObjects.Any())
            {
                foreach (var obj in lstObjects)
                {
                    DeleteBucketObject(bucketName, obj.Key);
                }

                lstObjects = (List<S3Object>)ListObjectsOfBucket(bucketName);
            }
        }

        public void UploadDirectoty(string bucketName, string directoryPath)
        {
            using (var client = GetClient())
            {
                var directoryTransferUtility = new TransferUtility(client);

                var request = new TransferUtilityUploadDirectoryRequest()
                {
                    BucketName = bucketName,
                    Directory = directoryPath,
                    SearchOption = SearchOption.AllDirectories,
                    StorageClass = S3StorageClass.Standard,
                    CannedACL = S3CannedACL.PublicRead,
                    UploadFilesConcurrently = true
                };

                request.UploadDirectoryFileRequestEvent += Request_UploadDirectoryFileRequestEvent;
                request.UploadDirectoryProgressEvent += Request_UploadDirectoryProgressEvent;

                directoryTransferUtility.UploadDirectory(request);
            }
        }

        public void SyncDirectotyToPath(string bucketName, string directoryPath)
        {
            //Recupera lista de keys do diretório
            var lstFiles = Directory.GetFiles(directoryPath, "*.*", SearchOption.AllDirectories);

            int count = lstFiles.Count();

            Console.WriteLine("Foram encontrados {0} arquivos!", lstFiles.Count());

            int index = 0;

            foreach (var file in lstFiles)
            {
                var newKeyName = GetNameKey(file);

                if (!Exists(bucketName, newKeyName))
                {
                    PutObject(bucketName, file, newKeyName);
                }

                index++;

                Console.WriteLine("Sync {0}/{1}", index, count);
            }
        }

        #endregion

        #region Private Methods

        private string GetNameKey(string fullPath)
        {
            return fullPath.Substring(fullPath.IndexOf("Uploads")).Replace('\\', '/').ToLower();
        }

        private byte[] ReadToEnd(Stream stream)
        {
            long originalPosition = 0;

            if (stream.CanSeek)
            {
                originalPosition = stream.Position;
                stream.Position = 0;
            }

            try
            {
                byte[] readBuffer = new byte[4096];

                int totalBytesRead = 0;
                int bytesRead;

                while ((bytesRead = stream.Read(readBuffer, totalBytesRead, readBuffer.Length - totalBytesRead)) > 0)
                {
                    totalBytesRead += bytesRead;

                    if (totalBytesRead == readBuffer.Length)
                    {
                        int nextByte = stream.ReadByte();
                        if (nextByte != -1)
                        {
                            byte[] temp = new byte[readBuffer.Length * 2];
                            Buffer.BlockCopy(readBuffer, 0, temp, 0, readBuffer.Length);
                            Buffer.SetByte(temp, totalBytesRead, (byte)nextByte);
                            readBuffer = temp;
                            totalBytesRead++;
                        }
                    }
                }

                byte[] buffer = readBuffer;
                if (readBuffer.Length != totalBytesRead)
                {
                    buffer = new byte[totalBytesRead];
                    Buffer.BlockCopy(readBuffer, 0, buffer, 0, totalBytesRead);
                }
                return buffer;
            }
            finally
            {
                if (stream.CanSeek)
                {
                    stream.Position = originalPosition;
                }
            }
        }

        #endregion

        #region Events

        private void FileTransferUtilityRequest_UploadProgressEvent(object sender, UploadProgressArgs e)
        {
            Console.WriteLine(" File: {0} de {1}", e.TransferredBytes, e.TotalBytes);
        }

        private void Request_UploadDirectoryProgressEvent(object sender, UploadDirectoryProgressArgs e)
        {
            Console.WriteLine(" Directory: {0} de {1}", e.TransferredBytes, e.TotalBytes);
        }

        private void Request_UploadDirectoryFileRequestEvent(object sender, UploadDirectoryFileRequestArgs e)
        {
            e.UploadRequest.Key = e.UploadRequest.Key.ToLower();
        }

        #endregion
    }
}
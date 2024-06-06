using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;

namespace Seeq.Link.Connector.DirectoryWatch.Utilities {

    /// <summary>
    /// Provides a method of computing a checksum for both in-memory objects and files. Implements convenient toString() and
    /// equals() methods for conversion to a hex representation and for comparison.
    /// </summary>
    public class Checksum : IEquatable<Checksum> {

        /// <summary>
        /// A hash byte array representing the checksum
        /// </summary>
        public byte[] Hash { get; } = new byte[16];

        /// <summary>
        /// Computes a checksum for an object by reading from a file.
        /// </summary>
        /// <param name="fileName">the file to create a checksum for</param>
        public Checksum(string fileName) {
            this.ComputeFromFile(fileName);
        }

        public Checksum(DirectoryInfo directoryInfo) {
            this.computeFromDirectoryInfo(directoryInfo);
        }

        private void computeFromDirectoryInfo(DirectoryInfo directoryInfo) {
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create()) {
                byte[] intBytes = BitConverter.GetBytes(directoryInfo.GetHashCode());
                if (BitConverter.IsLittleEndian)
                    Array.Reverse(intBytes);
                byte[] result = intBytes;
                this.AddDigestToHash(md5.ComputeHash(intBytes));
            }
        }

        /// <summary>
        /// Computes a checksum by hashing a byte array
        /// </summary>
        /// <param name="bytes">the bytes to create a checksum for</param>
        public Checksum(byte[] bytes) {
            this.ComputeFromByteArray(bytes);
        }

        /// <summary>
        /// Intended to be used in conjunction with <see cref="AddFile(string)"/> to checksum a set of files.
        /// </summary>
        public Checksum() { }

        private void ComputeFromByteArray(byte[] bytes) {
            if (bytes == null) {
                return;
            }

            using (MD5 md5 = MD5.Create()) {
                this.AddDigestToHash(md5.ComputeHash(bytes));
            }
        }

        private void ComputeFromFile(string fileName) {
            if (string.IsNullOrWhiteSpace(fileName) || !File.Exists(fileName)) {
                return;
            }

            using (var md5 = MD5.Create())
            using (var fileStream = File.OpenRead(fileName)) {
                this.AddDigestToHash(md5.ComputeHash(fileStream));
            }
        }

        private void AddDigestToHash(byte[] newDigest) {
            using (var result = new BinaryWriter(new MemoryStream(this.Hash)))
            using (var current = new BinaryReader(new MemoryStream(this.Hash)))
            using (var digest = new BinaryReader(new MemoryStream(newDigest))) {
                for (var i = 0; i < 2; i++) {
                    result.Write(current.ReadInt64() + digest.ReadInt64());
                }
            }
        }

        /// <summary>
        /// Add a file to the checksum computation. Useful for computing the checksum of a set of files.
        /// </summary>
        /// <param name="fileName">The file to add to the checksum</param>
        /// <returns>This object to allow fluent style programming.</returns>
        public Checksum AddFile(string fileName) {
            this.ComputeFromFile(fileName);

            return this;
        }

        /// <summary>
        /// Add files to the checksum computation
        /// </summary>
        /// <param name="files">The files to add to the checksum</param>
        /// <returns>This object to allow fluent style programming.</returns>
        public Checksum AddFiles(IEnumerable<string> files) {
            foreach (var fileName in files) {
                this.ComputeFromFile(fileName);
            }

            return this;
        }

        /// <summary>
        /// Add a byte array to the checksum computation.
        /// </summary>
        /// <param name="bytes">The output stream to add to the checksum</param>
        /// <returns>This object to allow fluent style programming.</returns>
        public Checksum addByteArray(byte[] bytes) {
            this.ComputeFromByteArray(bytes);

            return this;
        }

        /// <summary>
        /// Compares two checksum objects for equality
        /// </summary>
        /// <param name="other">the other object for comparison</param>
        /// <returns>true if the checksums are equal</returns>
        public bool Equals(Checksum other) {
            return other != null && this.Hash.SequenceEqual(other.Hash);
        }

        /// <summary>
        /// Returns a hexadecimal string representing the checksum
        /// </summary>
        /// <returns>A hexadecimal string representing the checksum</returns>
        public override string ToString() {
            return BitConverter.ToString(this.Hash).Replace("-", "");
        }

        /// <summary>
        /// Gets the hashcode for the checksum
        /// </summary>
        public override int GetHashCode() {
            return this.Hash.GetHashCode();
        }

        /// <summary>
        /// Compares two checksum objects for equality
        /// </summary>
        /// <param name="obj">the other object for comparison</param>
        /// <returns>true if the checksums are equal</returns>
        public override bool Equals(object obj) {
            return this.Equals(obj as Checksum);
        }
    }
}
using System;
using System.Security.Cryptography;
using System.Text;

namespace MemoryCtl;

internal static class GuidUtil
{
    // Repo-specific namespace GUID (arbitrary but stable).
    private static readonly Guid NamespaceGuid = new("9d2c3192-7b6e-4e33-9bd4-2e8a2b04c8b1");

    /// <summary>
    /// Deterministic GUID (RFC 4122 version 5 style) from a stable string key.
    /// Useful for dedupe keys (e.g. source chat + message range).
    /// </summary>
    public static Guid FromKey(string key) => FromNamespaceAndName(NamespaceGuid, key);

    public static Guid FromNamespaceAndName(Guid @namespace, string name)
    {
        var nsBytes = @namespace.ToByteArray();

        // Convert to network order for hashing
        SwapByteOrder(nsBytes);

        var nameBytes = Encoding.UTF8.GetBytes(name);

        var data = new byte[nsBytes.Length + nameBytes.Length];
        Buffer.BlockCopy(nsBytes, 0, data, 0, nsBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, data, nsBytes.Length, nameBytes.Length);

        byte[] hash;
        using (var sha1 = SHA1.Create())
            hash = sha1.ComputeHash(data);

        var newBytes = new byte[16];
        Array.Copy(hash, 0, newBytes, 0, 16);

        // Set version to 5 (0101)
        newBytes[6] = (byte)((newBytes[6] & 0x0F) | (5 << 4));
        // Set variant to RFC4122
        newBytes[8] = (byte)((newBytes[8] & 0x3F) | 0x80);

        // Back to little-endian for .NET
        SwapByteOrder(newBytes);
        return new Guid(newBytes);
    }

    private static void SwapByteOrder(byte[] guid)
    {
        // RFC 4122 byte order swap for Guid fields
        Array.Reverse(guid, 0, 4);
        Array.Reverse(guid, 4, 2);
        Array.Reverse(guid, 6, 2);
    }
}

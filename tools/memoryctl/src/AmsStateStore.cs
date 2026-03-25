using AMS.Core;

namespace MemoryCtl;

internal static class AmsStateStore
{
    /// <summary>
    /// Derives the AMS state path as a sibling of the --db path, using the db filename as a prefix.
    /// e.g. "foo/bar.memory.jsonl" → "foo/bar.memory.ams.json"
    /// </summary>
    public static string AmsPath(string dbPath)
    {
        var full = Path.GetFullPath(dbPath);
        var dir = Path.GetDirectoryName(full) ?? ".";
        var stem = Path.GetFileNameWithoutExtension(full); // e.g. "bar.memory"
        return Path.Combine(dir, stem + ".ams.json");
    }

    public static AmsStore Load(string dbPath)
    {
        var amsPath = AmsPath(dbPath);
        if (!File.Exists(amsPath)) return new AmsStore();
        return AmsPersistence.Deserialize(File.ReadAllText(amsPath));
    }

    public static void Save(string dbPath, AmsStore store)
    {
        var amsPath = AmsPath(dbPath);
        Directory.CreateDirectory(Path.GetDirectoryName(amsPath)!);
        File.WriteAllText(amsPath, AmsPersistence.Serialize(store), System.Text.Encoding.UTF8);
    }
}

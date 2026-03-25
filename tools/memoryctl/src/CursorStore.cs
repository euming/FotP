using System;
using System.IO;
using System.Text.Json;

namespace MemoryCtl;

internal static class CursorStore
{
    public static ChatCursor Load(string path)
    {
        if (!File.Exists(path))
            return new ChatCursor(LastLineNumber: 0, LastTs: null, LastMessageId: null);

        var json = File.ReadAllText(path);
        var c = JsonSerializer.Deserialize<ChatCursor>(json);
        return c ?? new ChatCursor(0, null, null);
    }

    public static void Save(string path, ChatCursor cursor)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path) ?? ".");
        File.WriteAllText(path, JsonSerializer.Serialize(cursor, new JsonSerializerOptions { WriteIndented = true }));
    }
}

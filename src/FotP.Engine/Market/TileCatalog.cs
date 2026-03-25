using FotP.Engine.Tiles;

namespace FotP.Engine.Market;

public static class TileCatalog
{
    private static readonly List<TileDefinition> _all;
    private static readonly Dictionary<string, TileDefinition> _byId;

    static TileCatalog()
    {
        _all = new List<TileDefinition>
        {
            // Level 3 — Yellow (7)
            new("farmer",            "Farmer",            3, TileColor.Yellow),
            new("guard",             "Guard",             3, TileColor.Yellow),
            new("indentured-worker", "Indentured Worker", 3, TileColor.Yellow),
            new("serf",              "Serf",              3, TileColor.Yellow),
            new("worker",            "Worker",            3, TileColor.Yellow),
            new("beggar",            "Beggar",            3, TileColor.Yellow),
            new("servant",           "Servant",           3, TileColor.Yellow),
            // Level 3 — Blue (1)
            new("soothsayer",        "Soothsayer",        3, TileColor.Blue),
            // Level 3 — Red (3)
            new("ankh",              "Ankh",              3, TileColor.Red),
            new("ancestral-guidance","Ancestral Guidance", 3, TileColor.Red),
            new("omen",              "Omen",              3, TileColor.Red),

            // Level 4 — Yellow (6)
            new("artisan",           "Artisan",           4, TileColor.Yellow),
            new("builder",           "Builder",           4, TileColor.Yellow),
            new("noble-adoption",    "Noble Adoption",    4, TileColor.Yellow),
            new("palace-servants",   "Palace Servants",   4, TileColor.Yellow),
            new("soldier",           "Soldier",           4, TileColor.Yellow),
            new("grain-merchant",    "Grain Merchant",    4, TileColor.Yellow),
            // Level 4 — Blue (2)
            new("entertainer",       "Entertainer",       4, TileColor.Blue),
            new("match-maker",       "Match Maker",       4, TileColor.Blue),
            // Level 4 — Red (3)
            new("good-omen",         "Good Omen",         4, TileColor.Red),
            new("palace-key",        "Palace Key",        4, TileColor.Red),
            new("spirit-of-dead",    "Spirit of the Dead",4, TileColor.Red),

            // Level 5 — Yellow (5)
            new("charioteer",        "Charioteer",        5, TileColor.Yellow),
            new("conspirator",       "Conspirator",       5, TileColor.Yellow),
            new("overseer",          "Overseer",          5, TileColor.Yellow),
            new("ship-captain",      "Ship Captain",      5, TileColor.Yellow),
            new("master-artisan",    "Master Artisan",    5, TileColor.Yellow),
            // Level 5 — Blue (3)
            new("tomb-builder",      "Tomb Builder",      5, TileColor.Blue),
            new("head-servant",      "Head Servant",      5, TileColor.Blue),
            new("priest",            "Priest",            5, TileColor.Blue),
            // Level 5 — Red (3)
            new("bad-omen",          "Bad Omen",          5, TileColor.Red),
            new("burial-mask",       "Burial Mask",       5, TileColor.Red),
            new("royal-decree",      "Royal Decree",      5, TileColor.Red),

            // Level 6 — Yellow (4)
            new("embalmer",          "Embalmer",          6, TileColor.Yellow),
            new("estate-overseer",   "Estate Overseer",   6, TileColor.Yellow),
            new("royal-attendants",  "Royal Attendants",  6, TileColor.Yellow),
            new("priestess",         "Priestess",         6, TileColor.Yellow),
            // Level 6 — Blue (4)
            new("grain-trader",      "Grain Trader",      6, TileColor.Blue),
            new("priest-of-dead",    "Priest of the Dead",6, TileColor.Blue),
            new("astrologer",        "Astrologer",        6, TileColor.Blue),
            new("surveyor",          "Surveyor",          6, TileColor.Blue),
            // Level 6 — Red (3)
            new("pharaohs-gift",     "Pharaoh's Gift",    6, TileColor.Red),
            new("secret-passage",    "Secret Passage",    6, TileColor.Red),
            new("treasure",          "Treasure",          6, TileColor.Red),

            // Level 7 — Yellow (5)
            new("queen",             "Queen",             7, TileColor.Yellow),
            new("general",           "General",           7, TileColor.Yellow),
            new("grand-vizier",      "Grand Vizier",      7, TileColor.Yellow),
            new("granary-master",    "Granary Master",    7, TileColor.Yellow),
            new("heir",              "Heir",              7, TileColor.Yellow),
            // Level 7 — Blue (2)
            new("royal-astrologer",  "Royal Astrologer",  7, TileColor.Blue),
            new("royal-mother",      "Royal Mother",      7, TileColor.Blue),
            // Level 7 — Red (3)
            new("queens-favor",      "Queen's Favor",     7, TileColor.Red),
            new("royal-death",       "Royal Death",       7, TileColor.Red),
            new("royal-power",       "Royal Power",       7, TileColor.Red),
        };

        _byId = _all.ToDictionary(t => t.Id);
    }

    public static IReadOnlyList<TileDefinition> All => _all;

    public static IReadOnlyList<TileDefinition> ByLevel(int level)
        => _all.Where(t => t.Level == level).ToList();

    public static IReadOnlyList<TileDefinition> ByColor(TileColor color)
        => _all.Where(t => t.Color == color).ToList();

    public static IReadOnlyList<TileDefinition> ByColorAndLevel(TileColor color, int level)
        => _all.Where(t => t.Color == color && t.Level == level).ToList();

    public static TileDefinition GetById(string id)
    {
        if (!_byId.TryGetValue(id, out var def))
            throw new KeyNotFoundException($"No tile with id '{id}' in catalog.");
        return def;
    }
}

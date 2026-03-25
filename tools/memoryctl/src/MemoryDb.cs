using System;
using System.Collections.Generic;
using CardBinder.Core;

namespace MemoryCtl;

public sealed class MemoryDb
{
    public CardBinderCore Core { get; }
    public Dictionary<Guid, CardPayload> PayloadByCardId { get; } = new();

    public MemoryDb(CardBinderCore core)
    {
        Core = core;
    }

    public bool TryGetPayload(CardId cardId, out CardPayload payload) => PayloadByCardId.TryGetValue(cardId.Value, out payload!);
}

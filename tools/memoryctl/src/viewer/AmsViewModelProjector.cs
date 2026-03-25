using AMS.Core;
using MemoryGraph.Abstractions;
using MemoryGraph.Infrastructure.AMS;
using MemoryCtl.Inspection;

namespace MemoryCtl.Viewer;

/// <summary>
/// Compatibility facade for the old AMS->view-model projection entry point.
/// AMS data extraction and viewer projection now live in separate classes.
/// </summary>
internal sealed class AmsViewModelProjector
{
    private readonly AmsInspectionSnapshotBuilder _snapshotBuilder;
    private readonly InspectionViewModelProjector _viewModelProjector;

    public AmsViewModelProjector(
        AmsStore store,
        AmsGraphStoreAdapter? adapter = null,
        Dictionary<Guid, MemoryCardPayload>? payloads = null)
    {
        _snapshotBuilder = new AmsInspectionSnapshotBuilder(
            store,
            adapter,
            payloads);
        _viewModelProjector = new InspectionViewModelProjector();
    }

    public RootViewModel Project()
    {
        var snapshot = _snapshotBuilder.Build();
        return _viewModelProjector.Project(snapshot);
    }
}

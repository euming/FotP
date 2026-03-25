// Polyfill for record types in netstandard2.1
#if !NET5_0_OR_GREATER
namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}
#endif

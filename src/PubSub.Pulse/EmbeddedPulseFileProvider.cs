using System.Reflection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Primitives;

namespace PubSub.Pulse;

/// <summary>
/// An <see cref="IFileProvider"/> over the embedded WASM client. Each asset is embedded with its
/// forward-slash relative path as the resource's logical name (see <c>PubSub.Pulse.csproj</c>),
/// so lookup is an exact match — no namespace mangling and no build-time manifest.
/// </summary>
internal sealed class EmbeddedPulseFileProvider : IFileProvider
{
    private readonly Assembly _assembly;
    private readonly Dictionary<string, string> _byPath; // normalized path -> actual resource name

    public EmbeddedPulseFileProvider(Assembly assembly)
    {
        _assembly = assembly;
        _byPath = assembly.GetManifestResourceNames()
            .ToDictionary(n => n.Replace('\\', '/'), n => n, StringComparer.Ordinal);
    }

    /// <summary>True if any asset was embedded — lets the host fail fast on a mis-packaged build.</summary>
    public bool HasContent => _byPath.ContainsKey("index.html");

    public IFileInfo GetFileInfo(string subpath)
    {
        var key = subpath.Replace('\\', '/').TrimStart('/');
        return _byPath.TryGetValue(key, out var resource)
            ? new EmbeddedResourceFile(_assembly, resource, Path.GetFileName(key))
            : new NotFoundFileInfo(subpath);
    }

    public IDirectoryContents GetDirectoryContents(string subpath) => NotFoundDirectoryContents.Singleton;

    public IChangeToken Watch(string filter) => NullChangeToken.Singleton;

    private sealed class EmbeddedResourceFile(Assembly assembly, string resource, string name) : IFileInfo
    {
        public bool Exists => true;
        public bool IsDirectory => false;
        public string? PhysicalPath => null;
        public string Name => name;
        public DateTimeOffset LastModified => DateTimeOffset.UnixEpoch;

        public long Length
        {
            get
            {
                using var stream = assembly.GetManifestResourceStream(resource);
                return stream?.Length ?? 0;
            }
        }

        public Stream CreateReadStream()
            => assembly.GetManifestResourceStream(resource)
               ?? throw new FileNotFoundException($"Embedded resource '{resource}' not found.");
    }
}

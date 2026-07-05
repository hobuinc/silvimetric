from urllib.parse import urlparse, urlunparse


DEFAULT_BACKEND = 'zarr'
BACKEND_PROTOCOLS = {'zarr', 'icechunk', 'tiledb'}


def split_storage_uri(uri: str, default_backend: str = DEFAULT_BACKEND):
    """Return the storage backend name and backend-specific URI."""
    parsed = urlparse(uri)
    scheme = parsed.scheme

    if '+' in scheme:
        backend, data_scheme = scheme.split('+', 1)
        if backend in BACKEND_PROTOCOLS:
            backend = _normalize_backend(backend)
            storage_uri = urlunparse(parsed._replace(scheme=data_scheme))
            return backend, _normalize_storage_uri(storage_uri)

    if scheme in BACKEND_PROTOCOLS:
        backend = _normalize_backend(scheme)
        if parsed.netloc:
            storage_uri = f'{parsed.netloc}{parsed.path}'
        else:
            storage_uri = parsed.path
        if parsed.query:
            storage_uri = f'{storage_uri}?{parsed.query}'
        return backend, _normalize_storage_uri(storage_uri)

    return default_backend, _normalize_storage_uri(uri)


def _normalize_backend(backend: str) -> str:
    if backend == 'icechunk':
        return 'zarr'
    return backend


def _normalize_storage_uri(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme == 'file':
        return parsed.path
    return uri

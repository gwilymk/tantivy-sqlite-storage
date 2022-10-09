# Tantivy sqlite storage

Have you ever wanted to use Tantivy, but got annoyed that it spreads all of its data all over the place rather than in one specific file?
Or wanted to use store your index along with the data it is indexing in one file?

Tantivy sqlite storage puts all of tantivy's files in one sqlite archive and massively reduces performance.
But it does allow for easy backup and restore (via the backup API), live sychronisation to an external server (via something like litestream) or just easier distribution (e.g. save file in an application).

# Example

See the `basic_search` example in the `examples` directory for an idea of how to use the library. Or the
[documentation](https://docs.rs/tantivy-sqlite-storage) for a working example.

# How it works

It is actually very simple.
Tantivy allows for overriding the storage layer with your own one.
Tantivy sqlite storage creates a single table, `tantivy-blobs`, and stores whatever tantivy wanted to store in there.
The table stores file name and content, and nothing else, so you can easily incorporate this with your own sqlite file elsewhere in your application.

# Benchmarks

Terrible benchmarks to follow.

On my machine (steam deck) took 22m3.238s to index wikipedia using tantivy-cli using this storage (not including the document content).
The resulting sqlite file was 3.5G in size.

Normal file store using the default mmap file storage took 19m51.956s to generate the index, and the final index was 3.1G in size.

The example search term (`barak obama`) took 3794 microseconds using sqlite storage and similar time using the default file storage.

However, startup time is significantly slower (about 8s on the same machine) with sqlite storage (takes a few seconds to load the index), and memory usage is much higher.
But once it is loaded, performance is comparable (although memory usage much higher).
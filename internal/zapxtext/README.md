# zapx-bluge segment core

This package is derived from the text-only portions of Bleve zapx v17.2.0.
Vector and command-line sources are intentionally excluded.

The on-disk format is owned by Bluge and is identified by segment type
`zapx-bluge`, version `1`. It is not a Bleve `zap` v17 segment. Version 1 adds
the following unsigned varints to every field metadata record, after the
section address pairs:

1. Number of documents containing at least one indexed term for the field.
2. Sum of all term frequencies for the field.

These values let Bluge construct exact BM25 collection statistics without
scanning the term dictionary during query setup. Any future change to the
field metadata or footer layout must increment the `zapx-bluge` version.

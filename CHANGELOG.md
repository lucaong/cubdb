# Changelog

For versions before `v1.0.0`, the changes are reported in the [GitHub
releases](https://github.com/lucaong/cubdb/releases).

Since `v1.0.0`, `CubDB` follows [semantic versioning](https://semver.org), and
reports changes here.

## v1.1.0

  - Add `clear/1` function to atomically delete all entries in the database

## v1.0.0

### Breaking changes from v0.17.0:

  - Better defaults:
    * `auto_file_sync` now defaults to `true` (slower but durable)
    * `auto_compact` now defaults to `true`
  - Functions `select/2` and `get_and_update_multi/4` now take the timeout as an option instead of an additional argument

### Other changes from v0.17.0:

  - Better internal handling of timeouts that ensures cleanup of resources on the callee side
  - Added `put_and_delete_multi/3` to atomically put and delete entries
  - Added `put_new/3` to put an entry only if the key does not exist yet
  - More efficient implementation of `put_multi/2` and `delete_multi/2`
  - Function `get_multi/2` does not block writers
  - Fix race condition during compaction
  - Function `get_and_update/3` avoids unnecessary disk writes (including the transaction header) when the value is unchanged
  - Remove caller timeout on `put_and_delete_multi/3` and `put_multi/2`, consistently with the other functions.
  - Remove default GenServer timeouts
  - Fix process (and file descriptor) leak upon compaction
  - Fix `cubdb_file?/1` regexp, making it stricter

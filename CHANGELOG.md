*(Version française : [CHANGELOG_fr.md](CHANGELOG_fr.md))*

# Changelog

One entry per published version, most recent first; "Unreleased" gathers what waits for the next tag.

## v0.1.0 (2026-10-29)

First public version.

- `read`: one row per partition, one column per group, with log start and end; most advanced group, active groups and
  offsets below the log start marked.
- `align --from oldest|newest|earliest|latest|<offset>|<group> --to <group>|all`.
- `delete` (groups × topics × partitions) and `clean` (one reference group kept per topic, refusal with the
  conflicting partitions when ambiguous, `--keep` to decide).
- Dry run by default, `--execute` to write, `--force` to move offsets backward, groups with active members refused
  (classic and KIP-848), CSV backup before every write and `restore`.
- Connection as with the Kafka tools: `--bootstrap-server`, `--command-config` (SASL PLAIN/SCRAM, TLS in PEM or PKCS12).
- Tested on Kafka 3.9 and 4.4.

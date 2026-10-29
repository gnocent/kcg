*(Version française : [README_fr.md](README_fr.md))*

# kcg

[![CI](https://github.com/gnocent/kcg/actions/workflows/ci.yml/badge.svg)](https://github.com/gnocent/kcg/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/gnocent/kcg?include_prereleases)](https://github.com/gnocent/kcg/releases)
[![License](https://img.shields.io/github/license/gnocent/kcg)](LICENSE)

Manage Kafka consumer group offsets without guessing which group is the right one: compare groups side by side, align one
on another, replay from the start or from an offset, and clean up the leftovers. A single static binary (Go, franz-go),
for Kafka 2.6 or later.

## Demo

<p align="center"><img src="demokcg.gif" alt="Animated demo: kcg read shows three billing groups on a topic, billing-v2 the most advanced; align copies billing-v2 onto billing, first as a dry run, then with --execute and a backup; clean refuses because billing and billing-v2 are now identical, then --keep billing-v2 deletes the other two; a final read shows only billing-v2"></p>

> [!NOTE]
> **How this project is made.** I'm not a developer: I write the specifications and check the result; the code is
> written by [Claude Code](https://claude.com/claude-code). What makes up for it: unit tests run with the race detector,
> and an automated acceptance suite of 6 criteria replayed on a Docker test bench with two Kafka versions (3.9 and 4.4,
> KRaft), including a KIP-848 consumer group. This is a **first public version (0.1.0)**, not yet run by many: what was
> checked, and what wasn't, is under [Security](#security).

## Why kcg

`kafka-consumer-groups.sh` works one group at a time. When a consumer was redeployed under a new group name, or several
groups were left behind on the same topic, the questions are about groups *together*: which one is ahead on each
partition, which one should be kept, how to put the others on the same offsets.

- **One table for all groups.** `read` shows one row per partition and one column per group, with the log start and
  end; the most advanced group on each partition is marked, as are active groups and offsets below the log start.
- **Align in one command.** `align` copies offsets from a group, from the minimum or maximum of several groups, from the
  start or end of the log, or from an offset, onto one group or all of them.
- **Clean up without picking the wrong one.** `clean` keeps one reference group per topic and refuses when the choice
  is ambiguous, listing the partitions in conflict.
- **Nothing written by accident.** Every write command is a dry run unless `--execute` is given, refuses groups with
  active members, needs `--force` to move offsets backward, and saves the previous offsets to a CSV file that `restore`
  puts back.

## Installation

Download the archive for your system from the [releases](https://github.com/gnocent/kcg/releases) (Linux amd64,
macOS arm64, Windows amd64; checksums in `SHA256SUMS`), or build it with Go 1.27.3 or later:

```bash
go install github.com/gnocent/kcg@latest
```

## Connection

As with the Kafka tools: `--bootstrap-server host:port[,host:port]` and, when needed, `--command-config client.properties`.
Recognized keys:

- `security.protocol`: PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL;
- `sasl.mechanism` (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512) and `sasl.jaas.config` (`username`, `password`);
- `ssl.truststore.*` and `ssl.keystore.*` in PEM (a file, or inline with `ssl.truststore.certificates`,
  `ssl.keystore.key` and `ssl.keystore.certificate.chain`) or PKCS12. JKS isn't supported: convert it to PKCS12;
- an empty `ssl.endpoint.identification.algorithm` turns off host name verification (the certificate chain is still
  verified).

`--topic` and `--group` take a comma-separated list and the wildcards `*` (any characters, `/` included) and `?` (one
character).

## Commands

| Command | What it does |
|---|---|
| `read` | One row per partition, one column per group. `*` marks the most advanced group, `!` an offset below the log start, `(active)` a group with members. |
| `align --from X --to Y` | Copies offsets. `X` is `oldest` or `newest` (minimum or maximum per partition of the matched groups), `earliest` or `latest` (start or end of the log), an offset (refused if it falls outside the log of a partition), or a group; a number is always an offset, never a group. `Y` is a group, or `all` for every matched group. A partition with no reference offset is skipped. |
| `delete [--partition 0,1]` | Deletes the offsets of the matched groups on the matched topics, on every partition unless `--partition` is given. |
| `clean [--keep G]` | Keeps one group per topic and deletes the others' offsets. Active groups are always kept, with a warning if one of them is behind. When every group is inactive, kcg keeps the most advanced one if it is ahead on every partition; otherwise it refuses and lists the partitions in conflict, and `--keep` decides. |
| `restore <file.csv>` | Puts back the offsets of a backup. |

For writes (`align`, `delete`, `clean`), `--topic` is required, and so is `--group`, except for an `align` from one
named group to another. Use `'*'` to target everything. `kcg <command> -h` lists a command's flags; `kcg -version`
prints the version.

## Write safety

- Without `--execute`, kcg prints the plan and writes nothing.
- A group with active members is refused, classic and KIP-848 alike; Kafka itself rejects offset changes on an active
  classic group.
- Moving an offset backward with `align` needs `--force`. `restore` doesn't, since it undoes.
- Each `--execute` first writes the previous offsets to `kcg-backup-<date>.csv` in the current directory
  (`group,topic,partition,offset`, with an empty offset where there was none). To undo:
  `kcg restore <file> --execute`.
- Deleting an offset is final: on restart, the consumer starts again according to its `auto.offset.reset`.

## Known limitations

- The backup keeps neither the commit metadata nor the leader epoch: `restore` puts the offsets back, not the metadata
  (Kafka Streams stores the partition time there).
- A PKCS12 truststore must mark its certificates as trusted, as `keytool` does. A `.p12` made with `openssl` is refused:
  use a PEM truststore instead.
- Group names are written as they are in the backup CSV. A name starting with `=`, `+`, `-` or `@` may be taken as a
  formula by a spreadsheet.

## Development

On Linux or WSL: `make check` runs vet, the unit tests and the build. The acceptance tests (`make recette`) run on the
[kafkabeat test bench](https://github.com/gnocent/kafkabeat/tree/main/bench): Kafka 3.9 on `localhost:19092` and 4.4
on `localhost:19094`, addresses overridden by `KCG_KAFKA39` and `KCG_KAFKA44`. `make demo` records the demo above
(needs [VHS](https://github.com/charmbracelet/vhs)).

## Security

**What kcg touches**

- **Kafka**: topic metadata, log start and end offsets, the list of consumer groups and their committed offsets. Only
  write commands run with `--execute` write anything: offset commits and offset deletions, for the groups and topics
  shown in the plan. It never reads or writes messages.
- **Disk**: the backup files, in the current directory.
- **Credentials** are read only from the `--command-config` file (keep it `0600`) and are never printed. TLS 1.2 at
  least.

**What was checked, and what wasn't**

- Unit tests with the race detector, `go vet`, `staticcheck` and `govulncheck`; the acceptance suite on the test bench.
  Kafka itself was checked to refuse offset writes on an active classic group, so kcg's own check isn't the only
  barrier.
- The test bench has no security enabled: SASL and TLS (PEM, PKCS12, mTLS) are covered by unit tests of the
  configuration only, not exercised against a secured cluster.
- **No independent external audit.** To report a vulnerability, don't open a public issue: use GitHub's private
  reporting (**Security** tab → **Report a vulnerability**), with the version (`kcg -version`). Acknowledgement within
  about two weeks at best, no commitment on a fix date; only the latest version is supported.

## License

This project is distributed under the **[GNU Affero General Public License v3.0](LICENSE)** (AGPLv3): you're free to use,
study, modify, and redistribute it, provided the source code (including your modifications) remains available under the
same terms — including when the software is exposed over a network.

> **Note of intent (not legally binding)**: the spirit of this project is to remain a community tool, improved
> collectively, not a product resold as-is. The AGPLv3 does not formally forbid commercial use, but that's the use its
> author hopes to see made of it.

## Disclaimer / limitation of liability

kcg is published **as is**, without warranty of any kind, express or implied — including, without limitation, the
warranties of merchantability, fitness for a particular purpose, and non-infringement (see sections 15 through 17 of the
[AGPLv3 license](LICENSE), which govern).

In particular:

- This project is developed and maintained **on its author's free time**, with no commitment to availability,
  maintenance, security patches, or future evolution.
- The author and contributors **decline any responsibility** for the direct or indirect consequences of using this tool —
  including, without limitation, lost or reprocessed messages, data loss, or service interruption.
- kcg changes consumer group offsets with the privileges it is given: **try it on a test cluster first**, and read the
  plan before adding `--execute`. Running it against a production cluster remains the sole responsibility of the person
  doing so.

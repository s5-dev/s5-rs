# vup

**Personal backup, sync, and archive, built on [S5](https://s5.pro).**

`vup` backs up your files to storage you choose — **client-side encrypted before
anything leaves your machine** — with content-addressed deduplication, snapshots
you can roll back to, and recovery from a single offline key. Backends: a local
disk, any S3-compatible bucket, or Sia (decentralized) via an indexd service.

> Status: `1.0.0-beta.2`. Moving fast; expect rough edges.

## Install

`vup` is a single binary. You need a recent **Rust toolchain** (edition 2024 —
Rust ≥ 1.85; install via [rustup](https://rustup.rs)).

**From a local checkout (today):**

```sh
git clone https://github.com/s5-dev/s5-rs
cd s5-rs
cargo install --path vup_cli      # installs the `vup` binary into ~/.cargo/bin
```

**From git (no clone):**

```sh
cargo install --git https://github.com/s5-dev/s5-rs s5_vup
```

**From crates.io** _(coming with the crates.io release)_:

```sh
cargo install s5_vup
```

> The package is `s5_vup`; the installed binary is `vup`. Make sure
> `~/.cargo/bin` is on your `PATH` (rustup sets this up).

_A one-line `curl … | sh` installer with prebuilt binaries is planned._

## Quick start

```sh
vup onboard                                # first-run wizard: keys, store, recovery key
vup backup ~/Documents ~/Photos backup:    # map paths to the `backup` vault + first snapshot
vup history backup:                        # list snapshots
vup restore backup: ./restored             # restore the latest snapshot into ./restored
```

`vup onboard` prints a **recovery key** once. Write it down and keep it offline —
it is the only way to recover your data if you lose this machine.

The CLI talks to a small background daemon that it **starts automatically** on
first use; you don't run anything separately. `vup status` shows what's running,
and `vup --help` lists every command.

## How references work

The grammar is **verb first**. A **trailing colon marks a vault**; a bare token
is always a literal local path.

```
music:            a configured vault           vup backup ~/Music music:
music:Photos      a path inside a vault         vup restore music:Photos ./out
music:#3          the vault at a past snapshot   vup restore music:#3 ./out
@alice            a paired friend               vup grant music: @alice --write
#<hash>           a vault-free snapshot (ro)     self-certifying, no vault context
```

Common verbs: `backup restore list history mount share copy automate join grant
revoke who status doctor tasks config`, plus management namespaces `vault store
device friend service`. System vaults carry a leading underscore (`_config:`,
`_identity:`). The old `+vault <verb>` form still works as a hidden alias through
the beta. Run `vup --help` for the full, authoritative command list.

See [`docs/reference/configuration.md`](../docs/reference/configuration.md) for
the config file format.

## Where things live

| What | Path (Linux; macOS/Windows differ) |
|---|---|
| Config | `~/.config/s5/config.toml` |
| Keys | `~/.config/s5/keys/` |
| Data (store, registry, vault roots) | `~/.local/share/s5/` |
| Logs | `~/.cache/s5/logs/node.log` |

Re-run setup by removing the config file first (`vup onboard` refuses to
overwrite an existing config).

## License

MIT OR Apache-2.0.

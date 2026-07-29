# Ascendium fork of JasperFx/wolverine — NATS interop + upgrade runbook

This is a **private Ascendium fork** of [JasperFx/wolverine](https://github.com/JasperFx/wolverine)
(MIT). Its sole purpose is to add the `UseInterop()` / `InteropWithCloudEvents()` surface to the
**NATS transport** (parity with the Kafka/RabbitMQ transports, which already ship it), and to
publish that one project as **`Ascendium.WolverineFx.Nats`** to `nuget.ascendium.ca`.

Everything else in this repo is stock upstream. We carry **one small patch**, not a rewrite.

## The model (why it's a real fork, not a file-copy)

The prior approach (`AscendiumInc/WolverineFx.Nats`, `AscendiumInc/Wolverine.ComplianceTests`)
copied the NATS source into standalone repos with **no upstream git ancestry**, so every Wolverine
release meant a manual re-port. This repo replaces both: it is a **true GitHub fork** with an
`upstream` remote, and our change lives as commits on the long-lived branch **`ascendium/nats-interop`**.
Upgrading is then a `git rebase`, and the in-repo `Wolverine.ComplianceTests` project is used
directly (no separate ComplianceTests package needed).

- **Branch:** `ascendium/nats-interop` — the published branch. Base of each release = the upstream
  release **tag** it was rebased onto.
- **The patch is 5 commits** (`git log --oneline V<tag>..HEAD`), in rebase order:
  1. `feat(nats): UseInterop + InteropWithCloudEvents interop augmentation` — ~70 lines across 7
     files + 1 new `INatsEnvelopeMapper.cs` + 2 interop test files. **Keep this commit pure** (only
     the interop code) so rebases stay clean.
  2. `chore(fork): Ascendium packaging + single-source nuget.config` — `PackageId` override on
     `Wolverine.Nats.csproj` + a root `nuget.config` pinning a single public source.
  3. `fix(nats): unbreak JetStream — DLQ ISender cast + ephemeral consumer filter` — `NatsEndpoint`
     built its dead-letter sender by casting an `ISendingAgent` to `ISender` (no production
     implementation satisfies it, so *every* listener with a dead-letter subject threw at startup),
     and `JetStreamSubscriber` set the singular `FilterSubject` on an ephemeral consumer, producing a
     malformed JetStream API subject (server err 10131).
  4. `fix(nats): stop silently dropping dead letters, and declare DeadLetterStorage` —
     `MoveToErrorsAsync` gated on JetStream's `NumDelivered`, which an in-process retry policy never
     advances, so it returned having published nothing and the caller then acked the message away.
     Also adds the `DeadLetterStorage` override NATS was the only native-DLQ endpoint to lack.
  5. `chore(fork): AscendiumPatch property for fork-only patch releases` — see
     [Versioning](#versioning).
- **Docs commits** (`docs(fork): …`) update this file and rebase along with the rest.
- **All of it stays inside `src/Transports/NATS/Wolverine.Nats*`.** Fixes that belong in Wolverine
  **core** are proposed upstream and carried nowhere — patching core would break the
  `git rebase --onto` property this whole runbook depends on. (Outstanding: making
  `ISupportDeadLetterQueue.MoveToErrorsAsync` report whether it handled the envelope, so a native
  no-op falls through to durable storage instead of dropping the message — it lives in
  `MessageContext` and would fix this failure class for every transport, not just NATS.)
- **What the augmentation does** (mirrors Kafka's `IKafkaEnvelopeMapper` pattern):
  `NatsEndpoint : Endpoint<INatsEnvelopeMapper, NatsEnvelopeMapper>`; new `INatsEnvelopeMapper`;
  `NatsEnvelopeMapper` implements it; listener/sender use `endpoint.EnvelopeMapper`;
  `Nats{Listener,Subscriber}Configuration` extend the `Interoperable*Configuration<…>` bases;
  `CoreNatsSubscriber` passes header-less messages through **only** when the endpoint's serializer
  is an `IUnwrapsMetadataMessageSerializer` (the CloudEvents mapper) so external CloudEvents
  producers are consumable.

## Versioning

Upstream sets `<Version>` directly in `Directory.Build.props` (no Nerdbank). So the package version
**equals that value** — building the branch off tag `V6.16.0` yields `Ascendium.WolverineFx.Nats
6.16.0`, and the `ProjectReference` to `Wolverine.csproj` packs as a `WolverineFx [6.16.0]`
dependency automatically. **Match our package version to the upstream tag.**

### Fork-only patch releases — use `-p:AscendiumPatch=N`, never `-p:Version=`

When we need to ship a fix against an **unchanged** upstream version, bump only *our* package:

```bash
dotnet pack … -p:AscendiumPatch=1     # publishes 6.24.0.1, depending on WolverineFx 6.24.0
```

`Wolverine.Nats.csproj` carries a conditional `<PackageVersion>$(Version).$(AscendiumPatch)</PackageVersion>`
that only applies when the property is set.

> **Do not use `-p:Version=6.24.0.1`.** This is what the runbook said until 2026-07-29, and it is
> broken. `Version` is a **global** MSBuild property, so it re-versions *every* project in the build —
> including `Wolverine.csproj`. The `ProjectReference` then packs as a dependency on
> `WolverineFx 6.24.0.1`, **a version upstream never published**, so every consumer restore fails with
> NU1102. The bug is invisible in the pack output and only shows up in the `.nuspec` (or at the
> consumer). `AscendiumPatch` is scoped to the one project, so the dependency keeps pointing at the
> real upstream release.
>
> **Always read the `.nuspec` before pushing** — it is the only place this class of mistake surfaces,
> and a pushed version cannot be replaced:
>
> ```bash
> unzip -p artifacts/Ascendium.WolverineFx.Nats.<version>.nupkg '*.nuspec' \
>   | grep -E '<id>|<version>|dependency id="WolverineFx"'
> ```

---

## Upgrade runbook — do this for every new Wolverine release

Say upstream just tagged **`V6.17.0`** and prime-radiant wants it.

### 1. Rebase the patch onto the new tag

```bash
cd /Users/jfmontpetit/Code/wolverine
git fetch upstream --tags
git switch ascendium/nats-interop
git rebase --onto V6.17.0 <previous-base-tag> ascendium/nats-interop
#   e.g. previous base was V6.16.0:  git rebase --onto V6.17.0 V6.16.0 ascendium/nats-interop
```

Conflicts only appear where upstream edited the exact lines our patch touches (the
`Endpoint<,>` / `EnvelopeMapper` / config bases). The 6.5.1 → 6.16.0 rebase had **zero** conflicts;
expect the same or a trivial hunk. Resolve, `git rebase --continue`.

> If the base tag is unknown, `git merge-base ascendium/nats-interop V6.16.0` or read the last
> `<base>` from the commit before ours: `git log --oneline` — the commit under our two is the tag.

### 2. Build + test against a real broker (intranet Docker via SSH socket-tunnel)

```bash
# open the tunnel (background); leave it running
rm -f /tmp/docker-intranet.sock
SSH_ASKPASS_REQUIRE=never ssh -nNT -L /tmp/docker-intranet.sock:/var/run/docker.sock ascendium@intranet &

dotnet build src/Transports/NATS/Wolverine.Nats.Tests/Wolverine.Nats.Tests.csproj

DOCKER_HOST=unix:///tmp/docker-intranet.sock \
TESTCONTAINERS_HOST_OVERRIDE=intranet \
TESTCONTAINERS_RYUK_DISABLED=true \
dotnet test src/Transports/NATS/Wolverine.Nats.Tests/Wolverine.Nats.Tests.csproj -f net10.0
```

Must be green. Baselines: **134/134** at 6.16.0, **166** at 6.24.0.1 — including the CloudEvents
round-trip tests `end_to_end_with_CloudEvents`, the 7 `InteropConfigurationSurfaceTests`, and the 7
`NatsDeadLetter*` tests. The compliance fixtures are the regression guard that our patch didn't break
the transport.

Two environmental gotchas, both of which look like our patch failing and are not:

- **`global_partitioned_sharded_processing` (2 tests) need a real Postgres** on `localhost:5433` —
  they use `Servers.PostgresConnectionString`, not a testcontainer. Without it they fail with
  `Failed to connect to [::1]:5433`. Stand it up on `intranet` and forward the port:

  ```bash
  SSH_ASKPASS_REQUIRE=never ssh ascendium@intranet 'docker run -d --name wolverine-test-pg \
    -p 5433:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
    postgres:17 -c max_connections=500'
  SSH_ASKPASS_REQUIRE=never ssh -f -nNT -L 5433:localhost:5433 ascendium@intranet
  ```

- **One or two `TrackedSession` timeouts (30s) per full run** are load flakes over the SSH tunnel,
  not regressions — it is a *different* test each run and each passes in isolation. Confirm with
  `--filter "FullyQualifiedName~<TheTest>"` before believing a failure. A deterministic break fails
  the same test every time.

### 3. Security-review the patch

The change surface is tiny and identical in shape each release. Review the diff
(`git diff <base-tag>..HEAD -- 'src/Transports/NATS/Wolverine.Nats/**/*.cs'`) and record the
outcome in **prime-radiant's** `Docs/security-reviews.md` ledger (that's the umbrella project).

### 4. Pack + publish

A **normal release** (new upstream version) needs no version flag — `Directory.Build.props` already
carries the rebased tag's `$(Version)`:

```bash
rm -rf ./artifacts
dotnet pack src/Transports/NATS/Wolverine.Nats/Wolverine.Nats.csproj -c Release \
  -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg -o ./artifacts
```

A **fork-only patch** on an unchanged upstream version adds `-p:AscendiumPatch=N` — and *only* that.
See [Versioning](#versioning) for why `-p:Version=` silently produces an unrestorable package:

```bash
dotnet pack src/Transports/NATS/Wolverine.Nats/Wolverine.Nats.csproj -c Release \
  -p:AscendiumPatch=1 -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg -o ./artifacts
```

**Verify the `.nuspec` before pushing.** A pushed version is immutable, and a wrong dependency
version shows up nowhere else — not in the pack output, not at build time, only at consumer restore:

```bash
unzip -p artifacts/Ascendium.WolverineFx.Nats.<version>.nupkg '*.nuspec' \
  | grep -E '<id>|<version>|dependency id="WolverineFx"'
# expect: <id>Ascendium.WolverineFx.Nats</id>
#         <version>6.24.0.1</version>                    <- ours (may carry the patch suffix)
#         dependency id="WolverineFx" version="6.24.0"    <- MUST be a real upstream release
```

Then push:

```bash
API_KEY=$(awk -F'"' '$2 ~ /v3\/index\.json/{print $4; exit}' ~/.nuget/NuGet/NuGet.Config)
dotnet nuget push ./artifacts/Ascendium.WolverineFx.Nats.<version>.nupkg \
  --source https://nuget.ascendium.ca/v3/index.json --api-key "$API_KEY"
dotnet nuget locals http-cache --clear   # so a fresh restore sees the new version
```

### 5. Push the branch (force — history was rewritten by the rebase)

```bash
git push origin ascendium/nats-interop --force-with-lease
```

> Pushing needs the `home-github` SSH host-alias (`git@home-github:AscendiumInc/wolverine.git`);
> the default `github.com` key is read-only, and the 1Password SSH agent may need a desktop approval.

### 6. Realign the whole Critter Stack, not just the Wolverine line

> **A Wolverine bump is a Critter Stack bump.** Wolverine ships in lockstep with JasperFx, Marten,
> Weasel and Polecat, and its assemblies are compiled against *exact* builds of them. A consumer that
> centrally pins any of those (CPM) keeps its old pin, and you get a **binary skew**: the build stays
> **green**, and the failure only appears at runtime as `TypeLoadException: Method '…' does not have
> an implementation` or `FileNotFoundException: Weasel.Storage, Version=…`. Do **not** treat a
> successful `dotnet build` as evidence that the versions are aligned — it isn't.

**Why it bites so hard:** those runtime exceptions are usually thrown *inside a message handler*.
Wolverine catches handler exceptions, logs them and moves the envelope to the error queue, so an
integration test that publishes and waits sees **only a timeout with no cause**. That is exactly how
the 6.16 → 6.24 bump hid a stale `Marten 9.11.0` pin: `WolverineFx.Marten 6.24.0` wants **9.20.0**,
the build was green, and the sole symptom was the Marten golden-path test hanging for 60 s. (The
prime-radiant golden-path tests now publish through a Wolverine **tracked session**, which rethrows
the handler's own exception instead of masking it as a timeout — keep it that way.)

**The floor check.** The fork's own `Directory.Packages.props` at the new tag is the source of truth
for what this Wolverine was actually built against. Diff it against each consumer's pins:

```bash
# what the new Wolverine build itself uses
grep -E 'Include="(JasperFx|Marten|Weasel|Polecat|Npgsql|NATS\.Net)' \
  /Users/jfmontpetit/Code/wolverine/Directory.Packages.props

# what a consumer currently pins
grep -E 'Include="(JasperFx|Marten|Weasel|Polecat|Npgsql|NATS\.Net)' \
  /Users/jfmontpetit/Code/prime-radiant/Directory.Packages.props
```

Cross-check against the declared floors in the packages the consumer actually references — this is
what catches a pin that is merely *stale* rather than outright invalid:

```bash
unzip -p ~/.nuget/packages/wolverinefx.marten/<ver>/wolverinefx.marten.<ver>.nupkg '*.nuspec' \
  | grep 'dependency id'
```

Bring every overlapping pin **up to** what the fork/nuspec says — default to matching it exactly, and
only stay higher where the consumer deliberately rides a newer line (prime-radiant pins `Npgsql` on
the .NET 10 line, above the fork's 9.x; higher is fine, *lower is the trap*). Never mechanically
downgrade a consumer to match the fork.

Then re-run the consumer's full integration suite — not just the build. NuGet raises `NU1109` only
when a pin sits *below a declared floor*; a pin that satisfies the floor but is binary-incompatible
passes restore silently, so the integration tests are the only gate that catches it.

Then, bottom-up:

1. **`wolverinefx-asyncapi`** — bump its `Directory.Packages.props` Wolverine line +
   `Ascendium.WolverineFx.Nats` to the new version, apply the floor check above, build/test, publish
   the `0.1.x` packages.
2. **`prime-radiant`** — bump `Directory.Packages.props` (the whole WolverineFx family +
   `Ascendium.WolverineFx.Nats` + `WolverineFx.AsyncApi*`), apply the floor check above, fix any
   `src/` breakage, full `dotnet test` green **against real infra** (skipped integration tests prove
   nothing — see the runbook in `prime-radiant/spikes/Nats.JetStream.Spike/README.md`), then the
   docs/samples/security lockstep + release.

Known moving parts, 6.16.0 → 6.24.0: `Marten` 9.11.0 → **9.20.0**, `NATS.Net` 2.7.0 → **2.8.2**.

## Exit ramp

If JasperFx accepts the interop hook upstream (a PR adding `UseInterop`/`InteropWithCloudEvents` to
the NATS transport), this fork dissolves: delete the branch and point prime-radiant back at stock
`WolverineFx.Nats`.

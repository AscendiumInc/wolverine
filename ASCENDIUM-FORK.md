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
- **The patch is 2 commits:**
  1. `feat(nats): UseInterop + InteropWithCloudEvents interop augmentation` — ~70 lines across 7
     files + 1 new `INatsEnvelopeMapper.cs` + 2 interop test files. **Keep this commit pure** (only
     the interop code) so rebases stay clean.
  2. `chore(fork): Ascendium packaging + single-source nuget.config` — `PackageId` override on
     `Wolverine.Nats.csproj` + a root `nuget.config` pinning a single public source.
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
dependency automatically. **Match our package version to the upstream tag.** If we ever need a
fork-only patch on the same upstream version, append a suffix (e.g. `6.16.0.1`) via a pack-time
`-p:Version=`.

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

Must be green (the 6.16.0 baseline was **134/134**, including the CloudEvents round-trip tests
`end_to_end_with_CloudEvents` and the 7 `InteropConfigurationSurfaceTests`). The compliance
fixtures are the regression guard that our patch didn't break the transport.

### 3. Security-review the patch

The change surface is tiny and identical in shape each release. Review the diff
(`git diff <base-tag>..HEAD -- 'src/Transports/NATS/Wolverine.Nats/**/*.cs'`) and record the
outcome in **prime-radiant's** `Docs/security-reviews.md` ledger (that's the umbrella project).

### 4. Pack + publish

```bash
dotnet pack src/Transports/NATS/Wolverine.Nats/Wolverine.Nats.csproj -c Release \
  -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg -o ./artifacts
# verify: nuspec <id>=Ascendium.WolverineFx.Nats, <version>=6.17.0, dependency WolverineFx 6.17.0

API_KEY=$(awk -F'"' '$2 ~ /v3\/index\.json/{print $4; exit}' ~/.nuget/NuGet/NuGet.Config)
dotnet nuget push ./artifacts/Ascendium.WolverineFx.Nats.6.17.0.nupkg \
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

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

### 6. Roll the consumers (bottom-up)

1. **`wolverinefx-asyncapi`** — bump its `Directory.Packages.props` Wolverine line +
   `Ascendium.WolverineFx.Nats` to the new version, build/test, publish the `0.1.x` packages.
2. **`prime-radiant`** — bump `Directory.Packages.props` (the whole WolverineFx family +
   `Ascendium.WolverineFx.Nats` + `WolverineFx.AsyncApi*`), resolve transitive floors, fix any
   `src/` breakage, full `dotnet test` green, then the docs/samples/security lockstep + release.

## Exit ramp

If JasperFx accepts the interop hook upstream (a PR adding `UseInterop`/`InteropWithCloudEvents` to
the NATS transport), this fork dissolves: delete the branch and point prime-radiant back at stock
`WolverineFx.Nats`.

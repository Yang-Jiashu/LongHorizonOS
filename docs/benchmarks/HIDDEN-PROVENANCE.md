# Hidden-Provenance Safety Benchmark

Run:

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m lhos.cli.core benchmark hidden-provenance --json
```

This is an offline safety benchmark for the current provenance boundary. It
does **not** claim to discover arbitrary reads performed by unrestricted Python
or third-party tools.

It checks two fail-closed cases:

1. A task declares `workspace://declared.txt` but records no mediated read.
   Coverage is `PARTIAL`, so strict admission denies verification.
2. A task records the declared input and also reports an unidentifiable
   `env://hidden` input. Coverage is `UNKNOWN`, so strict admission denies
   verification. The `audit` policy remains available during migration.

The benchmark therefore demonstrates a safety property:

> Missing or unknown provenance cannot be promoted to strict `VERIFIED`
> progress.

It does not measure model quality, provider price, GPU utilization, or
automatic dependency discovery.

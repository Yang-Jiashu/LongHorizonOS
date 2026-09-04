# Case Profile: Sphinx-11445 invalid run

## Case

```text
instance: sphinx-doc__sphinx-11445
mode: host-native Windows
```

The static arm produced a source patch that passed all 10 target/regression tests,
but the DSH session then hit SenseNova TPM quota before clean completion. The
LHOS arm hit RPM quota before producing a patch.

This run is invalid for performance comparison:

```text
static: provider rate-limit after a passing intermediate patch
LHOS:   provider rate-limit before verification
```

It is retained to show that provider quota and retry behavior must be reported
separately from OS scheduling gains.

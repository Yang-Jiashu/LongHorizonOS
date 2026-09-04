# Case Profile: Flask-4992 failure

## Case

```text
instance: pallets__flask-4992
mode: host-native Windows
```

Both DSH arms reached an implementation and passed the 18 pre-existing config
regression tests, but both used the wrong public parameter spelling:

```python
mode="rb"
```

The public SWE-bench test requires:

```python
text=False
```

Therefore:

```text
static: 1 F2P failed, 18 P2P passed
LHOS:   1 F2P failed, 18 P2P passed
```

This case is a correctness failure, not an efficiency result. It demonstrates
why the external evaluator, not the Agent's natural-language summary, owns
VERIFIED.

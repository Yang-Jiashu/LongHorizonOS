"""Controlled benchmark subsystem (spec sections 22-25).

- ``controlled``: deterministic task generator (22), oracle, scripted env.
- ``modes``: the 8 experiment modes of section 25 as config mappings.
- ``transcript``: the transcript-only baseline runner.
- ``metrics`` / ``scoring``: section 24 metric computation.
- ``runner``: drives (task, mode, seed) cells through the same Runtime.
- ``adapter``: the section 23 BenchmarkAdapter interface.
"""

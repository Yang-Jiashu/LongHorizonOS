# Config Loader Task

## Goal

Add a configuration loading module to the existing SampleApp project.

## Requirements

1. Support loading configuration from JSON files
2. Return a clear error when the config file is missing
3. Return a clear error when the JSON is invalid
4. Migrate the existing `load_settings` function in `app.py` to use the new config module
5. Add public unit tests for the config loader
6. Update the README with usage instructions for the new config module
7. Keep all existing tests passing

## Environment

- Python 3.11
- pytest for testing
- pydantic available
- The project is in `src/sample_app/`

## Tools Available

- filesystem: read, write, list, exists
- shell: command (workspace-confined, no network)

## Verification

- Public tests in `tests_public/` must pass
- Hidden tests (not visible to the runtime) will be run by the external grader
- The external grader checks each requirement independently

## Budget

- Max tokens: 200,000
- Max model calls: 100
- Max tool calls: 100
- Max wall-clock: 60 minutes

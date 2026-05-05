# Contributing

## Requirement-Derived Functional Tests

Functional library test entry points must be derived from repository
requirements. In functional modules, every `#[test]` function must be a
traced `requirement_*` test with exactly one Duvet requirement block, or
a traced `todo_*` test with exactly one Duvet todo block.

Shared helpers, fixtures, generators, and reusable assertions should not
carry Duvet blocks and should not be `#[test]` entry points. Move common
test logic into ordinary helper functions, then call those helpers from
requirement-specific tests.

Mutation and regression tests must map to an existing requirement. If a
regression covers valid behavior that is not specified yet, update the
appropriate spec or policy document first, then cite that requirement
from the test.

Mutation testing is intentionally manual. Do not add `cargo mutants` to
`./scripts/verify.sh`, `./tasks.sh verify`, or `./tasks.sh all`; run
`./tasks.sh mutants` only for an explicit mutation-squashing exercise.

Tooling-only tests that verify repository tooling rather than library
behavior may remain untraced. This exemption includes the
`traceability_audit` checker tests and its CLI harness.

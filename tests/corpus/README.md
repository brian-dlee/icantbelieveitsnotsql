# SQL compatibility corpus

Each child directory is a self-contained butter project with `butter.toml`,
schema files, and a `queries/` directory. The corpus test runs the real
generation pipeline against every case.

- Successful cases keep the complete generated output tree in `expected/`.
- Successful cases may include `expected-warnings.txt`; each non-empty line is
  a substring that must appear in the diagnostics. An empty file requires no
  warnings.
- Cases expected to fail contain `expected-error.txt` with a diagnostic
  substring. No generated output is compared for those cases.

Keep cases small and focused when adding a regression. A single case may
combine SQL features when their interaction is the behavior being pinned down.

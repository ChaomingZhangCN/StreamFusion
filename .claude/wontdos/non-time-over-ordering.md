# Released non-time OVER sort restrictions

Released Flink 2.2.1 and 1.18.1 require one ascending ORDER BY column before translating
an OVER operator. DESC fails with `The window can only be ordered in ASCENDING mode.`;
multiple columns fail with `The window can only be ordered by a single time column.`.
The restriction applies even to ordinary ROW_NUMBER in Flink 2.2, which otherwise admits
ordinary scalar ordering. Flink 1.18 additionally rejects ordinary non-time ordering.

Do not broaden the native matcher based on the capabilities of the retained Top-N sort
comparator. That would turn a released-host planning failure into native execution.
Sixteen SQL checks compare independent host/native root class, message and phase and
require no native rank plan, covering twelve scalar DESC types and four all-ascending
multi-column forms. The existing ascending rank and recovery checks remain controls.

This exclusion concerns released-host-invalid SQL, not a missing StreamFusion kernel.
Revisit only when the project's released Flink dependency admits these query forms;
keep its error contract until then. Current coverage lives in `docs/operators/over.md`.

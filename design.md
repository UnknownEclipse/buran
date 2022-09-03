# Design

The heart of Buran is a copy-on-write B+-Tree. Pages in the primary database file
are virtualized via a separate mapping table that translates page ids into byte ranges.
This means that in order to maintain atomicity, we only need to maintain a small
set of changes in the mapping table, rather than rewriting several primary database
pages.

"""Migration control-flow exceptions.

Kept in their own module because migration files are loaded by path while the
runner may be executing as __main__. Importing the runner from a migration
would execute it a second time under a different module name, producing a
*different* exception class that isinstance checks would miss.
"""

from __future__ import annotations


class MigrationDeferred(Exception):
    """The migration could not complete, but the deploy should continue.

    For work that is not a prerequisite for running the new code — index
    creation, say, where the crawler already degrades to a slower query path.
    The ledger records DEFERRED rather than COMPLETED, so the next run retries
    instead of skipping it forever.

    Raise this only when proceeding is genuinely safe. Anything the new code
    depends on should raise normally and stop the deploy.
    """

    def __init__(self, message: str, remedy: str | None = None):
        super().__init__(message)
        self.remedy = remedy

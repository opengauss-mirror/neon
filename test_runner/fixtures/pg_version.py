from __future__ import annotations

from enum import StrEnum

from typing_extensions import override

"""
This fixture is used to determine which version of Postgres to use for tests.
"""


# Inherit PgVersion from str rather than int to make it easier to pass as a command-line argument
class PgVersion(StrEnum):
    V14 = "14"
    V15 = "15"
    V16 = "16"
    V17 = "17"

    V702 = "V702"
    V703 = "V703"

    # Postgres Version for tests that uses `fixtures.utils.run_only_on_default_postgres`
    DEFAULT = V17

    # Instead of making version an optional parameter in methods, we can use this fake entry
    # to explicitly rely on the default server version (could be different from pg_version fixture value)
    NOT_SET = "<-POSTRGRES VERSION IS NOT SET->"

    # Make it less confusing in logs
    @override
    def __repr__(self) -> str:
        return f"'{self.value}'"

    @override
    def __str__(self) -> str:
        return self.value

    @property
    def is_opengauss(self) -> bool:
        return self in (PgVersion.V702, PgVersion.V703)

    @property
    def neon_local_cli_arg(self) -> str:
        # neon_local currently accepts PostgreSQL major versions. openGauss variants
        # use the PG14-compatible protocol/storage format in the local test setup.
        if self.is_opengauss:
            return "14"
        return self.value

    # In GitHub workflows we use Postgres version with v-prefix (e.g. v14 instead of just 14),
    # sometime we need to do so in tests.
    @property
    def v_prefixed(self) -> str:
        if self.is_opengauss:
            return self.value
        return f"v{self.value}"

    @override
    def __int__(self) -> int:
        if self == PgVersion.NOT_SET:
            raise ValueError("Cannot convert PgVersion.NOT_SET to int")
        return int(self.neon_local_cli_arg)

    def _cmp_key(self) -> int:
        return int(self)

    @override
    def __lt__(self, other: object) -> bool:
        if isinstance(other, PgVersion):
            return self._cmp_key() < other._cmp_key()
        return NotImplemented

    @override
    def __le__(self, other: object) -> bool:
        if isinstance(other, PgVersion):
            return self._cmp_key() <= other._cmp_key()
        return NotImplemented

    @override
    def __gt__(self, other: object) -> bool:
        if isinstance(other, PgVersion):
            return self._cmp_key() > other._cmp_key()
        return NotImplemented

    @override
    def __ge__(self, other: object) -> bool:
        if isinstance(other, PgVersion):
            return self._cmp_key() >= other._cmp_key()
        return NotImplemented

    @classmethod
    @override
    def _missing_(cls, value: object) -> PgVersion | None:
        if not isinstance(value, str):
            return None

        known_values = set(cls.__members__.values())

        # Allow passing version as v-prefixed string (e.g. "v14")
        if value.lower().startswith("v") and (v := value[1:]) in known_values:
            return cls(v)

        # Allow passing version as an int (i.e. both "15" and "150002" matches PgVersion.V15)
        if value.isdigit() and (v := value[:2]) in known_values:
            return cls(v)

        return None

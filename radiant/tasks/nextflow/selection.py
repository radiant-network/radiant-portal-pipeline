"""What the case selections of every pipeline share: the member order, the sequencing types,
the strict-mode error and the excluded case."""

from pydantic import BaseModel

from radiant.tasks.nextflow.problem import Problem

# Order members are emitted in: proband first, then parents, then everyone else.
REL_ORDER = {"proband": 0, "father": 1, "mother": 2, "brother": 3, "sister": 3, "sibling": 3, "fetus": 4}

# The pipeline's `sequencingType` column, uppercase. `wxs` and `wes` are the same thing
# under two dictionaries; the pipeline only knows WES.
SEQUENCING_TYPES = {"wgs": "WGS", "wxs": "WES", "wes": "WES"}


class CaseResolutionError(Exception):
    """One or more requested cases cannot be turned into a valid pipeline input."""


class ExcludedCase(BaseModel):
    """A candidate case that will not be run, and why. `reason` and `detail` are the first
    problem found, as the warnings and the strict-mode error report it; `problems` is every one,
    with the member it is about, for the decision log."""

    case_id: int
    reason: str
    detail: str
    problems: list[Problem] = []

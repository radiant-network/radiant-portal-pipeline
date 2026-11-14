"""What the scheduler decides for an item, and why.

One vocabulary for every pipeline: a `Decision`, and the `Reason` behind it. Each reason code
carries the decision it leads to and the sentence used to report it.
"""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class Decision(StrEnum):
    # Taken by the scheduler.
    RUN = "run"
    WAIT = "wait"
    BLOCKED = "blocked"
    SKIP = "skip"
    # Reported by the pipeline.
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ReasonCode(StrEnum):
    decision: Decision
    label: str

    def __new__(cls, code: str, decision: Decision, label: str):
        obj = str.__new__(cls, code)
        obj._value_ = code
        obj.decision = decision
        obj.label = label
        return obj

    PENDING_SEQUENCING = "pending_sequencing", Decision.WAIT, "has no completed sequencing experiment yet"
    PENDING_ALIGNMENT = (
        "pending_alignment",
        Decision.WAIT,
        "its current sequencing experiment has no alignment task yet",
    )
    MISSING_INPUT = "missing_input", Decision.BLOCKED, "its current alignment published no {detail} document"
    AMBIGUOUS_INPUT = (
        "ambiguous_input",
        Decision.BLOCKED,
        "its current alignment published more than one {detail} document. One task cannot legitimately "
        "produce two, so a document is mistyped at the source -- typically an index recorded with the "
        "format_code of the data it indexes",
    )
    MISSING_ALIQUOT = "missing_aliquot", Decision.BLOCKED, "its sequencing experiment has no aliquot"
    PROBAND_COUNT = "proband_count", Decision.BLOCKED, "the case does not have exactly one proband"
    UNSUPPORTED_SAMPLES = (
        "unsupported_samples",
        Decision.BLOCKED,
        "expected one tumoral sample, plus at most one normal one",
    )
    UNSUPPORTED_STRATEGY = "unsupported_strategy", Decision.BLOCKED, "unsupported experimental strategy {detail}"
    UNSUPPORTED_CALLER = (
        "unsupported_caller",
        Decision.BLOCKED,
        "its alignment caller ({detail}) is not DRAGEN, the only one the pipeline supports -- an unknown "
        "caller means the alignment task has no pipeline_name",
    )
    NO_PROJECT_CODE = (
        "no_project_code",
        Decision.BLOCKED,
        "the case has no project_code -- `cases.project_id` did not resolve to a row in `project`, "
        "and the batch PATCH requires it to look the case up",
    )
    TENANT_EXCLUDED = (
        "tenant_excluded",
        Decision.SKIP,
        "its tenant is in `excluded_tenants` (`$RADIANT_ETL_EXCLUDED_TENANTS`), which the ETL "
        "leaves out of case processing",
    )


class Reason(BaseModel):
    """A reason code, and what it is about (`gvcf`, a caller name...) when the code is shared."""

    model_config = ConfigDict(frozen=True)

    code: ReasonCode
    detail: str = ""

    @property
    def decision(self) -> Decision:
        return self.code.decision

    @property
    def text(self) -> str:
        return self.code.label.format(detail=self.detail or "?")

    def __str__(self) -> str:
        return f"{self.code}:{self.detail}" if self.detail else str(self.code)


# When an item has several reasons, the decision of the first of these that applies.
_PRECEDENCE = (Decision.SKIP, Decision.BLOCKED, Decision.WAIT)


def decision_of(reasons) -> Decision:
    """`run` without a reason; otherwise skip, then blocked, then wait."""
    decisions = {r.decision for r in reasons}
    return next((d for d in _PRECEDENCE if d in decisions), Decision.RUN)


# The fact queries still return one code per document kind; each maps to a shared code.
_QUERY_CODES = {
    "no_gvcf": (ReasonCode.MISSING_INPUT, "gvcf"),
    "no_gcnv": (ReasonCode.MISSING_INPUT, "gcnv"),
    "no_cram": (ReasonCode.MISSING_INPUT, "cram"),
    "no_metrics": (ReasonCode.MISSING_INPUT, "metrics"),
    "no_ssnv": (ReasonCode.MISSING_INPUT, "ssnv"),
    "ambiguous_gvcf": (ReasonCode.AMBIGUOUS_INPUT, "gvcf"),
    "ambiguous_gcnv": (ReasonCode.AMBIGUOUS_INPUT, "gcnv"),
    "ambiguous_cram": (ReasonCode.AMBIGUOUS_INPUT, "cram"),
    "ambiguous_dragen_metrics": (ReasonCode.AMBIGUOUS_INPUT, "metrics"),
    "ambiguous_ssnv": (ReasonCode.AMBIGUOUS_INPUT, "ssnv"),
}


def from_query(code: str) -> Reason:
    """The reason for an `exclusion_reason` returned by a fact query. Raises on an unknown code."""
    if code in _QUERY_CODES:
        reason_code, detail = _QUERY_CODES[code]
        return Reason(code=reason_code, detail=detail)
    return Reason(code=ReasonCode(code))

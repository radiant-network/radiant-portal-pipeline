"""Why a case, or a calling task, cannot run: one `Problem` per cause found.

Shared by the four selections (`resolve.py`, `cnv/`, `qc/`, `somatic/`) and by the decision log
(`decision.py`). Kept apart from both so neither imports the other.
"""

from pydantic import BaseModel


class Problem(BaseModel):
    """One reason a unit cannot run. The member fields are set when the reason is about one
    member (its sequencing, its document, its aliquot) and left empty when it is about the case
    or the task as a whole (`proband_count`, `unsupported_strategy`...)."""

    reason: str
    detail: str
    patient_id: int | None = None
    seq_id: int | None = None
    # The member's current alignment task, when it has one: where the document at fault hangs.
    task_id: int | None = None


def member_problem(member, reason: str, detail: str) -> Problem:
    """A problem about one member of a candidate case (`CaseMember`, `CnvMember`, `QcMember`): the
    patient, its current experiment and the alignment task to look at."""
    return Problem(
        reason=reason,
        detail=detail,
        patient_id=member.patient_id,
        seq_id=member.seq_id,
        task_id=member.alignment_task_id,
    )


def case_problem(reason: str, detail: str) -> Problem:
    """A problem about the case, or the calling task, as a whole: no member to point at."""
    return Problem(reason=reason, detail=detail)

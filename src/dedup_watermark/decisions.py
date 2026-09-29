from enum import Enum


class RecordDecision(Enum):
    """Outcome of running a record through DedupWatermarkProcessor.process()."""

    ACCEPT = "ACCEPT"                # new record, within watermark — process normally
    DUPLICATE = "DUPLICATE"          # key already seen — drop or route to a dup-audit sink
    LATE_DROPPED = "LATE_DROPPED"    # older than the watermark, and late policy is "drop"
    LATE_ACCEPTED = "LATE_ACCEPTED"  # older than the watermark, but late policy is "accept_flagged"

"""Thin wrapper around LineageTracker that pipeline stages use to emit events."""
from typing import Optional
from lineage.tracker import LineageTracker


class LineageEmitter:
    def __init__(self, tracker: LineageTracker, stage: str):
        self._tracker = tracker
        self._stage = stage

    def emit(
        self,
        operation: str,
        data_type: str,
        label: str,
        record_id: Optional[str] = None,
        parent_node_id: Optional[str] = None,
        attributes: Optional[dict] = None,
    ) -> str:
        """Emit a lineage event and return the new node_id."""
        return self._tracker.record_event(
            stage=self._stage,
            operation=operation,
            data_type=data_type,
            label=label,
            record_id=record_id,
            parent_node_id=parent_node_id,
            attributes=attributes,
        )

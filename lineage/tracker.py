import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
from lineage.store import write_lineage_store


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _uid() -> str:
    return str(uuid.uuid4())


@dataclass
class LineageNode:
    node_id: str
    stage: str        # PROVIDER | BRONZE | SILVER | GOLD
    data_type: str    # RAW | INGESTED | VALIDATED | TRANSFORMED | AGGREGATED
    label: str
    record_id: Optional[str]
    attributes: dict
    created_at: str


@dataclass
class LineageEdge:
    edge_id: str
    source_node_id: str
    target_node_id: str
    operation: str    # PRODUCE | INGEST | VALIDATE | TRANSFORM | AGGREGATE


@dataclass
class LineageGraph:
    run_id: str
    started_at: str
    completed_at: Optional[str]
    nodes: list = field(default_factory=list)
    edges: list = field(default_factory=list)


class LineageTracker:
    def __init__(self, run_id: Optional[str] = None):
        self.run_id = run_id or _uid()
        self._graph = LineageGraph(
            run_id=self.run_id,
            started_at=_now(),
            completed_at=None,
        )
        # Maps record_id -> latest node_id for chaining
        self._record_latest_node: dict[str, str] = {}

    def get_latest_node_id(self, record_id: str) -> Optional[str]:
        """Return the most recently emitted node_id for a given record_id, or None."""
        return self._record_latest_node.get(record_id)

    def record_event(
        self,
        stage: str,
        operation: str,
        data_type: str,
        label: str,
        record_id: Optional[str] = None,
        parent_node_id: Optional[str] = None,
        parent_node_ids: Optional[list] = None,
        attributes: Optional[dict] = None,
    ) -> str:
        node_id = _uid()
        node = LineageNode(
            node_id=node_id,
            stage=stage,
            data_type=data_type,
            label=label,
            record_id=record_id,
            attributes=attributes or {},
            created_at=_now(),
        )
        self._graph.nodes.append(asdict(node))

        # Collect all parent node ids
        parents: list[str] = []
        if parent_node_ids:
            parents.extend(parent_node_ids)
        elif parent_node_id:
            parents.append(parent_node_id)
        elif record_id and record_id in self._record_latest_node:
            parents.append(self._record_latest_node[record_id])

        for p in parents:
            edge = LineageEdge(
                edge_id=_uid(),
                source_node_id=p,
                target_node_id=node_id,
                operation=operation,
            )
            self._graph.edges.append(asdict(edge))

        # Update latest node for this record
        if record_id:
            self._record_latest_node[record_id] = node_id

        return node_id

    def complete(self) -> dict:
        self._graph.completed_at = _now()
        graph_dict = asdict(self._graph)
        write_lineage_store(graph_dict)
        return graph_dict

    def get_graph(self) -> dict:
        return asdict(self._graph)

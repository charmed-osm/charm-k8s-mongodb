import socket

from ops.framework import Object, EventBase, EventSource, StoredState, ObjectEvents
from ops.charm import CharmBase, CharmEvents
import logging
import json

logger = logging.getLogger(__name__)
# handler = logging.StreamHandler()
# formatter = logging.Formatter('%(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)


class MembersChangedEvent(EventBase):
    """This event is emitted whenever there is a change in a set of cluster member FQDNs."""


class ReplicaSetConfigured(EventBase):
    """This event is emitted when the replica set is configured"""


class MongoDBClusterEvents(ObjectEvents):
    members_changed = EventSource(MembersChangedEvent)
    replica_set_configured = EventSource(MembersChangedEvent)


class MongoDBCluster(Object):
    on = MongoDBClusterEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._relation = self.framework.model.get_relation(self._relation_name)

        self.state.set_default(fqdns=dict())
        self.framework.observe(
            charm.on[relation_name].relation_changed, self.on_relation_changed
        )
        self.framework.observe(
            charm.on[relation_name].relation_departed, self.on_relation_departed
        )

        self.framework.observe(
            charm.on[relation_name].relation_joined, self.on_relation_joined
        )

        self.framework.observe(self.on.replica_set_configured, self)

    def on_replica_set_configured(self, event):
        if not self.framework.model.unit.is_leader():
            raise RuntimeError("The initial unit of a cluster must also be a leader.")
        info = {"replSetConfigured": True}
        self._relation.data[self.model.app]["info"] = json.dumps(info)

    def on_relation_joined(self, event):
        fqdn = self.get_workload_fqdn()
        if fqdn is None or "-endpoints" not in fqdn:
            event.defer()
            return
        self.state.fqdns[self.model.unit.name] = fqdn
        event.relation.data[self.model.unit]["fqdn"] = fqdn

    def on_relation_changed(self, event):
        if event.unit:
            fqdn = event.relation.data[event.unit].get("fqdn")
            exists = self.state.fqdns.get(event.unit.name, None)
            # Bug 240: https://github.com/canonical/operator/issues/240
            # It will never enter here due to this ^ bug, because the state is shared.
            if fqdn and not exists:
                self.state.fqdns[event.unit.name] = fqdn
                self.on.members_changed.emit()
            # Bug 240 Workaround: Always emit the members_changed event.
            self.on.members_changed.emit()

    def on_relation_departed(self, event):
        if event.unit and event.unit.name in self.state.fqdns:
            self.state.fqdns.pop(event.unit.name)
            self.on.members_changed.emit()

    @property
    def is_joined(self):
        return self._relation is not None

    @property
    def fqdns(self):
        if self.model.unit.name not in self.state.fqdns:
            fqdn = self.get_workload_fqdn()
            self.state.fqdns[self.model.unit.name] = fqdn
        return self.state.fqdns

    @property
    def replSetConfigured(self):
        if not self.is_joined:
            return
        info = self._relation.data[self.model.app].get("info")
        if info:
            return json.loads(info).get("replSetConfigured")

    def get_workload_fqdn(self):
        try:
            pod_addr = self.model.get_binding(self._relation_name).network.bind_address
            if not pod_addr:
                return
            addr = socket.getnameinfo((str(pod_addr), 0), socket.NI_NAMEREQD)[0]
            return addr
        except Exception:
            return

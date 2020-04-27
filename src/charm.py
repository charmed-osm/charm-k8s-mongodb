#!/usr/bin/env python3

import sys

sys.path.append("lib")

from ops.charm import CharmBase
from ops.framework import StoredState
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
    ModelError,
)

import subprocess
import logging
from mongodb_cluster import MongoDBCluster

try:
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure
except ImportError:
    subprocess.run("python3 -m pip install pymongo", shell=True)
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure

logger = logging.getLogger(__name__)


class MongoCharm(CharmBase):
    state = StoredState()

    def __init__(self, framework, key):
        super().__init__(framework, key)

        # Observe Charm related events
        for event in (
            # Charm events
            self.on.config_changed,
            self.on.start,
            self.on.upgrade_charm,
        ):
            self.framework.observe(event, self)

        # Initialize peer relation
        self.peers = MongoDBCluster(self, "cluster")
        self.framework.observe(self.peers.on.members_changed, self.on_members_changed)

    def on_members_changed(self, event):
        config = self.framework.model.config
        logger.debug("Members changed")
        if self.framework.model.unit.is_leader():
            logger.debug(self.peers.replSetConfigured)
            if not self.peers.replSetConfigured and config["replica-set"]:
                fqdns = self.peers.fqdns
                try:
                    client = MongoClient(fqdns[self.framework.model.unit.name])
                    # Commands to initialize
                    config = {
                        "_id": config["replica-set-name"],
                        "members": [
                            {"_id": i, "host": fqdns[unit]} for i, unit in enumerate(fqdns)
                        ],
                    }
                    client.admin.command("replSetInitiate", config)
                except Exception:
                    pass
                finally:
                    # Set replica set configured
                    self.peers.on.replica_set_configured.emit()
                # client = MongoClient('mongodb://10.1.47.58:27017,10.1.47.59:27017/?replicaSet=rs0').

    def _apply_spec(self, spec):
        # Only apply the spec if this unit is a leader.
        if self.framework.model.unit.is_leader():
            self.framework.model.pod.set_spec(spec)
            self.state.spec = spec

    def make_pod_spec(self):
        config = self.framework.model.config

        ports = [{"name": "mongo", "containerPort": config["port"], "protocol": "TCP"}]
        command = [
            "mongod",
            "--bind_ip",
            "0.0.0.0",
            "--port",
            "{}".format(config["port"]),
        ]
        if config["replica-set"]:
            command.append("--replSet")
            command.append("{}".format(config["replica-set-name"]))
        kubernetes = {
            "readinessProbe": {
                "tcpSocket": {"port": config["port"]},
                "timeoutSeconds": 5,
                "periodSeconds": 5,
                "initialDelaySeconds": 10,
            },
            "livenessProbe": {
                "exec": {
                    "command": [
                        "/bin/sh",
                        "-c",
                        'mongo --port {} --eval "rs.status()" | grep -vq "REMOVED"'.format(
                            config["port"]
                        ),
                    ]
                },
                "timeoutSeconds": 5,
                "initialDelaySeconds": 45,
            },
        }
        spec = {
            "version": 2,
            "serviceAccount": {
                "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["list"]}]
            },
            "containers": [
                {
                    "name": self.framework.model.app.name,
                    "image": config["image"],
                    "ports": ports,
                    "command": command,
                    "kubernetes": kubernetes,
                }
            ],
        }
        return spec

    def on_config_changed(self, event):
        """Handle changes in configuration"""
        unit = self.model.unit

        new_spec = self.make_pod_spec()
        if self.state.spec != new_spec:
            unit.status = MaintenanceStatus("Applying new pod spec")

            self._apply_spec(new_spec)

            unit.status = ActiveStatus()

    def on_start(self, event):
        """Called when the charm is being installed"""
        unit = self.model.unit

        unit.status = MaintenanceStatus("Applying pod spec")

        new_pod_spec = self.make_pod_spec()
        self._apply_spec(new_pod_spec)

        unit.status = ActiveStatus()

    def on_upgrade_charm(self, event):
        """Upgrade the charm."""
        unit = self.model.unit

        # Mark the unit as under Maintenance.
        unit.status = MaintenanceStatus("Upgrading charm")

        self.on_start(event)

        # When maintenance is done, return to an Active state
        unit.status = ActiveStatus()


if __name__ == "__main__":
    main(MongoCharm)

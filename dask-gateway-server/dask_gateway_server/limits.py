from traitlets import Float, Integer
from traitlets.config import LoggingConfigurable

from .utils import MemoryLimit, format_bytes


class UserLimits(LoggingConfigurable):
    """Manages restrictions for user resource usage"""

    max_cores = Float(
        0,
        help="""
        The maximum number of cores available for each user.

        Set to 0 for no limit (default).
        """,
        min=0.0,
        config=True,
    )

    max_memory = MemoryLimit(
        0,
        help="""
        The maximum amount of memory (in bytes) available to each user.
        Can be an integer, or a string with one of the following suffixes:

        - K -> Kibibytes
        - M -> Mebibytes
        - G -> Gibibytes
        - T -> Tebibytes

        Set to 0 for no limit (default).
        """,
        config=True,
    )

    max_clusters = Integer(
        0,
        help="""
        The maximum number of clusters available for each user.

        Set to 0 for no limit (default).
        """,
        min=0,
        config=True,
    )

    @staticmethod
    def _sum_resources(active_clusters):
        memory = 0
        cores = 0
        for cluster in active_clusters:
            cores += cluster.cores
            memory += cluster.memory
            for worker in cluster.active_workers():
                cores += worker.cores
                memory += worker.memory
        return memory, cores

    def check_cluster_limits(self, user, memory, cores):
        if not (self.max_cores or self.max_memory or self.max_clusters):
            return True, None

        active_clusters = list(user.active_clusters())

        if self.max_clusters and len(active_clusters) >= self.max_clusters:
            msg = (
                "Cannot start new cluster, would exceed user limit of %d active clusters."
                % self.max_clusters
            )
            self.log.info(msg)
            return False, msg

        active_memory, active_cores = self._sum_resources(active_clusters)

        if self.max_cores and self.max_cores - active_cores < cores:
            msg = (
                "Cannot start a new cluster, would exceed user cores limit of %s"
                % self.max_cores
            )
            self.log.info(msg)
            return False, msg

        if self.max_memory and self.max_memory - active_memory < memory:
            msg = (
                "Cannot start a new cluster, would exceed user memory limit of %s"
                % format_bytes(self.max_memory)
            )
            self.log.info(msg)
            return False, msg

        return True, None

    def check_scale_limits(self, cluster, n, memory, cores):
        if not (self.max_cores or self.max_memory):
            return n, None

        active_memory, active_cores = self._sum_resources(
            cluster.user.active_clusters()
        )

        if self.max_memory:
            available_memory = self.max_memory - active_memory
            n_by_memory = int(available_memory / memory)
        else:
            n_by_memory = n

        if self.max_cores:
            available_cores = self.max_cores - active_cores
            n_by_cores = int(available_cores / cores)
        else:
            n_by_cores = n

        n_allowed = min(n, n_by_cores, n_by_memory)

        if n_allowed < n:
            if n_by_cores < n_by_memory:
                # Bound by cores
                msg = (
                    "Adding %d workers to cluster %s would exceed user cores "
                    "limit of %s, adding %d workers instead."
                    % (n, cluster.name, self.max_cores, n_allowed)
                )
            else:
                # Bound by memory
                msg = (
                    "Adding %d workers to cluster %s would exceed user memory "
                    "limit of %s, adding %d workers instead."
                    % (n, cluster.name, format_bytes(self.max_cores), n_allowed)
                )
            self.log.info(msg)
        else:
            msg = None

        return n_allowed, msg

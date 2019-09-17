import pytest

from dask_gateway_server import objects
from dask_gateway_server.limits import UserLimits


GiB = 2 ** 30


@pytest.fixture
async def db():
    """Initializes user alice with the following active resources:
    - 1 cluster
    - 5 cores
    - 3 GiB memory
    """
    db = objects.DataManager()
    db.load_database_state()

    alice = db.get_or_create_user("alice")

    # Create one stopped cluster
    c = db.create_cluster(alice, {}, memory=GiB, cores=1)
    for _ in range(2):
        w = db.create_worker(c, memory=GiB, cores=2)
        db.update_worker(
            w, status=objects.WorkerStatus.STOPPED, stop_time=objects.timestamp()
        )
        db.update_cluster(
            c, status=objects.ClusterStatus.STOPPED, stop_time=objects.timestamp()
        )

    # Create one active cluster with 2 active workers, 1 stopped worker
    c = db.create_cluster(alice, {}, memory=GiB, cores=1)
    for i in range(3):
        w = db.create_worker(c, memory=GiB, cores=2)
        if i == 0:
            db.update_worker(
                w, status=objects.WorkerStatus.STOPPED, stop_time=objects.timestamp()
            )
    return db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,params,warning",
    [
        ({"max_clusters": 2}, (GiB, 1), "active clusters"),
        ({"max_cores": 7}, (GiB, 2), "user cores limit"),
        ({"max_memory": "5 G"}, (2 * GiB, 2), "user memory limit"),
    ],
)
async def test_check_cluster_limits(kwargs, params, warning, db):
    alice = db.get_or_create_user("alice")

    limits = UserLimits(**kwargs)

    # Can create cluster
    allowed, msg = limits.check_cluster_limits(alice, GiB, 1)
    assert allowed
    assert msg is None

    # Create cluster
    db.create_cluster(alice, {}, memory=GiB, cores=1)

    # Cannot create cluster, limit triggered
    allowed, msg = limits.check_cluster_limits(alice, *params)
    assert not allowed
    assert warning in msg

    # With no limit, can create a new cluster
    limits = UserLimits()
    allowed, msg = limits.check_cluster_limits(alice, *params)
    assert allowed
    assert msg is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs,warning",
    [
        ({"max_cores": 11}, "user cores limit"),
        ({"max_memory": "9 G"}, "user memory limit"),
    ],
)
async def test_check_scale_limits(kwargs, warning, db):
    alice = db.get_or_create_user("alice")

    limits = UserLimits(**kwargs)

    # Can create cluster
    allowed, msg = limits.check_cluster_limits(alice, GiB, 1)
    assert allowed
    assert msg is None

    # Create cluster
    cluster = db.create_cluster(alice, {}, memory=GiB, cores=1)

    # Can scale cluster to 2
    n_allowed, msg = limits.check_scale_limits(cluster, 2, 2 * GiB, 2)
    assert n_allowed == 2
    assert msg is None

    # Cannot scale cluster to 3
    n_allowed, msg = limits.check_scale_limits(cluster, 3, 2 * GiB, 2)
    assert n_allowed == 2
    assert warning in msg

    # With no limit, can scale to 3
    limits = UserLimits()
    n_allowed, msg = limits.check_scale_limits(cluster, 3, 2 * GiB, 2)
    assert n_allowed == 3
    assert msg is None

import time
from collections import defaultdict

import pytest

from dask_gateway_server import objects


def check_consistency(db):
    users = db.db.execute(objects.users.select()).fetchall()
    clusters = db.db.execute(objects.clusters.select()).fetchall()
    workers = db.db.execute(objects.workers.select()).fetchall()

    # Check user state
    id_to_user = {}
    for u in users:
        user = db.username_to_user[u.name]
        assert db.cookie_to_user[u.cookie] is user
        id_to_user[u.id] = user
    assert len(db.cookie_to_user) == len(users)
    assert len(db.username_to_user) == len(users)

    # Check cluster state
    for c in clusters:
        cluster = db.id_to_cluster[c.id]
        assert db.token_to_cluster[c.token] is cluster
        user = id_to_user[c.user_id]
        assert user.clusters[c.name] is cluster
    assert len(db.id_to_cluster) == len(clusters)
    assert len(db.token_to_cluster) == len(clusters)

    # Check worker state
    cluster_to_workers = defaultdict(set)
    for w in workers:
        cluster = db.id_to_cluster[w.cluster_id]
        cluster_to_workers[cluster.name].add(w.name)
    for cluster in db.id_to_cluster.values():
        expected = cluster_to_workers[cluster.name]
        assert set(cluster.workers) == expected


@pytest.mark.asyncio
async def test_cleanup_expired_clusters(monkeypatch):
    db = objects.DataManager()

    alice = db.get_or_create_user("alice")

    current_time = time.time()

    def mytime():
        nonlocal current_time
        current_time += 0.5
        return current_time

    monkeypatch.setattr(time, "time", mytime)

    def add_cluster(stop=True):
        c = db.create_cluster(alice)
        for _ in range(5):
            w = db.create_worker(c)
            if stop:
                db.update_worker(
                    w,
                    status=objects.WorkerStatus.STOPPED,
                    stop_time=objects.timestamp(),
                )
        if stop:
            db.update_cluster(
                c, status=objects.ClusterStatus.STOPPED, stop_time=objects.timestamp()
            )
        return c

    add_cluster(stop=True)  # c1
    add_cluster(stop=True)  # c2
    c3 = add_cluster(stop=False)

    cutoff = mytime()

    c4 = add_cluster(stop=True)
    c5 = add_cluster(stop=False)

    check_consistency(db)

    # Set time to always return same value
    now = mytime()
    monkeypatch.setattr(time, "time", lambda: now)

    # 2 clusters are expired
    max_age = now - cutoff
    n = db.cleanup_expired(max_age)
    assert n == 2

    check_consistency(db)

    # c3, c4, c5 are all that remains
    assert set(db.id_to_cluster) == {c3.id, c4.id, c5.id}

    # Running again expires no clusters
    max_age = now - cutoff
    n = db.cleanup_expired(max_age)
    assert n == 0

    check_consistency(db)

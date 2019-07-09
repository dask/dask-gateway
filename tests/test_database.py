import base64
import time
from collections import defaultdict

import pytest
from cryptography.fernet import Fernet

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
        assert db.token_to_cluster[db.decode_token(c.token)] is cluster
        assert db.name_to_cluster[c.name] is cluster
        user = id_to_user[c.user_id]
        assert user.clusters[c.name] is cluster
    assert len(db.id_to_cluster) == len(clusters)
    assert len(db.name_to_cluster) == len(clusters)
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
    db.load_database_state()

    alice = db.get_or_create_user("alice")

    current_time = time.time()

    def mytime():
        nonlocal current_time
        current_time += 0.5
        return current_time

    monkeypatch.setattr(time, "time", mytime)

    def add_cluster(stop=True):
        c = db.create_cluster(alice, {})
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


@pytest.mark.asyncio
async def test_encryption(tmpdir):
    db_url = "sqlite:///%s" % tmpdir.join("dask_gateway.sqlite")
    encrypt_keys = [Fernet.generate_key() for i in range(3)]
    db = objects.DataManager(url=db_url, encrypt_keys=encrypt_keys)
    db.load_database_state()

    assert db.fernet is not None

    data = b"my secret data"
    encrypted = db.encrypt(data)
    assert encrypted != data

    data2 = db.decrypt(encrypted)
    assert data == data2

    alice = db.get_or_create_user("alice")
    c = db.create_cluster(alice, {})
    assert c.tls_cert is not None
    assert c.tls_key is not None

    # Check database state is encrypted
    with db.db.begin() as conn:
        res = conn.execute(
            objects.clusters.select(objects.clusters.c.id == c.id)
        ).fetchone()
    assert res.tls_credentials != b";".join((c.tls_cert, c.tls_key))
    cert, key = db.decrypt(res.tls_credentials).split(b";")
    token = db.decrypt(res.token).decode()
    assert cert == c.tls_cert
    assert key == c.tls_key
    assert token == c.token

    # Check can reload database with keys
    db2 = objects.DataManager(url=db_url, encrypt_keys=encrypt_keys)
    db2.load_database_state()
    c2 = db2.id_to_cluster[c.id]
    assert c2.tls_cert == c.tls_cert
    assert c2.tls_key == c.tls_key
    assert c2.token == c.token


def test_normalize_encrypt_key():
    key = Fernet.generate_key()
    # b64 bytes
    assert objects.normalize_encrypt_key(key) == key
    # b64 string
    assert objects.normalize_encrypt_key(key.decode()) == key
    # raw bytes
    raw = base64.urlsafe_b64decode(key)
    assert objects.normalize_encrypt_key(raw) == key

    # Too short
    with pytest.raises(ValueError) as exc:
        objects.normalize_encrypt_key(b"abcde")
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)

    # Too short decoded
    with pytest.raises(ValueError) as exc:
        objects.normalize_encrypt_key(b"\x00" * 43 + b"=")
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)

    # Invalid b64 encode
    with pytest.raises(ValueError) as exc:
        objects.normalize_encrypt_key(b"=" + b"a" * 43)
    assert "DASK_GATEWAY_ENCRYPT_KEYS" in str(exc.value)

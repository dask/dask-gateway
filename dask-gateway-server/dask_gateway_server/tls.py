from datetime import datetime, timedelta

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def new_keypair(sni):
    """Create a new self-signed certificate & key pair with the given SNI.

    Parameters
    ----------
    sni : str
        The SNI name to use.

    Returns
    -------
    cert_bytes : bytes
    key_bytes :  bytes
    """
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    dask_internal = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "dask-internal")]
    )
    altnames = x509.SubjectAlternativeName(
        [
            x509.DNSName(sni),
            x509.DNSName("dask-internal"),
            # allow skein appmaster and dask to share credentials
            x509.DNSName("skein-internal"),
        ]
    )
    now = datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(dask_internal)
        .issuer_name(dask_internal)
        .add_extension(altnames, critical=False)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )

    cert_bytes = cert.public_bytes(serialization.Encoding.PEM)

    return cert_bytes, key_bytes

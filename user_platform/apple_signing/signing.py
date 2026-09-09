"""签名材料密码学助手（移植自 iPASide signing.py，只保留 p12 生成——不签 IPA）。

生成发给 Apple 换开发证书的 RSA 密钥对 + PKCS#10 CSR，并把结果证书 + 本地私钥
组装成 PKCS#12——节点侧 go-codesign 重签 WDA 用。Apple 看不到私钥；它只存在于
服务端 PG secret_data。

zsign / IPA 签名 / dylib 注入不在本模块：节点有 go-codesign，服务端只物化材料。
"""

from __future__ import annotations

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from .errors import SigningError

P12_PASSWORD = "iPASide"


def generate_key_and_csr(common_name: str = "iPASide") -> tuple[bytes, str]:
    """返回 (private_key_pem, csr_pem)：全新 RSA-2048 开发密钥。"""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode()
    return key_pem, csr_pem


def build_p12(cert_der: bytes, key_pem: bytes, password: str = P12_PASSWORD) -> bytes:
    """DER 证书 + PEM 私钥 → PKCS#12（节点重签消费）。"""
    try:
        key = serialization.load_pem_private_key(key_pem, password=None)
        cert = x509.load_der_x509_certificate(cert_der)
    except Exception as exc:  # noqa: BLE001 — 缓存的密钥/证书损坏
        raise SigningError(f"缓存的签名证书材料无法解析: {exc}") from exc
    encryption = (
        serialization.BestAvailableEncryption(password.encode())
        if password
        else serialization.NoEncryption()
    )
    return pkcs12.serialize_key_and_certificates(b"iPASide", key, cert, None, encryption)

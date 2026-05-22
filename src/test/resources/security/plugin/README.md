# Test Certificates

Self-signed certificates used for integration testing with the security plugin.

## Certificate Details

| File | Description | Subject |
|---|---|---|
| `root-ca.pem` | Root CA certificate | `C=de, L=test, O=node, OU=node, CN=root` |
| `esnode.pem` | Node certificate (transport + HTTP) | `C=de, L=test, O=node, OU=node, CN=node-0.example.com` |
| `esnode-key.pem` | Node private key (PKCS8, RSA 2048) | — |
| `kirk.pem` | Admin client certificate | `C=de, L=test, O=client, OU=client, CN=kirk` |
| `kirk-key.pem` | Admin client private key (PKCS8, RSA 2048) | — |

The `esnode.pem` certificate includes the following Subject Alternative Names:
- `DNS:node-0.example.com`
- `DNS:localhost`
- `IP:127.0.0.1`
- `IP:::1` (IPv6 localhost)
- `RID:1.2.3.4.5.5`

The admin DN configured in `opensearch.yml` must match the kirk certificate subject:
```yaml
plugins.security.authcz.admin_dn: ["CN=kirk,OU=client,O=client,L=test,C=de"]
```

## How to Regenerate

```bash
cd src/test/resources/security/plugin

# Generate Root CA
openssl genrsa -out root-ca-key.pem 2048
openssl req -new -x509 -sha256 -key root-ca-key.pem \
  -subj "/C=de/L=test/O=node/OU=node/CN=root" \
  -out root-ca.pem -days 3650

# Generate node certificate with SANs (including IPv6 localhost)
openssl genrsa -out esnode-key.pem 2048
openssl req -new -key esnode-key.pem \
  -subj "/C=de/L=test/O=node/OU=node/CN=node-0.example.com" \
  -out esnode.csr
echo "subjectAltName=DNS:node-0.example.com,DNS:localhost,IP:127.0.0.1,IP:::1,RID:1.2.3.4.5.5" > esnode-ext.cnf
openssl x509 -req -in esnode.csr -CA root-ca.pem -CAkey root-ca-key.pem \
  -CAcreateserial -out esnode.pem -days 3650 -sha256 -extfile esnode-ext.cnf

# Generate kirk admin certificate
openssl genrsa -out kirk-key.pem 2048
openssl req -new -key kirk-key.pem \
  -subj "/C=de/L=test/O=client/OU=client/CN=kirk" \
  -out kirk.csr
openssl x509 -req -in kirk.csr -CA root-ca.pem -CAkey root-ca-key.pem \
  -CAcreateserial -out kirk.pem -days 3650 -sha256

# Cleanup temporary files
rm -f *.csr *.srl root-ca-key.pem esnode-ext.cnf
```

## References

- [OpenSearch self-signed certificate documentation](https://opensearch.org/docs/latest/security/configuration/generate-certificates/)
- [Security plugin DEVELOPER_GUIDE.md](https://github.com/opensearch-project/security/blob/main/DEVELOPER_GUIDE.md)

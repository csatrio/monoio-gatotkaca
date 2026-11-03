Demo certificates for the TLS tests, valid until 2032. Not secret: never use them outside tests.

- `rootCA.crt`: self-signed test CA.
- `server.crt`: server certificate for `monoio.rs`, signed by the CA.
- `server.key` (PKCS#1) and `server.pkcs8` (PKCS#8): the server private key.

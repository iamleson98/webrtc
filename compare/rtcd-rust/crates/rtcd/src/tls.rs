//! Serving the API over TLS.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, warn};

/// A handshake that takes longer is abandoned.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Loads a certificate chain and private key (PEM) into a TLS acceptor for TLS 1.2 and later.
pub fn acceptor(cert_file: &Path, key_file: &Path) -> io::Result<TlsAcceptor> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut io::BufReader::new(std::fs::File::open(cert_file)?))
            .collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no certificate in the certificate file",
        ));
    }
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut io::BufReader::new(std::fs::File::open(key_file)?))?
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "no private key in the key file")
            })?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Accepts connections on `listener`, speaks TLS on them and serves `router`, until `stop` turns
/// true. Connections already open are left to finish.
pub async fn serve(
    listener: TcpListener,
    tls: TlsAcceptor,
    router: Router,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let (stream, peer): (_, SocketAddr) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(a) => a,
                Err(err) => {
                    warn!(%err, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            _ = stop.changed() => return,
        };
        let (tls, router) = (tls.clone(), router.clone());
        tokio::spawn(async move {
            let Ok(Ok(stream)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await
            else {
                debug!(%peer, "tls handshake failed");
                return;
            };
            let service = TowerToHyperService::new(router);
            if let Err(err) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                debug!(%peer, %err, "connection ended with an error");
            }
        });
    }
}

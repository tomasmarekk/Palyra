use std::{
    net::IpAddr,
    time::{Duration, SystemTime},
};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};

use crate::{
    error::{IdentityError, IdentityResult},
    unix_ms,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IssuedCertificate {
    pub sequence: u64,
    pub subject: String,
    pub certificate_pem: String,
    #[serde(skip_serializing, skip_deserializing, default)]
    pub private_key_pem: String,
    pub issued_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredCertificateAuthority {
    pub certificate_pem: String,
    pub private_key_pem: String,
    pub sequence: u64,
}

pub struct CertificateAuthority {
    pub certificate_pem: String,
    key_pair: KeyPair,
    sequence: u64,
}

impl CertificateAuthority {
    pub fn new(common_name: &str) -> IdentityResult<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new())
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, common_name);
        params.distinguished_name = distinguished_name;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];

        let key_pair =
            KeyPair::generate().map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let certificate = params
            .self_signed(&key_pair)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let certificate_pem = certificate.pem();

        Ok(Self { certificate_pem, key_pair, sequence: 0 })
    }

    pub fn issue_client_certificate(
        &mut self,
        device_id: &str,
        identity_fingerprint: &str,
        validity: Duration,
    ) -> IdentityResult<IssuedCertificate> {
        let mut params = CertificateParams::new(Vec::<String>::new())
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, format!("Palyra Device {device_id}"));
        distinguished_name.push(DnType::OrganizationName, "Palyra");
        params.distinguished_name = distinguished_name;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.custom_extensions.push(rcgen::CustomExtension::from_oid_content(
            &[1, 3, 6, 1, 4, 1, 53594, 5, 1],
            identity_fingerprint.as_bytes().to_vec(),
        ));

        self.issue_leaf_certificate(params, validity, format!("device:{device_id}"))
    }

    pub fn issue_server_certificate(
        &mut self,
        common_name: &str,
        validity: Duration,
    ) -> IdentityResult<IssuedCertificate> {
        self.issue_server_certificate_with_sans(common_name, validity, &[], &[])
    }

    pub fn issue_server_certificate_with_sans(
        &mut self,
        common_name: &str,
        validity: Duration,
        additional_dns_names: &[String],
        additional_ip_addresses: &[IpAddr],
    ) -> IdentityResult<IssuedCertificate> {
        let mut dns_names = Vec::with_capacity(2 + additional_dns_names.len());
        push_unique_dns_name(&mut dns_names, "localhost");
        push_unique_dns_name(&mut dns_names, common_name);
        for dns_name in additional_dns_names {
            push_unique_dns_name(&mut dns_names, dns_name);
        }

        let mut params = CertificateParams::new(dns_names)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        for ip_address in additional_ip_addresses {
            if !params.subject_alt_names.iter().any(
                |entry| matches!(entry, SanType::IpAddress(existing) if existing == ip_address),
            ) {
                params.subject_alt_names.push(SanType::IpAddress(*ip_address));
            }
        }
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, common_name);
        params.distinguished_name = distinguished_name;
        self.issue_leaf_certificate(params, validity, format!("server:{common_name}"))
    }

    pub fn from_stored(state: &StoredCertificateAuthority) -> IdentityResult<Self> {
        let key_pair = KeyPair::from_pem(&state.private_key_pem)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        rcgen::Issuer::from_ca_cert_pem(&state.certificate_pem, &key_pair)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;

        Ok(Self {
            certificate_pem: state.certificate_pem.clone(),
            key_pair,
            sequence: state.sequence,
        })
    }

    #[must_use]
    pub fn to_stored(&self) -> StoredCertificateAuthority {
        StoredCertificateAuthority {
            certificate_pem: self.certificate_pem.clone(),
            private_key_pem: self.key_pair.serialize_pem(),
            sequence: self.sequence,
        }
    }

    fn issue_leaf_certificate(
        &mut self,
        mut params: CertificateParams,
        validity: Duration,
        subject: String,
    ) -> IdentityResult<IssuedCertificate> {
        let now = SystemTime::now();
        let expires_at_time = now + validity;
        params.not_before = now.into();
        params.not_after = expires_at_time.into();
        let key_pair =
            KeyPair::generate().map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let issuer = rcgen::Issuer::from_ca_cert_pem(&self.certificate_pem, &self.key_pair)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let certificate = params
            .signed_by(&key_pair, &issuer)
            .map_err(|error| IdentityError::Cryptographic(error.to_string()))?;
        let certificate_pem = certificate.pem();
        let private_key_pem = key_pair.serialize_pem();

        self.sequence = self.sequence.saturating_add(1);
        let issued_at = unix_ms(now)?;
        let expires_at = unix_ms(expires_at_time)?;

        Ok(IssuedCertificate {
            sequence: self.sequence,
            subject,
            certificate_pem,
            private_key_pem,
            issued_at_unix_ms: issued_at,
            expires_at_unix_ms: expires_at,
        })
    }
}

fn push_unique_dns_name(target: &mut Vec<String>, candidate: &str) {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        return;
    }
    if target.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
        return;
    }
    target.push(trimmed.to_owned());
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use x509_parser::{
        extensions::GeneralName,
        pem::parse_x509_pem,
        prelude::{FromDer, X509Certificate},
    };

    use super::CertificateAuthority;

    #[test]
    fn issued_certificate_validity_matches_requested_window() {
        let mut ca = CertificateAuthority::new("Palyra Test CA").expect("CA should initialize");
        let validity = Duration::from_secs(3_600);

        let issued = ca
            .issue_client_certificate(
                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                validity,
            )
            .expect("certificate issuance should succeed");

        let (_, pem) = parse_x509_pem(issued.certificate_pem.as_bytes())
            .expect("certificate PEM should parse");
        let (_, cert) =
            X509Certificate::from_der(&pem.contents).expect("certificate DER should parse");

        let not_before = cert.validity().not_before.timestamp();
        let not_after = cert.validity().not_after.timestamp();
        let validity_seconds = not_after.saturating_sub(not_before);
        assert!(
            (3_595..=3_605).contains(&validity_seconds),
            "expected validity around 3600s, got {validity_seconds}s"
        );

        let metadata_not_after = (issued.expires_at_unix_ms / 1_000) as i64;
        assert!(
            (metadata_not_after - not_after).abs() <= 2,
            "metadata expires_at_unix_ms ({metadata_not_after}) should align with x509 not_after ({not_after})"
        );
    }

    #[test]
    fn server_certificate_supports_custom_dns_and_ip_sans() {
        let mut ca = CertificateAuthority::new("Palyra Test CA").expect("CA should initialize");
        let issued = ca
            .issue_server_certificate_with_sans(
                "palyrad-node-rpc",
                Duration::from_secs(3_600),
                &[
                    "palyrad-node-rpc".to_owned(),
                    "node1.lan".to_owned(),
                    "NODE1.LAN".to_owned(),
                    "  ".to_owned(),
                ],
                &[IpAddr::V4(Ipv4Addr::new(192, 168, 1, 24))],
            )
            .expect("certificate issuance with SAN overrides should succeed");

        let (_, pem) = parse_x509_pem(issued.certificate_pem.as_bytes())
            .expect("certificate PEM should parse");
        let (_, cert) =
            X509Certificate::from_der(&pem.contents).expect("certificate DER should parse");
        let san_extension = cert
            .subject_alternative_name()
            .expect("SAN extension parse should succeed")
            .expect("SAN extension should be present");

        let mut dns_names = Vec::new();
        let mut ip_addresses = Vec::new();
        for general_name in &san_extension.value.general_names {
            match general_name {
                GeneralName::DNSName(value) => dns_names.push((*value).to_owned()),
                GeneralName::IPAddress(raw) => {
                    if raw.len() == 4 {
                        ip_addresses
                            .push(IpAddr::V4(Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3])));
                    }
                }
                _ => {}
            }
        }

        assert!(
            dns_names.iter().any(|value| value == "localhost"),
            "default localhost SAN should be present"
        );
        assert!(
            dns_names.iter().any(|value| value == "palyrad-node-rpc"),
            "common name SAN should be present"
        );
        assert!(
            dns_names.iter().any(|value| value == "node1.lan"),
            "custom DNS SAN should be present"
        );
        assert_eq!(
            dns_names.iter().filter(|value| value.eq_ignore_ascii_case("node1.lan")).count(),
            1,
            "custom DNS SAN should be deduplicated case-insensitively"
        );
        assert!(
            ip_addresses.contains(&IpAddr::V4(Ipv4Addr::new(192, 168, 1, 24))),
            "custom IP SAN should be present"
        );
    }
}

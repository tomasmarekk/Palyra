use std::{collections::HashSet, time::SystemTime};

use crate::{
    ca::IssuedCertificate,
    error::{IdentityError, IdentityResult},
    unix_ms,
};

use super::{
    helpers::certificate_fingerprint_hex, should_rotate_certificate, IdentityManager, PairedDevice,
    RevokedDevice,
};

impl IdentityManager {
    pub fn force_rotate_device_certificate(
        &mut self,
        device_id: &str,
    ) -> IdentityResult<IssuedCertificate> {
        self.mutate_persisted_state(|manager| {
            manager.force_rotate_device_certificate_inner(device_id)
        })
    }

    fn force_rotate_device_certificate_inner(
        &mut self,
        device_id: &str,
    ) -> IdentityResult<IssuedCertificate> {
        if self.revoked_devices.contains_key(device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let paired =
            self.paired_devices.get(device_id).cloned().ok_or(IdentityError::DeviceNotPaired)?;
        let previous_fingerprint =
            certificate_fingerprint_hex(&paired.current_certificate.certificate_pem)?;

        let rotated = self.ca.issue_client_certificate(device_id, self.certificate_validity)?;
        let rotated_fingerprint = certificate_fingerprint_hex(&rotated.certificate_pem)?;
        let previous_fingerprints = paired.certificate_fingerprints.clone();
        let mut updated = paired;
        updated.current_certificate = rotated.clone();
        if !updated.certificate_fingerprints.contains(&rotated_fingerprint) {
            updated.certificate_fingerprints.push(rotated_fingerprint);
        }
        self.revoked_certificate_fingerprints.insert(previous_fingerprint);
        for fingerprint in previous_fingerprints {
            self.revoked_certificate_fingerprints.insert(fingerprint);
        }
        self.paired_devices.insert(device_id.to_owned(), updated);
        Ok(rotated)
    }

    pub fn rotate_device_certificate_if_due(
        &mut self,
        device_id: &str,
        now: SystemTime,
    ) -> IdentityResult<IssuedCertificate> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        if self.revoked_devices.contains_key(device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let paired =
            self.paired_devices.get(device_id).cloned().ok_or(IdentityError::DeviceNotPaired)?;
        if paired.current_certificate.private_key_pem.is_empty() {
            let rotated = self.force_rotate_device_certificate_inner(device_id)?;
            self.persist_identity_state_bundle()?;
            return Ok(rotated);
        }
        if should_rotate_certificate(&paired.current_certificate, now, self.rotation_threshold)? {
            let rotated = self.force_rotate_device_certificate_inner(device_id)?;
            self.persist_identity_state_bundle()?;
            return Ok(rotated);
        }
        Ok(paired.current_certificate)
    }

    pub fn revoke_device(
        &mut self,
        device_id: &str,
        reason: &str,
        now: SystemTime,
    ) -> IdentityResult<()> {
        self.mutate_persisted_state(|manager| {
            if let Some(paired) = manager.paired_devices.remove(device_id) {
                manager.revoke_superseded_certificates(&paired)?;
            }
            let revoked = RevokedDevice {
                device_id: device_id.to_owned(),
                reason: reason.to_owned(),
                revoked_at_unix_ms: unix_ms(now)?,
            };
            manager.revoked_devices.insert(device_id.to_owned(), revoked);
            Ok(())
        })
    }

    #[must_use]
    pub fn paired_device(&self, device_id: &str) -> Option<&PairedDevice> {
        self.paired_devices.get(device_id)
    }

    #[must_use]
    pub fn revoked_devices(&self) -> HashSet<String> {
        self.revoked_devices.keys().cloned().collect()
    }

    #[must_use]
    pub fn revoked_certificate_fingerprints(&self) -> HashSet<String> {
        self.revoked_certificate_fingerprints.clone()
    }

    pub(super) fn revoke_superseded_certificates(
        &mut self,
        paired: &PairedDevice,
    ) -> IdentityResult<()> {
        for fingerprint in &paired.certificate_fingerprints {
            self.revoked_certificate_fingerprints.insert(fingerprint.clone());
        }
        self.revoked_certificate_fingerprints
            .insert(certificate_fingerprint_hex(&paired.current_certificate.certificate_pem)?);
        Ok(())
    }
}

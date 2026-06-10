//! Certificate rotation and device revocation on [`IdentityManager`].
//!
//! Revocation is two-layered: revoked device IDs can never pair again, and revoked
//! certificate fingerprints feed the mTLS verifier's revocation index so superseded or
//! revoked leaves stop connecting immediately. Rotation always revokes every prior
//! fingerprint of the device — there is never more than one valid leaf per device.

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
    /// Rotates a device's certificate unconditionally, revoking all of its previous
    /// certificate fingerprints.
    ///
    /// # Errors
    /// Returns [`IdentityError::DeviceRevoked`] / [`IdentityError::DeviceNotPaired`]
    /// for ineligible devices, [`IdentityError::Cryptographic`] on issuance failure,
    /// plus lock and persistence failure modes.
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

    /// Returns the device's current certificate, rotating first if it is near expiry
    /// or if its private key is unavailable.
    ///
    /// State is reloaded under the cross-process lock before deciding, so a revocation
    /// written by another process wins over a cached certificate.
    ///
    /// # Errors
    /// Same failure modes as [`Self::force_rotate_device_certificate`].
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
        // Private keys are never persisted, so after a reload the stored certificate
        // is unusable by the device — reissue regardless of remaining lifetime.
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

    /// Revokes a device: removes its pairing, revokes all of its certificates, and
    /// records a tombstone that permanently blocks re-pairing under this ID.
    ///
    /// Idempotent for already-revoked or never-paired IDs (the tombstone is refreshed
    /// with the new reason and timestamp).
    ///
    /// # Errors
    /// Returns certificate-parsing, lock, and persistence failure modes.
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

    /// Returns the pairing record for `device_id`, if currently paired.
    #[must_use]
    pub fn paired_device(&self, device_id: &str) -> Option<&PairedDevice> {
        self.paired_devices.get(device_id)
    }

    /// Returns all paired devices, sorted by device ID for deterministic output.
    #[must_use]
    pub fn paired_devices(&self) -> Vec<PairedDevice> {
        let mut devices = self.paired_devices.values().cloned().collect::<Vec<_>>();
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        devices
    }

    /// Returns the revocation tombstone for `device_id`, if revoked.
    #[must_use]
    pub fn revoked_device_record(&self, device_id: &str) -> Option<&RevokedDevice> {
        self.revoked_devices.get(device_id)
    }

    /// Returns all revocation tombstones, sorted by device ID for deterministic output.
    #[must_use]
    pub fn revoked_device_records(&self) -> Vec<RevokedDevice> {
        let mut devices = self.revoked_devices.values().cloned().collect::<Vec<_>>();
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        devices
    }

    /// Returns the set of revoked device IDs.
    #[must_use]
    pub fn revoked_devices(&self) -> HashSet<String> {
        self.revoked_devices.keys().cloned().collect()
    }

    /// Returns all revoked certificate fingerprints — the payload for refreshing a
    /// [`RevocationIndex`](crate::RevocationIndex).
    #[must_use]
    pub fn revoked_certificate_fingerprints(&self) -> HashSet<String> {
        self.revoked_certificate_fingerprints.clone()
    }

    /// Returns whether the given hex SHA-256 certificate fingerprint is revoked.
    #[must_use]
    pub fn is_revoked_certificate_fingerprint(&self, fingerprint: &str) -> bool {
        self.revoked_certificate_fingerprints.contains(fingerprint)
    }

    /// Finds which paired device a certificate fingerprint belongs to (current or
    /// historic), e.g. to attribute an mTLS connection to a device.
    #[must_use]
    pub fn device_id_for_certificate_fingerprint(&self, fingerprint: &str) -> Option<String> {
        self.paired_devices.iter().find_map(|(device_id, paired)| {
            let current_matches =
                certificate_fingerprint_hex(&paired.current_certificate.certificate_pem)
                    .ok()
                    .is_some_and(|value| value == fingerprint);
            let historic_matches =
                paired.certificate_fingerprints.iter().any(|value| value == fingerprint);
            if current_matches || historic_matches {
                Some(device_id.clone())
            } else {
                None
            }
        })
    }

    /// Unpairs a device and revokes its certificates without leaving a tombstone, so
    /// the same device ID may pair again later. Returns whether the device was paired.
    ///
    /// # Errors
    /// Returns certificate-parsing, lock, and persistence failure modes.
    pub fn remove_paired_device(&mut self, device_id: &str) -> IdentityResult<bool> {
        self.mutate_persisted_state(|manager| {
            let Some(paired) = manager.paired_devices.remove(device_id) else {
                return Ok(false);
            };
            manager.revoke_superseded_certificates(&paired)?;
            Ok(true)
        })
    }

    /// Removes a revocation tombstone, allowing the device ID to pair again. Returns
    /// whether a tombstone existed.
    ///
    /// Certificate fingerprints revoked alongside the device stay revoked — clearing
    /// re-admits the ID, never previously issued certificates.
    ///
    /// # Errors
    /// Returns lock and persistence failure modes.
    pub fn clear_revoked_device(&mut self, device_id: &str) -> IdentityResult<bool> {
        self.mutate_persisted_state(|manager| {
            Ok(manager.revoked_devices.remove(device_id).is_some())
        })
    }

    /// Marks every certificate fingerprint of `paired` (historic and current) revoked.
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

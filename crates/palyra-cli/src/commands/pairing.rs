use crate::*;

pub(crate) fn run_pairing(command: PairingCommand) -> Result<()> {
    match command {
        PairingCommand::Pair {
            device_id,
            client_kind,
            method,
            proof,
            proof_stdin,
            allow_insecure_proof_arg,
            store_dir,
            approve,
            simulate_rotation,
        } => {
            if !approve {
                anyhow::bail!(
                    "decision=deny_by_default approval_required=true reason=pairing requires explicit --approve"
                );
            }

            let store_root = resolve_identity_store_root(store_dir)?;
            let store = build_identity_store(&store_root)?;
            let mut manager = IdentityManager::with_store(store.clone())
                .context("failed to initialize identity manager")?;
            let proof = resolve_pairing_proof(proof, proof_stdin, allow_insecure_proof_arg)?;
            let pairing_method = build_pairing_method(method, &proof);

            let started_at = SystemTime::now();
            let session = manager
                .start_pairing(to_identity_client_kind(client_kind), pairing_method, started_at)
                .context("failed to start pairing session")?;
            let device = DeviceIdentity::generate(&device_id)
                .context("failed to generate device identity")?;

            let hello = manager
                .build_device_hello(&session, &device, &proof)
                .context("failed to build device pairing hello")?;
            let completed_at = SystemTime::now();
            let result = manager
                .complete_pairing(hello, completed_at)
                .context("failed to complete pairing handshake")?;
            if let Err(store_error) = device.store(store.as_ref()) {
                let rollback = manager.revoke_device(
                    &device_id,
                    "device identity persistence failed after pairing",
                    SystemTime::now(),
                );
                if let Err(rollback_error) = rollback {
                    anyhow::bail!(
                        "failed to persist device identity after pairing ({store_error}); rollback revoke failed ({rollback_error})"
                    );
                }
                anyhow::bail!(
                    "failed to persist device identity after pairing: {store_error}; pairing was rolled back"
                );
            }

            println!(
                "pairing.status=paired device_id={} client_kind={} method={} identity_fingerprint={} signing_public_key_hex={} transcript_hash={} store_root={}",
                result.device.device_id,
                result.device.client_kind.as_str(),
                method.as_str(),
                result.identity_fingerprint,
                result.signing_public_key_hex,
                result.transcript_hash_hex,
                store_root.display(),
            );

            if simulate_rotation {
                manager
                    .rotate_device_certificate_if_due(
                        &device_id,
                        SystemTime::now() + DEFAULT_CERT_VALIDITY,
                    )
                    .context("failed to rotate certificate in simulation mode")?;
                println!("pairing.rotation=simulated rotated=true");
            }

            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

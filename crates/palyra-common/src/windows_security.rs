//! Windows-only security primitives over Win32: owner+SYSTEM DACL hardening, current-user
//! DPAPI encryption, current-user SID lookup, and PID liveness probing.
//!
//! Used by the vault's encrypted-file fallback and daemon state hardening; every API is
//! `cfg(windows)` and fails with an `io::Error` carrying the Win32 last-error context.

#[cfg(windows)]
use std::{
    ffi::OsStr,
    io,
    os::windows::ffi::OsStrExt,
    path::Path,
    ptr::{null, null_mut},
    slice,
};

#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, GetLastError, LocalFree, ERROR_ACCESS_DENIED, ERROR_INSUFFICIENT_BUFFER,
        ERROR_INVALID_PARAMETER, HANDLE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Security::{
        Authorization::{
            ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
            SetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT,
        },
        Cryptography::{
            CryptProtectData, CryptUnprotectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
        },
        GetSecurityDescriptorDacl, GetTokenInformation, TokenUser, DACL_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, TOKEN_QUERY, TOKEN_USER,
    },
    System::Threading::{
        GetCurrentProcess, OpenProcess, OpenProcessToken, WaitForSingleObject,
        PROCESS_QUERY_LIMITED_INFORMATION,
    },
};

// The standard SYNCHRONIZE access right, required by `WaitForSingleObject`; spelled as a
// local const because the enabled `windows-sys` feature set does not re-export it.
#[cfg(windows)]
const PROCESS_SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;

/// Closes a Win32 handle on drop so every early-return path releases it.
#[cfg(windows)]
struct HandleGuard(HANDLE);

#[cfg(windows)]
impl Drop for HandleGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the guard owns a handle obtained from a successful Win32 open call
            // and is the only closer; null handles are skipped above.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

/// Frees a `LocalAlloc`-backed pointer on drop (SID strings, security descriptors, DPAPI
/// output blobs are all allocated by the system with `LocalAlloc`).
#[cfg(windows)]
struct LocalFreeGuard(*mut core::ffi::c_void);

#[cfg(windows)]
impl Drop for LocalFreeGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: the guard owns a pointer the system allocated via `LocalAlloc` and
            // is the only freer; null pointers are skipped above.
            unsafe {
                let _ = LocalFree(self.0.cast());
            }
        }
    }
}

#[cfg(windows)]
fn wide_null(value: impl AsRef<OsStr>) -> Vec<u16> {
    value.as_ref().encode_wide().chain(std::iter::once(0)).collect()
}

#[cfg(windows)]
fn io_error(operation: &str) -> io::Error {
    io::Error::other(format!("{operation}: {}", io::Error::last_os_error()))
}

/// Returns the current process owner's SID in string form (e.g. for SDDL ACE templates).
///
/// # Errors
/// Returns an `io::Error` with Win32 last-error context if the process token cannot be
/// opened or queried, or if the SID cannot be converted to a string.
#[cfg(windows)]
pub fn current_user_sid() -> io::Result<String> {
    let mut token = null_mut();
    // SAFETY: `GetCurrentProcess` returns a pseudo-handle that needs no closing, and
    // `token` is a valid out-pointer; on success the guard below closes the token.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io_error("failed to open current process token"));
    }
    let _token = HandleGuard(token);

    // Standard Win32 sizing idiom: the first call is expected to fail with
    // ERROR_INSUFFICIENT_BUFFER and report the required buffer size.
    let mut required_bytes = 0_u32;
    // SAFETY: a null buffer with length 0 is the documented sizing-call form;
    // `required_bytes` is a valid out-pointer.
    unsafe {
        GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required_bytes);
    }
    // SAFETY: trivially safe FFI call reading thread-local last-error state.
    let last_error = unsafe { GetLastError() };
    if required_bytes == 0 || last_error != ERROR_INSUFFICIENT_BUFFER {
        return Err(io_error("failed to size current token user info"));
    }

    let mut buffer = vec![0_u8; required_bytes as usize];
    // SAFETY: `buffer` is writable for exactly `required_bytes` bytes as sized above.
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            required_bytes,
            &mut required_bytes,
        )
    } == 0
    {
        return Err(io_error("failed to read current token user info"));
    }

    // SAFETY: a successful `GetTokenInformation(TokenUser, ..)` call guarantees the buffer
    // starts with a valid TOKEN_USER structure; the buffer outlives this borrow.
    let token_user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
    let mut sid_ptr = null_mut::<u16>();
    // SAFETY: `token_user.User.Sid` is a valid SID from the call above and `sid_ptr` is a
    // valid out-pointer; on success the system allocates the string via `LocalAlloc`.
    if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &mut sid_ptr) } == 0 {
        return Err(io_error("failed to convert token SID to string"));
    }
    let _sid_guard = LocalFreeGuard(sid_ptr.cast());
    // SAFETY: `ConvertSidToStringSidW` succeeded, so `sid_ptr` points at a NUL-terminated
    // wide string; the loop only reads up to and including that terminator.
    let sid_len = unsafe {
        let mut len = 0_usize;
        while *sid_ptr.add(len) != 0 {
            len += 1;
        }
        len
    };
    // SAFETY: `sid_len` was measured against the same allocation directly above.
    String::from_utf16(unsafe { slice::from_raw_parts(sid_ptr, sid_len) })
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Replaces a file or directory DACL so only `owner_sid` and SYSTEM have access.
///
/// The DACL is protected (`D:P`), cutting ACE inheritance from parent directories;
/// directory ACEs use `OICI` so new children inherit the restriction. This is the Windows
/// analogue of `chmod 600`/`700` for secret-bearing state paths.
///
/// # Errors
/// Returns an `io::Error` with Win32 last-error context if the SDDL string cannot be
/// converted, the DACL cannot be extracted, or the security info cannot be applied.
#[cfg(windows)]
pub fn harden_windows_path_permissions(
    path: &Path,
    owner_sid: &str,
    is_directory: bool,
) -> io::Result<()> {
    let ace_flags = if is_directory { "OICI" } else { "" };
    let owner_ace = format!("(A;{ace_flags};FA;;;{owner_sid})");
    let system_ace = format!("(A;{ace_flags};FA;;;SY)");
    let sddl = format!("D:P{owner_ace}{system_ace}");
    let sddl_wide = wide_null(sddl);

    let mut security_descriptor = null_mut::<core::ffi::c_void>();
    // SAFETY: `sddl_wide` is NUL-terminated and outlives the call; the out-pointer is
    // valid, and on success the system allocates the descriptor via `LocalAlloc`.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            SDDL_REVISION_1,
            &mut security_descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io_error("failed to build Windows security descriptor"));
    }
    let _security_descriptor = LocalFreeGuard(security_descriptor.cast());

    let mut dacl_present = 0_i32;
    let mut dacl_defaulted = 0_i32;
    let mut dacl_ptr = null_mut();
    // SAFETY: `security_descriptor` is the valid descriptor produced above and all three
    // out-pointers are valid; `dacl_ptr` borrows into the descriptor allocation.
    if unsafe {
        GetSecurityDescriptorDacl(
            security_descriptor.cast(),
            &mut dacl_present,
            &mut dacl_ptr,
            &mut dacl_defaulted,
        )
    } == 0
    {
        return Err(io_error("failed to extract Windows DACL"));
    }
    if dacl_present == 0 {
        return Err(io::Error::other(
            "failed to extract Windows DACL: security descriptor has no DACL",
        ));
    }

    let path_wide = wide_null(path.as_os_str());
    // SAFETY: `path_wide` is NUL-terminated, `dacl_ptr` points into the still-guarded
    // descriptor allocation, and unused security parts are passed as null per the API.
    let result = unsafe {
        SetNamedSecurityInfoW(
            path_wide.as_ptr().cast_mut(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            dacl_ptr,
            null_mut(),
        )
    };
    if result != 0 {
        // Win32 error codes fit comfortably in i32; the cast cannot truncate meaningfully.
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    Ok(())
}

#[cfg(windows)]
fn data_blob_from_slice(bytes: &[u8]) -> io::Result<CRYPT_INTEGER_BLOB> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "payload exceeds Windows DATA_BLOB size")
    })?;
    Ok(CRYPT_INTEGER_BLOB {
        cbData: len,
        pbData: if bytes.is_empty() { null_mut() } else { bytes.as_ptr().cast_mut() },
    })
}

#[cfg(windows)]
fn data_blob_to_vec(blob: &CRYPT_INTEGER_BLOB) -> Vec<u8> {
    if blob.pbData.is_null() || blob.cbData == 0 {
        return Vec::new();
    }
    // SAFETY: the blob was filled by a successful DPAPI call, so `pbData` is valid for
    // `cbData` bytes; non-null and non-zero were checked above.
    unsafe { slice::from_raw_parts(blob.pbData, blob.cbData as usize) }.to_vec()
}

/// Encrypts bytes with DPAPI bound to the current user account (no UI prompts).
///
/// Output can only be decrypted by the same user on the same machine via
/// [`dpapi_unprotect_current_user`].
///
/// # Errors
/// Returns an `io::Error` if the payload exceeds the DATA_BLOB size limit or the DPAPI
/// call fails (Win32 last-error context included).
#[cfg(windows)]
pub fn dpapi_protect_current_user(raw: &[u8]) -> io::Result<Vec<u8>> {
    let input = data_blob_from_slice(raw)?;
    let mut output = CRYPT_INTEGER_BLOB { cbData: 0, pbData: null_mut() };
    // SAFETY: `input` describes a live borrow of `raw`, `output` is a valid out-blob, and
    // optional parameters are passed as null per the API; on success the system allocates
    // `output.pbData` via `LocalAlloc`, freed by the guard below.
    if unsafe {
        CryptProtectData(
            &input,
            null(),
            null(),
            null(),
            null_mut(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    } == 0
    {
        return Err(io_error("failed to protect DPAPI payload"));
    }
    let _output = LocalFreeGuard(output.pbData.cast());
    Ok(data_blob_to_vec(&output))
}

/// Decrypts bytes previously produced by [`dpapi_protect_current_user`].
///
/// # Errors
/// Returns an `io::Error` if the payload exceeds the DATA_BLOB size limit, the ciphertext
/// is corrupt, or it was protected by a different user/machine.
#[cfg(windows)]
pub fn dpapi_unprotect_current_user(raw: &[u8]) -> io::Result<Vec<u8>> {
    let input = data_blob_from_slice(raw)?;
    let mut output = CRYPT_INTEGER_BLOB { cbData: 0, pbData: null_mut() };
    // SAFETY: same contract as in `dpapi_protect_current_user` — live input borrow, valid
    // out-blob, system-allocated output freed by the guard below.
    if unsafe {
        CryptUnprotectData(
            &input,
            null_mut(),
            null(),
            null(),
            null_mut(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    } == 0
    {
        return Err(io_error("failed to unprotect DPAPI payload"));
    }
    let _output = LocalFreeGuard(output.pbData.cast());
    Ok(data_blob_to_vec(&output))
}

/// Probes whether a process with the given PID is still running.
///
/// # Errors
/// Returns an `io::Error` only for unexpected probe failures; "process gone" and
/// "access denied" map to `Ok(false)`/`Ok(true)` respectively.
#[cfg(windows)]
pub fn process_is_alive(pid: u32) -> io::Result<bool> {
    // SAFETY: `OpenProcess` has no pointer parameters; a null return is handled below.
    let handle = unsafe {
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE_ACCESS, 0, pid)
    };
    if handle.is_null() {
        // SAFETY: trivially safe FFI call reading thread-local last-error state.
        return match unsafe { GetLastError() } {
            // No such PID exists at all.
            ERROR_INVALID_PARAMETER => Ok(false),
            // The PID exists but belongs to a process we may not open — still alive.
            ERROR_ACCESS_DENIED => Ok(true),
            _ => Err(io_error("failed to open process for liveness probe")),
        };
    }
    let _handle = HandleGuard(handle);
    // A zero-timeout wait distinguishes running (timeout) from exited (signaled): an open
    // handle alone is not proof of liveness because handles keep zombie processes visible.
    // SAFETY: `handle` is the valid non-null handle opened above and stays open via the
    // guard for the duration of the call.
    match unsafe { WaitForSingleObject(handle, 0) } {
        WAIT_TIMEOUT => Ok(true),
        WAIT_OBJECT_0 => Ok(false),
        WAIT_FAILED => Err(io_error("failed to query process liveness")),
        _ => Ok(true),
    }
}

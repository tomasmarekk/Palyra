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

#[cfg(windows)]
const PROCESS_SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;

#[cfg(windows)]
struct HandleGuard(HANDLE);

#[cfg(windows)]
impl Drop for HandleGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

#[cfg(windows)]
struct LocalFreeGuard(*mut core::ffi::c_void);

#[cfg(windows)]
impl Drop for LocalFreeGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
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

#[cfg(windows)]
pub fn current_user_sid() -> io::Result<String> {
    let mut token = null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io_error("failed to open current process token"));
    }
    let _token = HandleGuard(token);

    let mut required_bytes = 0_u32;
    unsafe {
        GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required_bytes);
    }
    let last_error = unsafe { GetLastError() };
    if required_bytes == 0 || last_error != ERROR_INSUFFICIENT_BUFFER {
        return Err(io_error("failed to size current token user info"));
    }

    let mut buffer = vec![0_u8; required_bytes as usize];
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

    let token_user = unsafe { &*(buffer.as_ptr().cast::<TOKEN_USER>()) };
    let mut sid_ptr = null_mut::<u16>();
    if unsafe { ConvertSidToStringSidW(token_user.User.Sid, &mut sid_ptr) } == 0 {
        return Err(io_error("failed to convert token SID to string"));
    }
    let _sid_guard = LocalFreeGuard(sid_ptr.cast());
    let sid_len = unsafe {
        let mut len = 0_usize;
        while *sid_ptr.add(len) != 0 {
            len += 1;
        }
        len
    };
    String::from_utf16(unsafe { slice::from_raw_parts(sid_ptr, sid_len) })
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

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
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    let _ = dacl_defaulted;
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
    let bytes = unsafe { slice::from_raw_parts(blob.pbData, blob.cbData as usize) }.to_vec();
    bytes
}

#[cfg(windows)]
pub fn dpapi_protect_current_user(raw: &[u8]) -> io::Result<Vec<u8>> {
    let input = data_blob_from_slice(raw)?;
    let mut output = CRYPT_INTEGER_BLOB { cbData: 0, pbData: null_mut() };
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

#[cfg(windows)]
pub fn dpapi_unprotect_current_user(raw: &[u8]) -> io::Result<Vec<u8>> {
    let input = data_blob_from_slice(raw)?;
    let mut output = CRYPT_INTEGER_BLOB { cbData: 0, pbData: null_mut() };
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

#[cfg(windows)]
pub fn process_is_alive(pid: u32) -> io::Result<bool> {
    let handle = unsafe {
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE_ACCESS, 0, pid)
    };
    if handle.is_null() {
        return match unsafe { GetLastError() } {
            ERROR_INVALID_PARAMETER => Ok(false),
            ERROR_ACCESS_DENIED => Ok(true),
            _ => Err(io_error("failed to open process for liveness probe")),
        };
    }
    let _handle = HandleGuard(handle);
    match unsafe { WaitForSingleObject(handle, 0) } {
        WAIT_TIMEOUT => Ok(true),
        WAIT_OBJECT_0 => Ok(false),
        WAIT_FAILED => Err(io_error("failed to query process liveness")),
        _ => Ok(true),
    }
}

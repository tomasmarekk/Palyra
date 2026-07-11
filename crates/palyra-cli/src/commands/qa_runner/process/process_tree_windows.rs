use super::*;

#[cfg(windows)]
pub(super) const WINDOWS_CREATE_SUSPENDED: u32 = 0x0000_0004;
#[cfg(windows)]
const WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_CLOSE: u32 = 0x0000_2000;
#[cfg(windows)]
const WINDOWS_JOB_BASIC_ACCOUNTING_INFORMATION: i32 = 1;
#[cfg(windows)]
const WINDOWS_JOB_EXTENDED_LIMIT_INFORMATION: i32 = 9;
#[cfg(windows)]
const WINDOWS_THREAD_SUSPEND_RESUME: u32 = 0x0002;
#[cfg(windows)]
const WINDOWS_TH32CS_SNAPTHREAD: u32 = 0x0000_0004;

#[cfg(windows)]
pub(super) type WindowsRawHandle = *mut std::ffi::c_void;

#[cfg(windows)]
#[repr(C)]
#[derive(Default)]
pub(super) struct WindowsJobBasicLimitInformation {
    per_process_user_time_limit: i64,
    per_job_user_time_limit: i64,
    limit_flags: u32,
    minimum_working_set_size: usize,
    maximum_working_set_size: usize,
    active_process_limit: u32,
    affinity: usize,
    priority_class: u32,
    scheduling_class: u32,
}

#[cfg(windows)]
#[repr(C)]
#[derive(Default)]
pub(super) struct WindowsIoCounters {
    read_operation_count: u64,
    write_operation_count: u64,
    other_operation_count: u64,
    read_transfer_count: u64,
    write_transfer_count: u64,
    other_transfer_count: u64,
}

#[cfg(windows)]
#[repr(C)]
#[derive(Default)]
pub(super) struct WindowsJobExtendedLimitInformation {
    basic_limit_information: WindowsJobBasicLimitInformation,
    io_info: WindowsIoCounters,
    process_memory_limit: usize,
    job_memory_limit: usize,
    peak_process_memory_used: usize,
    peak_job_memory_used: usize,
}

#[cfg(windows)]
#[repr(C)]
#[derive(Default)]
pub(super) struct WindowsJobBasicAccountingInformation {
    total_user_time: i64,
    total_kernel_time: i64,
    this_period_total_user_time: i64,
    this_period_total_kernel_time: i64,
    total_page_fault_count: u32,
    total_processes: u32,
    active_processes: u32,
    total_terminated_processes: u32,
}

#[cfg(windows)]
#[repr(C)]
#[derive(Default)]
pub(super) struct WindowsThreadEntry32 {
    pub(super) size: u32,
    usage_count: u32,
    thread_id: u32,
    owner_process_id: u32,
    base_priority: i32,
    priority_delta: i32,
    pub(super) flags: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "CreateJobObjectW"]
    fn windows_create_job_object(
        security_attributes: *const std::ffi::c_void,
        name: *const u16,
    ) -> WindowsRawHandle;
    #[link_name = "SetInformationJobObject"]
    fn windows_set_information_job_object(
        job: WindowsRawHandle,
        information_class: i32,
        information: *const std::ffi::c_void,
        information_length: u32,
    ) -> i32;
    #[link_name = "AssignProcessToJobObject"]
    pub(super) fn windows_assign_process_to_job_object(
        job: WindowsRawHandle,
        process: WindowsRawHandle,
    ) -> i32;
    #[link_name = "TerminateJobObject"]
    fn windows_terminate_job_object(job: WindowsRawHandle, exit_code: u32) -> i32;
    #[link_name = "QueryInformationJobObject"]
    fn windows_query_information_job_object(
        job: WindowsRawHandle,
        information_class: i32,
        information: *mut std::ffi::c_void,
        information_length: u32,
        return_length: *mut u32,
    ) -> i32;
    #[link_name = "CloseHandle"]
    fn windows_close_handle(handle: WindowsRawHandle) -> i32;
    #[link_name = "CreateToolhelp32Snapshot"]
    fn windows_create_toolhelp32_snapshot(flags: u32, process_id: u32) -> WindowsRawHandle;
    #[link_name = "Thread32First"]
    fn windows_thread32_first(snapshot: WindowsRawHandle, entry: *mut WindowsThreadEntry32) -> i32;
    #[link_name = "Thread32Next"]
    fn windows_thread32_next(snapshot: WindowsRawHandle, entry: *mut WindowsThreadEntry32) -> i32;
    #[link_name = "OpenThread"]
    fn windows_open_thread(access: u32, inherit_handle: i32, thread_id: u32) -> WindowsRawHandle;
    #[link_name = "ResumeThread"]
    fn windows_resume_thread(thread: WindowsRawHandle) -> u32;
}

#[cfg(windows)]
pub(super) struct WindowsOwnedHandle {
    pub(super) handle: WindowsRawHandle,
}

#[cfg(windows)]
// SAFETY: the wrapper owns a kernel handle whose Win32 operations are thread-safe; ownership is
// unique and `Drop` performs the only close.
unsafe impl Send for WindowsOwnedHandle {}

#[cfg(windows)]
impl WindowsOwnedHandle {
    pub(super) fn new(handle: WindowsRawHandle) -> io::Result<Self> {
        if handle.is_null() || handle == (-1_isize as WindowsRawHandle) {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { handle })
        }
    }

    pub(super) fn get(&self) -> WindowsRawHandle {
        self.handle
    }
}

#[cfg(windows)]
impl Drop for WindowsOwnedHandle {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the live handle and closes it exactly once.
        let _ = unsafe { windows_close_handle(self.handle) };
    }
}

#[cfg(windows)]
pub(super) struct WindowsJobHandle {
    pub(super) handle: WindowsOwnedHandle,
}

#[cfg(windows)]
impl WindowsJobHandle {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: null security attributes and a null name request an unnamed job.
        let raw = unsafe { windows_create_job_object(std::ptr::null(), std::ptr::null()) };
        let handle = WindowsOwnedHandle::new(raw)?;
        let mut limits = WindowsJobExtendedLimitInformation::default();
        limits.basic_limit_information.limit_flags = WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_CLOSE;
        let information_length = u32::try_from(std::mem::size_of_val(&limits))
            .map_err(|_| io::Error::other("job limit structure is too large"))?;
        // SAFETY: `limits` has the documented C layout for extended job limits and remains live
        // for the call.
        if unsafe {
            windows_set_information_job_object(
                handle.get(),
                WINDOWS_JOB_EXTENDED_LIMIT_INFORMATION,
                std::ptr::from_ref(&limits).cast(),
                information_length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { handle })
    }

    pub(super) fn terminate(&self) -> io::Result<()> {
        // SAFETY: the job handle remains valid and the exit code is an application-owned value.
        if unsafe { windows_terminate_job_object(self.handle.get(), 1) } == 0 {
            let error = io::Error::last_os_error();
            if self.active_processes()? != 0 {
                return Err(error);
            }
        }
        Ok(())
    }

    fn active_processes(&self) -> io::Result<u32> {
        let mut information = WindowsJobBasicAccountingInformation::default();
        let information_length = u32::try_from(std::mem::size_of_val(&information))
            .map_err(|_| io::Error::other("job accounting structure is too large"))?;
        // SAFETY: `information` is a writable buffer with the documented accounting layout.
        if unsafe {
            windows_query_information_job_object(
                self.handle.get(),
                WINDOWS_JOB_BASIC_ACCOUNTING_INFORMATION,
                std::ptr::from_mut(&mut information).cast(),
                information_length,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(information.active_processes)
    }

    pub(super) fn wait_until_inactive(&self, timeout: Duration) -> io::Result<bool> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| io::Error::other("job cleanup deadline overflow"))?;
        loop {
            if self.active_processes()? == 0 {
                return Ok(true);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
        }
    }
}

#[cfg(windows)]
pub(super) fn resume_suspended_windows_process(process_id: u32) -> io::Result<()> {
    // SAFETY: the snapshot flags are fixed and the returned handle is validated below.
    let snapshot = unsafe { windows_create_toolhelp32_snapshot(WINDOWS_TH32CS_SNAPTHREAD, 0) };
    let snapshot = WindowsOwnedHandle::new(snapshot)?;
    let mut entry = WindowsThreadEntry32 {
        size: u32::try_from(std::mem::size_of::<WindowsThreadEntry32>())
            .map_err(|_| io::Error::other("thread entry structure is too large"))?,
        ..WindowsThreadEntry32::default()
    };
    // SAFETY: the snapshot and writable entry are live for the enumeration.
    if unsafe { windows_thread32_first(snapshot.get(), &mut entry) } == 0 {
        return Err(io::Error::last_os_error());
    }
    loop {
        if entry.owner_process_id == process_id {
            // SAFETY: the thread id came from the live snapshot and only resume rights are asked.
            let thread =
                unsafe { windows_open_thread(WINDOWS_THREAD_SUSPEND_RESUME, 0, entry.thread_id) };
            let thread = WindowsOwnedHandle::new(thread)?;
            // SAFETY: `thread` is open with resume rights and is still live.
            let previous = unsafe { windows_resume_thread(thread.get()) };
            if previous == u32::MAX {
                return Err(io::Error::last_os_error());
            }
            if previous != 1 {
                return Err(io::Error::other(format!(
                    "unexpected suspend count {previous} for process {process_id}"
                )));
            }
            return Ok(());
        }
        // SAFETY: the snapshot and output entry remain valid for the next item.
        if unsafe { windows_thread32_next(snapshot.get(), &mut entry) } == 0 {
            break;
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("suspended thread for process {process_id} was not found"),
    ))
}

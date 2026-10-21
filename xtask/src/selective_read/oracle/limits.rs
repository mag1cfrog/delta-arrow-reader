use super::{Result, require, rules::integer};
use serde_json::Value;
use std::path::Path;

pub(super) struct Limits {
    old: Option<libc::rlimit>,
    pub quota: Option<u64>,
}
impl Limits {
    pub(super) fn enter(root: &Path, limits: &Value) -> Result<Self> {
        if limits.is_null() {
            return Ok(Self {
                old: None,
                quota: None,
            });
        }
        std::fs::create_dir_all(root)?;
        let disk = integer(&limits["disk_bytes"])?;
        require(
            disk > 8 * 1024 * 1024
                && crate::selective_read::pairs::free_space(root)? >= disk + 512 * 1024 * 1024,
            "oracle disk allowance exceeds available space",
        )?;
        let seconds = u32::try_from(integer(&limits["elapsed_seconds"])?)?;
        require(seconds > 0, "invalid oracle deadline")?;
        let mut old = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: valid writable rlimit, and this guard lives in the dedicated oracle process.
        if unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut old) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let limited = libc::rlimit {
            rlim_cur: integer(&limits["memory_bytes"])?.min(old.rlim_cur),
            rlim_max: old.rlim_max,
        };
        // SAFETY: valid initialized rlimit; only the soft limit changes and Drop restores it.
        if unsafe { libc::setrlimit(libc::RLIMIT_AS, &limited) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: oracle workers own their deadline and keep the default SIGALRM handler.
        unsafe {
            libc::alarm(seconds);
        }
        Ok(Self {
            old: Some(old),
            quota: Some(disk - 8 * 1024 * 1024),
        })
    }
}
impl Drop for Limits {
    fn drop(&mut self) {
        if let Some(old) = &self.old {
            // SAFETY: cancel our timer before restoring the saved process soft limit.
            unsafe {
                libc::alarm(0);
                libc::setrlimit(libc::RLIMIT_AS, old);
            }
        }
    }
}

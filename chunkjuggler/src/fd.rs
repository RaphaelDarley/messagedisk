pub fn set_fd_limit() {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };

        // Get current limit
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            use tracing::{info, warn};

            info!(
                "Current limit: soft={}, hard={}",
                limit.rlim_cur, limit.rlim_max
            );

            // Set to maximum allowed
            // limit.rlim_cur = limit.rlim_max;
            limit.rlim_cur = limit.rlim_max.min(100_000);

            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                info!("Increased to: {}", limit.rlim_cur);
            } else {
                warn!("Failed to set limit");
            }
        }
    }
}

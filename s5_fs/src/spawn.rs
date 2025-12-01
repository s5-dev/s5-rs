//! Cross-platform async task spawning.
//!
//! Provides unified APIs for spawning async tasks that work on both
//! native (tokio) and WASM (wasm-bindgen-futures) platforms.

use std::future::Future;

/// Spawns an async task on the current runtime.
///
/// - On native: uses `tokio::spawn`
/// - On WASM: uses `wasm_bindgen_futures::spawn_local`
#[cfg(target_arch = "wasm32")]
pub fn spawn_task<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_task<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

/// Spawns a delayed task that runs after a specified duration.
///
/// - On native: uses `tokio::spawn` + `tokio::time::sleep`
/// - On WASM: uses `wasm_bindgen_futures::spawn_local` + JS setTimeout
#[cfg(target_arch = "wasm32")]
pub fn spawn_delayed<F>(delay_ms: u64, future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(async move {
        sleep_ms(delay_ms).await;
        future.await;
    });
}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_delayed<F>(delay_ms: u64, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        future.await;
    });
}

/// Platform-agnostic sleep for WASM using JS setTimeout via Promise.
#[cfg(target_arch = "wasm32")]
async fn sleep_ms(ms: u64) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        let window = web_sys::window().expect("no global window");
        window
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms as i32)
            .expect("setTimeout failed");
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

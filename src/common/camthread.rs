use std::sync::{Arc, Weak};
use tokio::{
    sync::watch::{Receiver as WatchReceiver, Sender as WatchSender},
    time::{interval, sleep, timeout, Duration},
};
use tokio_util::sync::CancellationToken;

use crate::{config::CameraConfig, utils::connect_and_login, AnyResult};
use neolink_core::bc_protocol::BcCamera;

// Reconnect backoff: grows (doubling) while a camera can't be reached and resets
// once it actually connects, so an offline camera is retried rarely instead of
// hammered (which otherwise pegged a CPU core). The cap is large so a camera that
// is offline for a long time (e.g. a battery cam left uncharged) settles into
// occasional retries rather than a continuous reconnect/discovery loop.
const MIN_BACKOFF: Duration = Duration::from_millis(50);
const MAX_BACKOFF: Duration = Duration::from_secs(600);
// Keepalive (get_linktype) misses tolerated before declaring the camera dead. Generous so a
// flaky-but-streaming battery cam isn't torn down into a reconnect loop (see neolink#14);
// real disconnects are caught by camera.join(), not this counter.
const MAX_MISSED_PINGS: u32 = 30;

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum NeoCamThreadState {
    Connected,
    Disconnected,
}

pub(crate) struct NeoCamThread {
    state: WatchReceiver<NeoCamThreadState>,
    config: WatchReceiver<CameraConfig>,
    cancel: CancellationToken,
    camera_watch: WatchSender<Weak<BcCamera>>,
}

impl NeoCamThread {
    pub(crate) async fn new(
        watch_state_rx: WatchReceiver<NeoCamThreadState>,
        watch_config_rx: WatchReceiver<CameraConfig>,
        camera_watch_tx: WatchSender<Weak<BcCamera>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            state: watch_state_rx,
            config: watch_config_rx,
            cancel,
            camera_watch: camera_watch_tx,
        }
    }
    async fn run_camera(&mut self, config: &CameraConfig, backoff: &mut Duration) -> AnyResult<()> {
        let name = config.name.clone();
        log::trace!("Attempting connection with config: {config:?}");
        let camera = Arc::new(connect_and_login(config).await?);
        log::trace!("  - Connected");
        // Connection succeeded: reset the reconnect backoff. A camera that never
        // gets here (offline) keeps its grown backoff instead of hammering.
        *backoff = MIN_BACKOFF;

        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up
        if let Err(e) = update_camera_time(&camera, &name, config.update_time).await {
            log::warn!("Could not set camera time, (perhaps missing on this camera of your login in not an admin): {e:?}");
        }
        sleep(Duration::from_secs(2)).await; // Delay a little since some calls will error if camera is waking up

        self.camera_watch.send_replace(Arc::downgrade(&camera));

        let cancel_check = self.cancel.clone();
        // Now we wait for a disconnect
        tokio::select! {
            _ = cancel_check.cancelled() => {
                AnyResult::Ok(())
            }
            v = camera.join() => {
                v?;
                Ok(())
            },
            v = async {
                let mut interval = interval(Duration::from_secs(5));
                let mut missed_pings = 0;
                loop {
                    interval.tick().await;
                    log::trace!("Sending ping");
                    match timeout(Duration::from_secs(5), camera.get_linktype()).await {
                        Ok(Ok(_)) => {
                            log::trace!("Ping reply");
                            missed_pings = 0;
                            continue
                        },
                        Ok(Err(neolink_core::Error::UnintelligibleReply { reply, why })) => {
                            // Camera does not support pings just wait forever
                            log::trace!("Pings not supported: {reply:?}: {why}");
                            futures::future::pending().await
                        },
                        Ok(Err(e)) => {
                            break Err(e.into());
                        },
                        Err(_) => {
                            // Timeout. Be tolerant: flaky battery cams (Argus) intermittently stop
                            // answering get_linktype while still streaming fine. Dropping them after
                            // only ~5 missed pings (~30s) caused a reconnect loop (cf. neolink#14)
                            // that never let the stream stabilise and churned reconnects hard enough
                            // to trigger the runaway-CPU spin. A genuine connection drop is caught by
                            // camera.join() above; this ping is only a secondary liveness check, so
                            // allow many misses (~5 min) before giving up.
                            if missed_pings < MAX_MISSED_PINGS {
                                missed_pings += 1;
                                continue;
                            } else {
                                log::error!("Timed out waiting for camera ping reply");
                                break Err(anyhow::anyhow!("Timed out waiting for camera ping reply"));
                            }
                        }
                    }
                }
            } => v,
        }?;

        let _ = camera.logout().await;
        let _ = camera.shutdown().await;

        Ok(())
    }

    // Will run and attempt to maintain the connection
    //
    // A watch sender is used to send the new camera
    // whenever it changes
    pub(crate) async fn run(&mut self) -> AnyResult<()> {
        let mut backoff = MIN_BACKOFF;

        loop {
            self.state
                .clone()
                .wait_for(|state| matches!(state, NeoCamThreadState::Connected))
                .await?;
            let mut config_rec = self.config.clone();

            let config = config_rec.borrow_and_update().clone();
            let name = config.name.clone();

            let mut state = self.state.clone();

            let res = tokio::select! {
                Ok(_) = config_rec.changed() => {
                    None
                }
                Ok(_) = state.wait_for(|state| matches!(state, NeoCamThreadState::Disconnected)) => {
                    log::trace!("State changed to disconnect");
                    None
                }
                v = self.run_camera(&config, &mut backoff) => {
                    Some(v)
                }
            };
            self.camera_watch.send_replace(Weak::new());

            if res.is_none() {
                // If None go back and reload NOW
                //
                // This occurs if there was a config change
                log::trace!("Config change or Manual disconnect");
                continue;
            }

            // Else we see what the result actually was
            let result = res.unwrap();

            match result {
                Ok(()) => {
                    // Normal shutdown
                    log::trace!("Normal camera shutdown");
                    self.cancel.cancel();
                    return Ok(());
                }
                Err(e) => {
                    // An error
                    // Check if it is non-retry
                    let e_inner = e.downcast_ref::<neolink_core::Error>();
                    match e_inner {
                        Some(neolink_core::Error::CameraLoginFail) => {
                            // Fatal
                            log::error!("{name}: Login credentials were not accepted");
                            self.cancel.cancel();
                            return Err(e);
                        }
                        _ => {
                            // Non fatal
                            log::warn!("{name}: Connection Lost: {:?}", e);
                            log::info!("{name}: Attempt reconnect in {:?}", backoff);
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for NeoCamThread {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn update_camera_time(camera: &BcCamera, name: &str, update_time: bool) -> AnyResult<()> {
    let cam_time = camera.get_time().await?;
    let mut update = false;
    if let Some(time) = cam_time {
        log::info!("{}: Camera time is already set: {}", name, time);
        if update_time {
            update = true;
        }
    } else {
        update = true;
        log::warn!("{}: Camera has no time set, Updating", name);
    }
    if update {
        use std::time::SystemTime;
        let new_time = SystemTime::now();

        log::info!("{}: Setting time to {:?}", name, new_time);
        match camera.set_time(new_time.into()).await {
            Ok(_) => {
                let cam_time = camera.get_time().await?;
                if let Some(time) = cam_time {
                    log::info!("{}: Camera time is now set: {}", name, time);
                }
            }
            Err(e) => {
                log::error!(
                    "{}: Camera did not accept new time (is user an admin?): Error: {:?}",
                    name,
                    e
                );
            }
        }
    }
    Ok(())
}

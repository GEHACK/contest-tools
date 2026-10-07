use std::{
    future::Future,
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use gstreamer::{
    Bus, MessageView, Pipeline, State,
    glib::{
        Object, SignalHandlerId,
        object::{Cast, ObjectExt},
    },
    prelude::{ElementExt, GstObjectExt},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum PipelineMonitorError {
    #[error("pipeline builder failed")]
    Build,

    #[error("pipeline has no bus")]
    MissingBus,

    #[error("failed to set pipeline state to {target:?}: {message}")]
    SetState { target: State, message: String },
}

pub trait PipelineBuilder {
    fn build(&self) -> impl Future<Output = Result<Pipeline, ()>> + Send;
    fn set_running(&self, _running: bool) {}
}

struct BusConnection {
    bus: Bus,
    handler_id: SignalHandlerId,
}

impl BusConnection {
    fn disconnect(self) {
        self.bus.disconnect(self.handler_id);
        self.bus.remove_signal_watch();
    }
}

pub struct PipelineState {
    pipeline: Option<Pipeline>,
    bus_connection: Option<BusConnection>,
    state: State,
}

pub struct PipelineMonitor<T: PipelineBuilder> {
    name: String,
    builder: T,
    token: CancellationToken,
    state: RwLock<PipelineState>,
}

impl<T: PipelineBuilder + Send + Sync + 'static> PipelineMonitor<T> {
    fn connect_bus(self: &Arc<Self>, pipeline: &Pipeline, bus: &Bus) -> BusConnection {
        bus.add_signal_watch();

        let weak_monitor = Arc::downgrade(self);
        let pipeline = pipeline.clone();
        let name = self.name.clone();

        let handler_id = bus.connect_message(None, move |_, message| {
            Self::handle_message(&weak_monitor, &pipeline, &name, message);
        });

        BusConnection {
            bus: bus.clone(),
            handler_id,
        }
    }

    fn handle_message(
        monitor: &Weak<Self>,
        pipeline: &Pipeline,
        name: &str,
        message: &gstreamer::Message,
    ) {
        match message.view() {
            MessageView::StateChanged(changed)
                if message
                    .src()
                    .map(|src| src == pipeline.upcast_ref::<Object>())
                    .unwrap_or(false) =>
            {
                tracing::info!(
                    "Pipeline {name} state changed: {:?} -> {:?}",
                    changed.old(),
                    changed.current()
                );

                if let Some(monitor) = monitor.upgrade() {
                    monitor.builder.set_running(changed.current() == State::Playing);
                }
            }

            MessageView::Error(error) => {
                tracing::error!(
                    "Pipeline {name} error from {:?}: {} ({:?})",
                    error.src().map(|src| src.path_string()),
                    error.error(),
                    error.debug()
                );

                let _ = pipeline.set_state(State::Null);
                if let Some(monitor) = monitor.upgrade() {
                    monitor.schedule_restart();
                }
            }

            MessageView::Eos(..) => {
                tracing::error!("Pipeline {name} end-of-stream");

                let _ = pipeline.set_state(State::Null);
                if let Some(monitor) = monitor.upgrade() {
                    monitor.schedule_restart();
                }
            }

            _ => {}
        }
    }

    fn schedule_restart(self: &Arc<Self>) {
        let monitor = Arc::clone(self);

        tokio::spawn(async move {
            tokio::select! {
                _ = monitor.token.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }

            if let Err(error) = monitor.restart().await {
                tracing::error!(%error, pipeline=monitor.name, "Failed to restart pipeline");
                monitor.schedule_restart();
            }
        });
    }

    fn install_pipeline(self: &Arc<Self>, pipeline: Pipeline) -> Result<(), PipelineMonitorError> {
        let bus = pipeline.bus().ok_or(PipelineMonitorError::MissingBus)?;

        let connection = self.connect_bus(&pipeline, &bus);

        if let Err(error) = pipeline.set_state(State::Playing) {
            connection.disconnect();
            return Err(PipelineMonitorError::SetState {
                target: State::Playing,
                message: error.to_string(),
            });
        }

        let old_connection = {
            let mut state = self.state.write().expect("Lock poisoning occurred");
            state.pipeline = Some(pipeline);
            state.state = State::Playing;
            state.bus_connection.replace(connection)
        };

        if let Some(connection) = old_connection {
            connection.disconnect();
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.builder.set_running(false);
        let (pipeline, connection) = {
            let mut state = self.state.write().expect("Lock poisoning occurred");
            state.state = State::Null;
            (state.pipeline.take(), state.bus_connection.take())
        };

        if let Some(connection) = connection {
            connection.disconnect();
        }
        if let Some(pipeline) = pipeline {
            let _ = pipeline.set_state(State::Null);
        }
    }

    async fn restart(self: &Arc<Self>) -> Result<(), PipelineMonitorError> {
        self.stop();

        if self.token.is_cancelled() {
            return Ok(());
        }

        let pipeline = self
            .builder
            .build()
            .await
            .map_err(|()| PipelineMonitorError::Build)?;
        self.install_pipeline(pipeline)
    }

    pub async fn start(name: impl Into<String>, builder: T, token: CancellationToken) -> Arc<Self> {
        let monitor = Arc::new(Self {
            name: name.into(),
            builder,
            token,
            state: RwLock::new(PipelineState {
                pipeline: None,
                bus_connection: None,
                state: State::Null,
            }),
        });

        if let Err(error) = monitor.restart().await {
            tracing::error!(%error, pipeline=monitor.name, "Initial pipeline build failed");
            monitor.schedule_restart();
        }

        return monitor;
    }
}

use std::sync::Arc;

use gstreamer::Pipeline;

use crate::{
    app_pipeline_builder::AppPipelineBuilder, dbus_client::DBus,
    pipeline_distributor::PipelineDistributor, pipeline_monitor::PipelineBuilder,
    screencast::Screencast,
};

pub struct ScreencastPipelineBuilder {
    pipeline_distributor: Arc<PipelineDistributor>,
    encoder: String,
}

impl ScreencastPipelineBuilder {
    pub fn new(pipeline_distributor: Arc<PipelineDistributor>, encoder: String) -> Self {
        Self {
            pipeline_distributor,
            encoder,
        }
    }
}

impl PipelineBuilder for ScreencastPipelineBuilder {
    fn build(&self) -> impl Future<Output = Result<Pipeline, ()>> + Send {
        let distributor = Arc::clone(&self.pipeline_distributor);
        let encoder = self.encoder.clone();

        async move {
            let dbus = match DBus::connect().await {
                Ok(dbus) => dbus,
                Err(error) => {
                    tracing::error!(%error, "failed to connect to D-Bus");
                    return Err(());
                }
            };
            tracing::info!("connected to D-Bus");
            let screencast = match Screencast::start(&dbus).await {
                Ok(screencast) => screencast,
                Err(error) => {
                    tracing::error!(%error, "failed to start the screencast");
                    return Err(());
                }
            };
            tracing::info!("started the screencast");
            let src = format!(
                "pipewiresrc on-disconnect=eos path={} keepalive-time=100",
                screencast.pipewire_node_id
            );

            let builder = AppPipelineBuilder::new(Arc::clone(&distributor), src, encoder, false);

            let pipeline = builder.build().await?;
            tracing::info!("Started the screencast pipeline.");
            Ok(pipeline)
        }
    }

    fn set_running(&self, running: bool) {
        *self
            .pipeline_distributor
            .is_running
            .write()
            .expect("Lock poisoning occurred") = running;
    }
}

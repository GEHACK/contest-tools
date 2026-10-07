use std::sync::Arc;

use gstreamer::Pipeline;

use crate::{
    app_pipeline_builder::AppPipelineBuilder, pipeline_distributor::PipelineDistributor,
    pipeline_monitor::PipelineBuilder,
};

pub struct WebcamPipelineBuilder {
    pipeline_distributor: Arc<PipelineDistributor>,
    encoder: String,
    webcam_device: String,
}

impl WebcamPipelineBuilder {
    pub fn new(
        pipeline_distributor: Arc<PipelineDistributor>,
        encoder: String,
        webcam_device: String,
    ) -> Self {
        Self {
            pipeline_distributor,
            encoder,
            webcam_device,
        }
    }
}

impl PipelineBuilder for WebcamPipelineBuilder {
    fn build(&self) -> impl Future<Output = Result<Pipeline, ()>> + Send {
        let builder = AppPipelineBuilder::new(
            Arc::clone(&self.pipeline_distributor),
            format!("v4l2src device={} ! decodebin", self.webcam_device),
            self.encoder.clone(),
            true,
        );

        async move {
            builder.build().await.map(|pipeline| {
                tracing::info!("Started the webcam pipeline.");
                pipeline
            })
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

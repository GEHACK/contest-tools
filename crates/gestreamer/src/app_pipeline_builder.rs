use std::{future::Future, io, sync::Arc};

use gstreamer::{Pipeline, prelude::*};
use gstreamer_app::{AppSink, AppSinkCallbacks};

use crate::{pipeline_distributor::PipelineDistributor, pipeline_monitor::PipelineBuilder};

pub struct AppPipelineBuilder {
    pipeline_distributor: Arc<PipelineDistributor>,
    src: String,
    encoder: String,
    audio: bool,
}

impl AppPipelineBuilder {
    pub fn new(
        pipeline_distributor: Arc<PipelineDistributor>,
        src: impl Into<String>,
        encoder: impl Into<String>,
        audio: bool,
    ) -> Self {
        Self {
            pipeline_distributor,
            src: src.into(),
            encoder: encoder.into(),
            audio,
        }
    }

    fn build_pipeline(
        pipeline_distributor: Arc<PipelineDistributor>,
        src: &str,
        encoder: &str,
        audio: bool,
    ) -> Result<Pipeline, Box<dyn std::error::Error + Send + Sync>> {
        let max_buffers = crate::pipeline_distributor::CHANNEL_SIZE + 1;
        let alignment = crate::pipeline_distributor::PACKETS_IN_BUFFER;

        let mut description = format!(
            "{src} !
             videoconvert !
             {encoder} !
             queue !
             mpegtsmux name=mux alignment={alignment} !
             appsink name=ts_sink emit-signals=true sync=false max-buffers={max_buffers}"
        );

        if audio {
            description.push_str(
                "
                pipewiresrc on-disconnect=eos !
                audioconvert !
                audioresample !
                audio/x-raw,rate=48000 !
                fdkaacenc !
                aacparse !
                queue !
                mux.",
            );
        }

        let pipeline = gstreamer::parse::launch(&description)?
            .downcast::<Pipeline>()
            .map_err(|_| io::Error::other("Parsed element is not a pipeline"))?;

        let sink = pipeline
            .by_name("ts_sink")
            .ok_or_else(|| io::Error::other("Appsink 'ts_sink' not found"))?
            .downcast::<AppSink>()
            .map_err(|_| io::Error::other("'ts_sink' is not an appsink"))?;

        sink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|err| {
                        tracing::error!(error = %err.message, "failed to pull sample data");
                        gstreamer::FlowError::Error
                    })?;

                    pipeline_distributor.write_sample(&sample);
                    Ok(gstreamer::FlowSuccess::Ok)
                })
                .build(),
        );

        if audio {
            pipeline.use_clock(Some(&gstreamer::SystemClock::obtain()));
            pipeline.set_start_time(gstreamer::ClockTime::NONE);
        }

        Ok(pipeline)
    }
}

impl PipelineBuilder for AppPipelineBuilder {
    fn build(&self) -> impl Future<Output = Result<Pipeline, ()>> + Send {
        let distributor = Arc::clone(&self.pipeline_distributor);
        let src = self.src.clone();
        let encoder = self.encoder.clone();
        let audio = self.audio;

        async move {
            Self::build_pipeline(distributor, &src, &encoder, audio).map_err(|error| {
                tracing::error!(%error, "failed to create pipeline");
            })
        }
    }
}

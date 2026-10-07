use std::sync::Arc;

use actix_web::{
    App, HttpResponse, HttpServer, get,
    web::{self, Data},
};
use clap::Parser;
use shared::setup_logging;
use tokio_util::sync::CancellationToken;

use crate::{
    args::Args, pipeline_distributor::PipelineDistributor, pipeline_monitor::PipelineMonitor,
    screencast_pipeline_builder::ScreencastPipelineBuilder,
    webcam_pipeline_builder::WebcamPipelineBuilder,
};

mod app_pipeline_builder;
mod args;
mod dbus_client;
mod pipeline_distributor;
mod pipeline_monitor;
mod screencast;
mod screencast_pipeline_builder;
mod webcam_pipeline_builder;

#[get("/screencast.ts")]
async fn get_screencast(state: web::Data<Arc<AppState>>) -> HttpResponse {
    state.screencast_pipeline.handle_stream().await
}

#[get("/webcam.ts")]
async fn get_webcam(state: web::Data<Arc<AppState>>) -> HttpResponse {
    state.webcam_pipeline.handle_stream().await
}

#[derive(Clone)]
struct AppState {
    screencast_pipeline: Arc<PipelineDistributor>,
    webcam_pipeline: Arc<PipelineDistributor>,
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    setup_logging!("info");
    let args = Args::parse();
    let shutdown = CancellationToken::new();

    gstreamer::init().expect("Unable to initialize gstreamer");

    let screencast_pipeline: Arc<PipelineDistributor> = Arc::new(*PipelineDistributor::new());
    if args.screencast {
        let builder =
            ScreencastPipelineBuilder::new(Arc::clone(&screencast_pipeline), args.encoder.clone());
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let monitor = PipelineMonitor::start("screencast", builder, token.clone()).await;
            token.cancelled().await;
            monitor.stop();
        });
    }

    let webcam_pipeline: Arc<PipelineDistributor> = Arc::new(*PipelineDistributor::new());
    if let Some(device) = &args.webcam {
        let builder = WebcamPipelineBuilder::new(
            Arc::clone(&webcam_pipeline),
            args.encoder.clone(),
            device.clone(),
        );
        let token = shutdown.child_token();

        tokio::spawn(async move {
            let monitor = PipelineMonitor::start("webcam", builder, token.clone()).await;
            token.cancelled().await;
            monitor.stop();
        });
    }

    let state: Arc<AppState> = Arc::new(AppState {
        screencast_pipeline,
        webcam_pipeline,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(Arc::clone(&state)))
            .service(get_screencast)
            .service(get_webcam)
    })
    .bind((args.bind_address, args.port))?
    .run()
    .await?;

    shutdown.cancel();

    return Ok(());
}

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    web::{self, Data},
};
use clap::Parser;
use gstreamer::{
    ClockTime, MessageView, Object, Pipeline, Sample, State, SystemClock, glib::object::Cast,
    prelude::*,
};
use gstreamer_app::AppSink;
use shared::setup_logging;
use tokio_util::sync::CancellationToken;

use crate::{
    args::Args,
    dbus_client::DBus,
    pipeline_distributor::PipelineDistributor,
    screencast::{Screencast, ScreencastError},
};

mod args;
mod dbus_client;
mod pipeline_distributor;
mod screencast;

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

async fn ensure_dbus() -> DBus {
    tracing::info!("connecting to D-Bus");
    loop {
        let result = DBus::connect().await;
        if let Ok(dbus) = result {
            tracing::info!("connected to D-Bus");
            return dbus;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn ensure_screencast(dbus: &DBus) -> Screencast {
    tracing::info!("starting the screencast");
    loop {
        let result = Screencast::start(&dbus).await;
        if let Ok(screencast) = result {
            tracing::info!("started the screencast");
            return screencast;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

fn monitor_pipeline(name: &str, pipeline: Pipeline, token: CancellationToken) {
    tracing::info!("Starting the monitoring of pipeline {name}...");
    let bus = pipeline.bus().expect("Should have a bus");
    bus.add_signal_watch();
    while !token.is_cancelled() {
        let Some(message) = bus.timed_pop(gstreamer::ClockTime::from_mseconds(200)) else {
            continue;
        };
        match message.view() {
            MessageView::StateChanged(state_changed)
                if message
                    .src()
                    .map(|src| src == pipeline.upcast_ref::<Object>())
                    .unwrap_or(false) =>
            {
                tracing::info!(
                    "Pipeline {name} state changed: {:?} -> {:?}",
                    state_changed.old(),
                    state_changed.current()
                );
            }

            MessageView::Error(error) => {
                tracing::error!(
                    "Pipeline {name} error from {:?}: {} ({:?})",
                    error.src().map(|src| src.path_string()),
                    error.error(),
                    error.debug()
                );
                break;
            }

            MessageView::Eos(..) => break,

            _ => {}
        }
    }
    let _ = pipeline.set_state(State::Null);
}

async fn start_screencast(
    pipeline_distributor: Arc<PipelineDistributor>,
    encoder: String,
    token: CancellationToken,
) {
    let dbus: DBus = ensure_dbus().await;
    let screencast: Screencast = ensure_screencast(&dbus).await;
    let src: String = format!(
        "pipewiresrc on-disconnect=eos path={} keepalive-time=100",
        screencast.pipewire_node_id
    );
    tracing::info!("Starting the screencast pipeline...");
    match create_pipeline(Arc::clone(&pipeline_distributor), &src, &encoder, false) {
        Ok(pipeline) => {
            {
                let mut is_running = pipeline_distributor
                    .is_running
                    .write()
                    .expect("Lock poisoning occurred");
                *is_running = true;
            }
            tracing::info!("Started the screencast pipeline.");
            // The bus loop blocks, so run it on Tokio's blocking pool.
            let _ = tokio::task::spawn_blocking(move || {
                monitor_pipeline("screencast", pipeline, token);
            })
            .await;
        }
        Err(e) => tracing::error!(error = %e, "failed to start gstreamer pipeline for screencast"),
    }
}

async fn start_webcam(
    pipeline_distributor: Arc<PipelineDistributor>,
    encoder: String,
    webcam_device: String,
    token: CancellationToken,
) {
    let src: String = format!("v4l2src device={} ! decodebin", webcam_device);
    tracing::info!("Starting the webcam pipeline...");
    match create_pipeline(Arc::clone(&pipeline_distributor), &src, &encoder, true) {
        Ok(pipeline) => {
            {
                let mut is_running = pipeline_distributor
                    .is_running
                    .write()
                    .expect("Lock poisoning occurred");
                *is_running = true;
            }
            tracing::info!("Started the webcam pipeline.");
            // The bus loop blocks, so run it on Tokio's blocking pool.
            let _ = tokio::task::spawn_blocking(move || {
                monitor_pipeline("webcam", pipeline, token);
            })
            .await;
        }
        Err(e) => tracing::error!(error = %e, "failed to start gstreamer pipeline for webcam"),
    }
}

fn create_pipeline(
    pipeline_distributor: Arc<PipelineDistributor>,
    src: &str,
    encoder: &str,
    audio: bool,
) -> Result<Pipeline, Box<dyn Error + Send + Sync>> {
    // Prevent encode buffer starvation due to lagging broadcast consumers.
    // This is needed due to the zero-copy implementation all the way to the HTTP clients.
    let max_buffers = crate::pipeline_distributor::CHANNEL_SIZE + 1;
    let mut pipeline_description: String = format!(
        "
{src} !
videoconvert !
{encoder} !
queue !
mpegtsmux name=mux alignment=7 !
appsink name=ts_sink
        emit-signals=true
        sync=false
        max-buffers={max_buffers}"
    );

    if audio {
        pipeline_description += &format!(
            "
pipewiresrc on-disconnect=eos !
audioconvert !
audioresample !
audio/x-raw,rate=48000 !
fdkaacenc !
aacparse !
queue !
mux."
        );
    }

    let pipeline: Pipeline = gstreamer::parse::launch(&pipeline_description)?
        .downcast::<Pipeline>()
        .map_err(|_| "Pipeline is not the expected type")?;

    let sink: AppSink = pipeline
        .by_name("ts_sink")
        .expect("The appsink named ts_sink not found")
        .downcast::<AppSink>()
        .map_err(|_| "ts_sink is not an appsink")?;

    let f = move |sink: &AppSink| {
        let sample: Sample = sink.pull_sample().map_err(|err| {
            tracing::error!(error = %err.message, "failed to pull sample data");
            return gstreamer::FlowError::Error;
        })?;
        pipeline_distributor.write_sample(&sample);

        Ok(gstreamer::FlowSuccess::Ok)
    };

    sink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(f)
            .build(),
    );

    if audio {
        // Force a pipeline clock to prevent clock issues between video and audio clocks
        pipeline.use_clock(Some(&SystemClock::obtain()));
        pipeline.set_start_time(None);
    }

    let _ = pipeline.set_state(gstreamer::State::Playing)?;
    return Ok(pipeline);
}

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
        let distributor = Arc::clone(&screencast_pipeline);
        let encoder: String = args.encoder.clone();
        let token = shutdown.child_token();
        tokio::spawn(async move {
            start_screencast(distributor, encoder, token).await;
        });
    }

    let webcam_pipeline: Arc<PipelineDistributor> = Arc::new(*PipelineDistributor::new());
    if let Some(device) = &args.webcam {
        let distributor = Arc::clone(&webcam_pipeline);
        let encoder: String = args.encoder.clone();
        let device = device.clone();
        let token = shutdown.child_token();
        tokio::spawn(async move {
            start_webcam(distributor, encoder, device, token).await;
        });
    }

    let state: Arc<AppState> = Arc::new(AppState {
        screencast_pipeline,
        webcam_pipeline,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(Arc::clone(&state)))
            .service(hello)
            .service(get_screencast)
            .service(get_webcam)
    })
    .bind((args.bind_address, args.port))?
    .run()
    .await?;

    shutdown.cancel();

    return Ok(());
}

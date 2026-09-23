use std::{error::Error, sync::Arc};

use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    web::{self, Bytes, Data},
};
use clap::Parser;
use gstreamer::{Pipeline, Sample, glib::object::Cast, prelude::*};
use gstreamer_app::AppSink;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    args::Args, dbus_client::DBus, pipeline_distributor::PipelineDistributor,
    screencast::Screencast,
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
    println!("Connecting to D-Bus...");
    loop {
        let result = DBus::connect().await;
        if let Ok(dbus) = result {
            println!("Connected to D-Bus");
            return dbus;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn ensure_screencast(dbus: &DBus) -> Screencast {
    println!("Starting the screencast...");
    loop {
        let result = Screencast::start(&dbus).await;
        if let Ok(screencast) = result {
            println!("Started the screencast");
            return screencast;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn start_screencast(pipeline_distributor: Arc<PipelineDistributor>, encoder: String) {
    let dbus: DBus = ensure_dbus().await;
    let screencast: Screencast = ensure_screencast(&dbus).await;
    let src: String = format!(
        "pipewiresrc on-disconnect=eos path={} keepalive-time=100",
        screencast.pipewire_node_id
    );
    match create_pipeline(Arc::clone(&pipeline_distributor), &src, &encoder, false) {
        Ok(()) => {
            let mut is_running = pipeline_distributor
                .is_running
                .write()
                .expect("Lock poisoning occurred");
            *is_running = true;
        }
        Err(e) => eprintln!("Failed to start gstreamer pipeline for screencast: {}", e),
    }
}

fn create_pipeline(
    pipeline_distributor: Arc<PipelineDistributor>,
    src: &str,
    encoder: &str,
    audio: bool,
) -> Result<(), Box<dyn Error>> {
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
        max-buffers=10"
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
            eprintln!("Failed to pull sample data: {}", err.message);
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

    pipeline.set_state(gstreamer::State::Playing);
    return Ok(());
}

#[get("/screencast.ts")]
async fn get_screencast(state: web::Data<Arc<AppState>>) -> HttpResponse {
    if *state
        .screencast_pipeline
        .is_running
        .read()
        .expect("Lock poisoning occurred")
    {
        let stream: BroadcastStream<Bytes> = state.screencast_pipeline.create_stream();
        return HttpResponse::Ok()
            .content_type("video/mp2t")
            .keep_alive()
            .append_header(("Cache-Control", "no-cache"))
            .streaming(stream);
    }
    return HttpResponse::ServiceUnavailable().body("Screencast is not available");
}

#[derive(Clone)]
struct AppState {
    screencast_pipeline: Arc<PipelineDistributor>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    gstreamer::init().expect("Unable to initialize gstreamer");

    let screencast_pipeline: Arc<PipelineDistributor> = Arc::new(*PipelineDistributor::new());
    if args.screencast {
        tokio::spawn(start_screencast(
            Arc::clone(&screencast_pipeline),
            args.encoder.clone(),
        ));
    }

    let state: Arc<AppState> = Arc::new(AppState {
        screencast_pipeline,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(Arc::clone(&state)))
            .service(hello)
            .service(get_screencast)
    })
    .bind((args.bind_address, args.port))?
    .run()
    .await
}

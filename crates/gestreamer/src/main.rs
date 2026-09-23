use std::sync::Arc;

use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    web::{self, Bytes, Data},
};
use clap::Parser;
use gstreamer::{
    Pipeline, Sample,
    glib::{Error, object::Cast},
    prelude::*,
};
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

async fn start_screencast(encoder: &str) -> Result<Arc<PipelineDistributor>, ()> {
    let dbus: DBus = ensure_dbus().await;
    let screencast: Screencast = ensure_screencast(&dbus).await;
    let src: String = format!(
        "pipewiresrc on-disconnect=eos path={} keepalive-time=100",
        screencast.pipewire_node_id
    );
    return create_pipeline(&src, encoder, false).map_err(|err| {
        eprintln!("Failed to start gstreamer pipeline for screencast: {}", err);
    });
}

fn create_pipeline(
    src: &str,
    encoder: &str,
    audio: bool,
) -> Result<Arc<PipelineDistributor>, Error> {
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
        .expect("Pipeline is not the expected type");

    let sink: AppSink = pipeline
        .by_name("ts_sink")
        .expect("The appsink named ts_sink not found")
        .downcast::<AppSink>()
        .expect("my_sink is not an appsink");

    let pipeline_distributor: Arc<PipelineDistributor> = Arc::new(*PipelineDistributor::new());

    let callback_distributor = Arc::clone(&pipeline_distributor);
    let f = move |sink: &AppSink| {
        let sample: Sample = sink.pull_sample().map_err(|err| {
            eprintln!("Failed to pull sample data: {}", err.message);
            return gstreamer::FlowError::Error;
        })?;
        callback_distributor.write_sample(&sample);

        Ok(gstreamer::FlowSuccess::Ok)
    };

    sink.set_callbacks(
        gstreamer_app::AppSinkCallbacks::builder()
            .new_sample(f)
            .build(),
    );

    pipeline.set_state(gstreamer::State::Playing);

    return Ok(pipeline_distributor);
}

#[get("/screencast.ts")]
async fn get_screencast(state: web::Data<Arc<PipelineDistributor>>) -> HttpResponse {
    let stream: BroadcastStream<Bytes> = state.create_stream();
    HttpResponse::Ok()
        .content_type("video/mp2t")
        .keep_alive()
        .append_header(("Cache-Control", "no-cache"))
        .streaming(stream)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    gstreamer::init().expect("Unable to initialize gstreamer");
    let distributor = start_screencast(&args.encoder).await.unwrap();
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(Arc::clone(&distributor)))
            .service(hello)
            .service(get_screencast)
    })
    .bind((args.bind_address, args.port))?
    .run()
    .await
}

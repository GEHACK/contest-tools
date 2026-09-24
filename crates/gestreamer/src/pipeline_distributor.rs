use std::sync::RwLock;

use actix_web::web::Bytes;
use gstreamer::Sample;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_stream::wrappers::BroadcastStream;

pub const MPEGTS_PACKET_SIZE: usize = 188;
pub const PACKETS_IN_BUFFER: usize = 7;
pub const MPEGTS_BUFFER_SIZE: usize = MPEGTS_PACKET_SIZE * PACKETS_IN_BUFFER;
pub const CHANNEL_SIZE: usize = 512;

pub type MpegTsBuffer = Bytes;

pub struct PipelineDistributor {
    sender: Sender<MpegTsBuffer>,
    pub is_running: RwLock<bool>,
}

impl PipelineDistributor {
    pub fn new() -> Box<PipelineDistributor> {
        let (tx, _) = broadcast::channel::<MpegTsBuffer>(CHANNEL_SIZE);
        let pd = Box::new(PipelineDistributor {
            sender: tx,
            is_running: RwLock::new(false),
        });
        return pd;
    }

    pub fn create_stream(&self) -> BroadcastStream<Bytes> {
        let rx: Receiver<MpegTsBuffer> = self.sender.subscribe();
        return BroadcastStream::new(rx);
    }

    pub fn write_sample(&self, sample: &Sample) {
        let Some(buffer) = sample.buffer_owned() else {
            tracing::error!("failed to get sample buffer");
            return;
        };
        if buffer.size() != MPEGTS_BUFFER_SIZE {
            tracing::error!("dropping an MPEG-TS buffer of invalid size");
            return;
        }
        let Ok(mapped) = buffer.into_mapped_buffer_readable() else {
            tracing::error!("failed to map sample buffer");
            return;
        };
        let _ = self.sender.send(Bytes::from_owner(mapped));
    }
}

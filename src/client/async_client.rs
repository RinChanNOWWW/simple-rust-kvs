use crate::{
    network::{Request, Response},
    KvsError, Result,
};
use futures::{SinkExt, StreamExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream, ToSocketAddrs,
};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct KvsClient {
    reader: tokio_serde::SymmetricallyFramed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        Response,
        SymmetricalJson<Response>,
    >,
    writer: tokio_serde::SymmetricallyFramed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        Request,
        SymmetricalJson<Request>,
    >,
}

impl KvsClient {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<KvsClient> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();

        let reader = tokio_serde::SymmetricallyFramed::new(
            FramedRead::new(read_half, LengthDelimitedCodec::new()),
            SymmetricalJson::<Response>::default(),
        );
        let writer = tokio_serde::SymmetricallyFramed::new(
            FramedWrite::new(write_half, LengthDelimitedCodec::new()),
            SymmetricalJson::<Request>::default(),
        );

        Ok(KvsClient { reader, writer })
    }

    pub async fn get(mut self, key: String) -> Result<Option<String>> {
        let resp = self.send_data(Request::Get { key }).await?;
        match resp {
            Response::Get(s) => Ok(s),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub async fn set(mut self, key: String, value: String) -> Result<()> {
        let resp = self.send_data(Request::Set { key, value }).await?;
        match resp {
            Response::Set => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }
    pub async fn remove(mut self, key: String) -> Result<()> {
        let resp = self.send_data(Request::Remove { key }).await?;
        match resp {
            Response::Remove => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e)),
            _ => Err(KvsError::WrongCommandError),
        }
    }

    async fn send_data(&mut self, req: Request) -> Result<Response> {
        self.writer.send(req).await?;
        match self.reader.next().await {
            Some(Ok(resp)) => Ok(resp),
            _ => Err(KvsError::OtherError("some error".to_string())),
        }
    }
}

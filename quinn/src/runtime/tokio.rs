use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};

use tokio::{
    io::Interest,
    time::{Sleep, sleep_until},
};

use super::{AsyncTimer, AsyncUdpSocket, Runtime};

/// A Quinn runtime for Tokio
#[derive(Debug)]
pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(sleep_until(t.into()))
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }

    fn wrap_udp_socket(&self, sock: std::net::UdpSocket) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Ok(Box::new(UdpSocket {
            socket: Arc::new(UdpSocketInner {
                inner: udp::UdpSocketState::new((&sock).into())?,
                io: tokio::net::UdpSocket::from_std(sock)?,
            }),
        }))
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

impl AsyncTimer for Sleep {
    fn reset(self: Pin<&mut Self>, t: Instant) {
        Self::reset(self, t.into())
    }
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Future::poll(self, cx)
    }
}

#[derive(Debug)]
struct UdpSocketInner {
    io: tokio::net::UdpSocket,
    inner: udp::UdpSocketState,
}

#[derive(Debug, Clone)]
struct UdpSocket {
    socket: Arc<UdpSocketInner>,
}

impl AsyncUdpSocket for UdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn super::UdpSender>> {
        Box::pin(TokioUdpSender::new(self.socket.clone()))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.socket.io.poll_recv_ready(cx))?;

            match self.socket.io.try_io(Interest::READABLE, || {
                self.socket.inner.recv((&self.socket.io).into(), bufs, meta)
            }) {
                Ok(res) => return Poll::Ready(Ok(res)),
                // `try_io` clears readiness only for `WouldBlock`. Looping on any other
                // error would spin: readiness stays asserted, so `poll_recv_ready`
                // completes at once and the recv fails again, without ever yielding.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.socket.io.local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.socket.inner.may_fragment()
    }

    fn max_receive_segments(&self) -> usize {
        self.socket.inner.gro_segments()
    }
}

#[derive(Debug)]
struct TokioUdpSender {
    last_send_error: Option<Instant>,
    socket: Arc<UdpSocketInner>,
}

impl TokioUdpSender {
    fn new(socket: Arc<UdpSocketInner>) -> Self {
        Self {
            last_send_error: None,
            socket,
        }
    }
}

impl super::UdpSender for TokioUdpSender {
    fn poll_send(
        mut self: Pin<&mut Self>,
        transmit: &udp::Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            ready!(self.socket.io.poll_send_ready(cx)?);

            match self
                .socket
                .inner
                .try_send((&self.socket.io).into(), transmit)
            {
                // We thought the socket was writable, but it wasn't, then retry so that either another
                // `poll_send_ready()` call determines that the socket is indeed not writable and
                // registers us for a wakeup, or the send succeeds if this really was just a
                // transient failure.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => {
                    super::log_sendmsg_error(&mut self.last_send_error, &e, transmit);
                    return Poll::Ready(Ok(()));
                }
                Ok(()) => return Poll::Ready(Ok(())),
            }
        }
    }

    fn max_transmit_segments(&self) -> usize {
        self.socket.inner.max_gso_segments()
    }
}

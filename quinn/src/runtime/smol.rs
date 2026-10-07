use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use std::{io, sync::Arc, task::ready};

use async_io::Async;
use async_io::Timer;

use super::{AsyncTimer, AsyncUdpSocket, Runtime, UdpSender};

/// A Quinn runtime for smol
#[derive(Debug)]
pub struct SmolRuntime;

impl Runtime for SmolRuntime {
    fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(Timer::at(t))
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        ::smol::spawn(future).detach();
    }

    fn wrap_udp_socket(&self, sock: std::net::UdpSocket) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Ok(Box::new(UdpSocket::new(sock)?))
    }
}

impl AsyncTimer for Timer {
    fn reset(mut self: Pin<&mut Self>, t: Instant) {
        self.set_at(t)
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Future::poll(self, cx).map(|_| ())
    }
}

#[derive(Debug)]
struct UdpSocketInner {
    inner: udp::UdpSocketState,
    io: Async<std::net::UdpSocket>,
}

#[derive(Debug, Clone)]
struct UdpSocket {
    socket: Arc<UdpSocketInner>,
}

impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            socket: Arc::new(UdpSocketInner {
                inner: udp::UdpSocketState::new((&sock).into())?,
                io: Async::new_nonblocking(sock)?,
            }),
        })
    }
}

impl AsyncUdpSocket for UdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(SmolUdpSender::new(self.socket.clone()))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.socket.io.poll_readable(cx))?;

            match self.socket.inner.recv((&self.socket.io).into(), bufs, meta) {
                Ok(res) => return Poll::Ready(Ok(res)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.socket.io.as_ref().local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.socket.inner.may_fragment()
    }

    fn max_receive_segments(&self) -> usize {
        self.socket.inner.gro_segments()
    }
}

#[derive(Debug)]
struct SmolUdpSender {
    last_send_error: Option<Instant>,
    socket: Arc<UdpSocketInner>,
}

impl SmolUdpSender {
    fn new(socket: Arc<UdpSocketInner>) -> Self {
        Self {
            last_send_error: None,
            socket,
        }
    }
}

impl UdpSender for SmolUdpSender {
    fn poll_send(
        mut self: Pin<&mut Self>,
        transmit: &udp::Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            ready!(self.socket.io.poll_writable(cx)?);

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

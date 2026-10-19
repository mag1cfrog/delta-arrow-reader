//! Keeps an operation's subscriber and parent span with its work.
//!
//! Capture the context at the operation boundary. Use `in_scope` for synchronous
//! work, or `bind` to restore the context whenever a future or stream is polled
//! or dropped. Tracing guards only live for one poll or synchronous call.

use futures_util::Stream;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{Dispatch, Span};

#[derive(Clone)]
pub(crate) struct TracingContext {
    dispatch: Dispatch,
    parent_span: Span,
}

impl TracingContext {
    pub(crate) fn capture() -> Self {
        Self {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            parent_span: Span::current(),
        }
    }

    pub(crate) fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || {
            let _restore_caller_spans =
                SuspendedSpans::exit_until(&self.dispatch, &self.parent_span);
            self.parent_span.in_scope(work)
        })
    }

    pub(crate) fn bind<T>(&self, work: T) -> WithTracingContext<T> {
        WithTracingContext {
            work: Some((self.clone(), Box::pin(work))),
        }
    }
}

pub(crate) fn spawn_blocking<F, T>(work: F) -> tokio::task::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let context = TracingContext::capture();
    tokio::task::spawn_blocking(move || context.in_scope(work))
}

// WithSubscriber scopes polling only; this wrapper also scopes cleanup.
pub(crate) struct WithTracingContext<T> {
    // Work and context have one lifetime. None means they have both been released.
    work: Option<(TracingContext, Pin<Box<T>>)>,
}

impl<T> WithTracingContext<T> {
    fn drop_work(&mut self) {
        if let Some((context, work)) = self.work.take() {
            // Moving the pinned Box leaves the work in place until it is dropped in scope.
            context.in_scope(|| drop(work));
        }
    }
}

impl<T: Future> Future for WithTracingContext<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some((context, work)) = this.work.as_mut() else {
            unreachable!("completed future polled again");
        };
        let result = context.in_scope(|| work.as_mut().poll(cx));
        if result.is_ready() {
            this.drop_work();
        }
        result
    }
}

// Reader streams end on their first error, even if the caller never polls for EOF.
impl<S, T, E> Stream for WithTracingContext<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some((context, work)) = this.work.as_mut() else {
            return Poll::Ready(None);
        };
        let result = context.in_scope(|| work.as_mut().poll_next(cx));
        if matches!(result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.drop_work();
        }
        result
    }
}

impl<T> Drop for WithTracingContext<T> {
    fn drop(&mut self) {
        self.drop_work();
    }
}

// Entering a span does not always make it current: Span::none() does nothing,
// and Registry ignores repeated entries of an already active span. Exit later
// caller scopes first, then restore them when the bound work returns or unwinds.
struct SuspendedSpans<'a> {
    dispatch: &'a Dispatch,
    spans: Vec<Span>,
    // This guard must restore spans on the same thread that exited them.
    _thread_bound: PhantomData<*const ()>,
}

impl<'a> SuspendedSpans<'a> {
    fn exit_until(dispatch: &'a Dispatch, parent_span: &Span) -> Self {
        let mut suspended = Self {
            dispatch,
            spans: Vec::new(),
            _thread_bound: PhantomData,
        };
        let parent_id = parent_span.id();
        loop {
            let current_span = Span::current();
            let Some(current_id) = current_span.id() else {
                break;
            };
            if Some(&current_id) == parent_id.as_ref() {
                break;
            }
            // Keep exited spans alive until Drop re-enters them, including during unwind.
            suspended.spans.push(current_span);
            dispatch.exit(&current_id);
        }
        suspended
    }
}

impl Drop for SuspendedSpans<'_> {
    fn drop(&mut self) {
        for span in self.spans.iter().rev() {
            if let Some(id) = span.id() {
                self.dispatch.enter(&id);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;

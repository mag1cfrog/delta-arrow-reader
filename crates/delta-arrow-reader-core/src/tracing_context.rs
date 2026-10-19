//! Request context across polling, destruction, and blocking handoffs.

use futures_util::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{Dispatch, Span};

#[derive(Clone)]
pub(crate) struct TracingContext {
    dispatch: Dispatch,
    span: Span,
}

impl TracingContext {
    pub(crate) fn capture() -> Self {
        Self {
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: Span::current(),
        }
    }

    pub(crate) fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || self.span.in_scope(work))
    }

    pub(crate) fn bind<T>(&self, work: T) -> WithContext<T> {
        WithContext {
            inner: Some(Box::pin(work)),
            context: Some(self.clone()),
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

// WithSubscriber scopes polling only. Cleanup must use the same dispatch too.
// Boxing keeps the inner value pinned while allowing its Drop to run in scope.
pub(crate) struct WithContext<T> {
    inner: Option<Pin<Box<T>>>,
    context: Option<TracingContext>,
}

impl<T> WithContext<T> {
    fn clear(&mut self) {
        if let Some(context) = self.context.take() {
            context.in_scope(|| drop(self.inner.take()));
        }
    }
}

impl<T: Future> Future for WithContext<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let (Some(context), Some(inner)) = (&this.context, &mut this.inner) else {
            unreachable!("completed future polled again");
        };
        let result = context.in_scope(|| inner.as_mut().poll(cx));
        if result.is_ready() {
            this.clear();
        }
        result
    }
}

impl<T: Stream> Stream for WithContext<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let (Some(context), Some(inner)) = (&this.context, &mut this.inner) else {
            return Poll::Ready(None);
        };
        let result = context.in_scope(|| inner.as_mut().poll_next(cx));
        if matches!(result, Poll::Ready(None)) {
            this.clear();
        }
        result
    }
}

impl<T> Drop for WithContext<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::test_tracing::Collector;
    use futures_util::FutureExt;
    use tracing::{Level, subscriber::with_default};

    struct Cleanup;
    impl Drop for Cleanup {
        fn drop(&mut self) {
            tracing::info!(target: "delta_arrow_reader::test", event = "work.cleanup");
        }
    }

    #[test]
    fn polling_and_drop_restore_the_previous_context_and_release_the_owner() {
        for pending in [false, true] {
            let owner = Collector::new(Level::TRACE);
            let other = Collector::new(Level::TRACE);
            let span = with_default(
                owner.clone(),
                || tracing::info_span!(target: "delta_arrow_reader::test", "owner"),
            );
            let parent = span.id().unwrap().into_u64();
            let mut work = with_default(owner.clone(), || {
                span.in_scope(|| {
                    TracingContext::capture().bind(async {
                        let _cleanup = Cleanup;
                        tracing::info!(target: "delta_arrow_reader::test", event = "work.poll");
                        if pending {
                            std::future::pending::<()>().await;
                        }
                    })
                })
            });
            drop(span);
            with_default(other.clone(), || {
                assert_eq!(Pin::new(&mut work).now_or_never().is_none(), pending);
                drop(work);
                tracing::info!(target: "delta_arrow_reader::test", event = "unrelated.probe");
            });
            let events = owner.events();
            assert_eq!(events.len(), 2);
            assert_eq!(events[1].fields["event"], "work.cleanup");
            assert!(events.iter().all(|event| owner.linked_to(event, parent)));
            assert_eq!(other.events().len(), 1);
            owner.assert_balanced();
            let lifetime = owner.lifetime();
            drop(owner);
            assert!(lifetime.upgrade().is_none());
        }
    }

    #[test]
    fn panicking_scoped_work_restores_the_previous_dispatch_and_span() {
        let owner = Collector::new(Level::INFO);
        let other = Collector::new(Level::INFO);
        let context = with_default(owner.clone(), TracingContext::capture);
        with_default(other.clone(), || {
            let parent = tracing::info_span!(target: "delta_arrow_reader::test", "outer");
            let _entered = parent.enter();
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                context.in_scope(|| panic!("worker panic"));
            }));
            assert!(panic.is_err());
            tracing::info!(target: "delta_arrow_reader::test", event = "after.panic");
            let event = other.events().pop().unwrap();
            assert!(other.linked_to(&event, parent.id().unwrap().into_u64()));
        });
        assert!(owner.records().is_empty());
        other.assert_balanced();
    }
}

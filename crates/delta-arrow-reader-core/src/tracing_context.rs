//! Request context across polling, destruction, and blocking handoffs.

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
        tracing::dispatcher::with_default(&self.dispatch, || {
            // A missing or already active span may not override a later caller's current span.
            let _suspended = SuspendedSpans::new(&self.dispatch, &self.span);
            self.span.in_scope(work)
        })
    }

    pub(crate) fn bind<T>(&self, work: T) -> WithContext<T> {
        WithContext {
            inner: Some(Box::pin(work)),
            context: Some(self.clone()),
        }
    }
}

struct SuspendedSpans<'a> {
    dispatch: &'a Dispatch,
    spans: Vec<Span>,
    _thread_bound: PhantomData<*const ()>,
}

impl<'a> SuspendedSpans<'a> {
    fn new(dispatch: &'a Dispatch, parent: &Span) -> Self {
        let mut suspended = Self {
            dispatch,
            spans: Vec::new(),
            _thread_bound: PhantomData,
        };
        let parent_id = parent.id();
        loop {
            let span = Span::current();
            let Some(id) = span.id() else {
                return suspended;
            };
            if Some(&id) == parent_id.as_ref() {
                return suspended;
            }
            // Keep the span alive until it has been re-entered, including during unwind.
            suspended.spans.push(span);
            dispatch.exit(&id);
        }
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

// Reader streams end on their first error, even if the caller never polls for EOF.
impl<S, T, E> Stream for WithContext<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let (Some(context), Some(inner)) = (&this.context, &mut this.inner) else {
            return Poll::Ready(None);
        };
        let result = context.in_scope(|| inner.as_mut().poll_next(cx));
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
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
    fn absent_parent_survives_polling_cleanup_and_unwind_under_the_same_subscriber() {
        for pending in [false, true] {
            let collector = Collector::new(Level::TRACE);
            with_default(collector.clone(), || {
                let root = TracingContext::capture();
                let mut work = root.bind(async {
                    let _cleanup = Cleanup;
                    let child = tracing::info_span!(target: "delta_arrow_reader::test", "child");
                    child.in_scope(|| {
                        tracing::info!(target: "delta_arrow_reader::test", event = "child.work");
                    });
                    if pending {
                        std::future::pending::<()>().await;
                    }
                });
                let outer =
                    tracing::info_span!(target: "delta_arrow_reader::test", "unrelated outer");
                let _outer = outer.enter();
                let inner =
                    tracing::info_span!(target: "delta_arrow_reader::test", "unrelated inner");
                let _inner = inner.enter();
                assert_eq!(Pin::new(&mut work).now_or_never().is_none(), pending);
                assert_eq!(Span::current().id(), inner.id());
                drop(work);
                assert_eq!(Span::current().id(), inner.id());
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    root.in_scope(|| {
                        assert!(Span::current().is_none());
                        panic!("work panicked");
                    });
                }));
                assert!(panic.is_err());
                assert_eq!(Span::current().id(), inner.id());
                let records = collector.records();
                let child = records
                    .iter()
                    .find(|record| record.name == "child")
                    .unwrap();
                assert_eq!(child.parent, None);
                let events = collector.events();
                assert_eq!(events[0].parent, child.span_id);
                assert_eq!(events[1].fields["event"], "work.cleanup");
                assert_eq!(events[1].parent, None);
            });
            collector.assert_balanced();
        }
    }

    #[test]
    fn absent_parent_restores_registry_spans_with_repeated_entries() {
        use tracing_subscriber::registry::LookupSpan;

        let dispatch = Dispatch::new(tracing_subscriber::Registry::default());
        tracing::dispatcher::with_default(&dispatch, || {
            let root = TracingContext::capture();
            let outer = tracing::info_span!("outer");
            let entered_outer = outer.enter();
            let inner = tracing::info_span!("inner");
            let entered_inner = inner.enter();
            let repeated_outer = outer.enter();
            let before = Span::current().id();
            root.in_scope(|| {
                assert!(Span::current().is_none());
                let child = tracing::info_span!("child");
                let registry = dispatch
                    .downcast_ref::<tracing_subscriber::Registry>()
                    .unwrap();
                assert!(
                    registry
                        .span(&child.id().unwrap())
                        .unwrap()
                        .parent()
                        .is_none()
                );
                let entered = child.enter();
                root.in_scope(|| assert!(Span::current().is_none()));
                assert_eq!(Span::current().id(), child.id());
                drop(entered);
                assert!(Span::current().is_none());
            });
            assert_eq!(Span::current().id(), before);
            drop(repeated_outer);
            assert_eq!(Span::current().id(), inner.id());
            drop(entered_inner);
            assert_eq!(Span::current().id(), outer.id());
            drop(entered_outer);
            assert!(Span::current().is_none());
        });
    }

    #[test]
    fn captured_parent_survives_polling_cleanup_and_unwind_with_registry() {
        struct CleanupInSpan(Span);
        impl Drop for CleanupInSpan {
            fn drop(&mut self) {
                assert_eq!(Span::current().id(), self.0.id());
            }
        }

        for owner_stays_entered in [false, true] {
            for pending in [false, true] {
                let dispatch = Dispatch::new(tracing_subscriber::Registry::default());
                tracing::dispatcher::with_default(&dispatch, || {
                    let owner = tracing::info_span!(parent: None, "owner");
                    let context = owner.in_scope(TracingContext::capture);
                    let entered_owner = owner_stays_entered.then(|| owner.enter());
                    let mut work = context.bind(async {
                        assert_eq!(Span::current().id(), owner.id());
                        let _cleanup = CleanupInSpan(owner.clone());
                        let child = tracing::info_span!("work child");
                        child.in_scope(|| {
                            context.in_scope(|| assert_eq!(Span::current().id(), owner.id()));
                            assert_eq!(Span::current().id(), child.id());
                        });
                        if pending {
                            std::future::pending::<()>().await;
                        }
                    });
                    let later = tracing::info_span!(parent: None, "later caller");
                    let entered_later = later.enter();
                    let repeated_owner = owner_stays_entered.then(|| owner.enter());
                    let repeated_later = later.enter();
                    assert_eq!(Pin::new(&mut work).now_or_never().is_none(), pending);
                    assert_eq!(Span::current().id(), later.id());
                    drop(work);
                    assert_eq!(Span::current().id(), later.id());
                    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        context.in_scope(|| {
                            assert_eq!(Span::current().id(), owner.id());
                            panic!("work panicked");
                        });
                    }));
                    assert_eq!(
                        panic.unwrap_err().downcast_ref::<&str>(),
                        Some(&"work panicked")
                    );
                    assert_eq!(Span::current().id(), later.id());
                    drop(repeated_later);
                    assert_eq!(Span::current().id(), later.id());
                    drop(repeated_owner);
                    assert_eq!(Span::current().id(), later.id());
                    drop(entered_later);
                    assert_eq!(
                        Span::current().id(),
                        owner_stays_entered.then(|| owner.id()).flatten()
                    );
                    drop(entered_owner);
                    assert!(Span::current().is_none());
                });
            }
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

    #[test]
    fn terminal_stream_error_drops_inner_in_context_and_releases_the_subscriber() {
        use futures_util::{StreamExt, stream};

        let owner = Collector::new(Level::INFO);
        let other = Collector::new(Level::INFO);
        let mut work = with_default(owner.clone(), || {
            TracingContext::capture().bind(stream::unfold(Cleanup, |cleanup| async move {
                Some((Err::<(), _>("terminal error"), cleanup))
            }))
        });
        with_default(other.clone(), || {
            assert_eq!(
                work.next().now_or_never(),
                Some(Some(Err("terminal error")))
            );
            tracing::info!(target: "delta_arrow_reader::test", event = "unrelated.probe");
        });
        let events = owner.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["event"], "work.cleanup");
        assert_eq!(other.events().len(), 1);
        let lifetime = owner.lifetime();
        drop(owner);
        assert!(lifetime.upgrade().is_none());
        assert_eq!(work.next().now_or_never(), Some(None));
        assert_eq!(work.next().now_or_never(), Some(None));
    }
}

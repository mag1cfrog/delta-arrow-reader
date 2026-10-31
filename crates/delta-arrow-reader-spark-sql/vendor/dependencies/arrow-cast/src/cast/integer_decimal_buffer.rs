// Modified for the DAR Spark SQL runtime. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::cell::RefCell;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::{Array, PrimitiveArray, types::ArrowPrimitiveType};
use arrow_buffer::{Buffer, MutableBuffer, ScalarBuffer};

const MIN_BYTES: usize = 64 * 1024;
const MAX_BYTES: usize = 256 * 1024;
thread_local! {
    // ponytail: At most 256 KiB idle per OS thread; a process-wide budget is
    // needed if callers create enough long-lived threads for this to matter.
    static IDLE: RefCell<Option<MutableBuffer>> = const { RefCell::new(None) };
}

struct Recycled(Option<MutableBuffer>);

impl Drop for Recycled {
    fn drop(&mut self) {
        // A result may be released on a different thread. Return its buffer
        // there, or free it normally if that thread's cache is full or gone.
        let _ = IDLE.try_with(|slot| {
            if let Ok(mut slot) = slot.try_borrow_mut() {
                if slot.is_none() {
                    *slot = self.0.take();
                }
            }
        });
    }
}

pub(super) fn unary<T: ArrowPrimitiveType, D: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    op: impl Fn(T::Native) -> D::Native,
) -> PrimitiveArray<D> {
    let bytes = array.len().saturating_mul(size_of::<D::Native>());
    if !(MIN_BYTES..=MAX_BYTES).contains(&bytes) {
        return array.unary(op);
    }
    let recycled = IDLE
        .try_with(|slot| slot.try_borrow_mut().ok().and_then(|mut slot| slot.take()))
        .ok()
        .flatten()
        .filter(|buffer| buffer.len() == bytes && buffer.as_ptr().cast::<D::Native>().is_aligned());
    let mut buffer = if let Some(mut buffer) = recycled {
        for (output, input) in buffer
            .typed_data_mut::<D::Native>()
            .iter_mut()
            .zip(array.values())
        {
            *output = op(*input);
        }
        buffer
    } else {
        let values: Vec<D::Native> = array.values().iter().map(|v| op(*v)).collect();
        MutableBuffer::from(values)
    };
    if buffer.capacity() != bytes {
        return PrimitiveArray::new(
            ScalarBuffer::new(buffer.into(), 0, array.len()),
            array.nulls().cloned(),
        );
    }
    let ptr = NonNull::new(buffer.as_mut_ptr()).unwrap();
    let owner = Arc::new(Recycled(Some(buffer)));
    // SAFETY: The owner holds the fully initialized, aligned allocation of
    // exactly `bytes` bytes. All Arrow clones/slices retain it; only the final
    // release can return the allocation to the releasing thread's idle slot.
    let buffer = unsafe { Buffer::from_custom_allocation(ptr, bytes, owner) };
    PrimitiveArray::new(
        ScalarBuffer::new(buffer, 0, array.len()),
        array.nulls().cloned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, types::*};

    #[test]
    fn reuse_preserves_retained_results_and_thread_ownership() {
        IDLE.with(|slot| slot.borrow_mut().take());
        let source =
            Int64Array::from_iter((0..8194).map(|i| (i % 5 != 0).then_some(i as i64 - 4000)))
                .slice(1, 8192);
        let first = unary::<Int64Type, Decimal128Type>(&source, i128::from);
        assert_eq!(
            first.get_buffer_memory_size(),
            128 * 1024 + source.nulls().unwrap().buffer().capacity()
        );
        let retained = first.slice(3, 8189);
        drop(first);
        let second = unary::<Int64Type, Decimal128Type>(&source, |v| i128::from(v) * 10);
        let second_ptr = second.values().as_ptr();
        assert_ne!(retained.values().as_ptr(), second_ptr);
        drop(second);
        let third = unary::<Int64Type, Decimal128Type>(&source, |v| i128::from(v) * 100);
        assert_eq!(third.values().as_ptr(), second_ptr);
        assert_eq!(
            retained.iter().collect::<Vec<_>>(),
            source
                .slice(3, 8189)
                .iter()
                .map(|v| v.map(i128::from))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            third.iter().collect::<Vec<_>>(),
            source
                .iter()
                .map(|v| v.map(|v| i128::from(v) * 100))
                .collect::<Vec<_>>()
        );

        // Cross-thread final release can populate the receiving thread's cache.
        std::thread::spawn(move || {
            IDLE.with(|slot| assert!(slot.borrow().is_none()));
            drop(retained);
            let ptr = IDLE.with(|slot| slot.borrow().as_ref().unwrap().as_ptr());
            let input = Int64Array::from(vec![7; 8192]);
            let reused = unary::<Int64Type, Decimal128Type>(&input, i128::from);
            assert_eq!(reused.values().as_ptr().cast::<u8>(), ptr);
            assert!(reused.values().iter().all(|v| *v == 7));
            drop(reused);
        })
        .join()
        .unwrap();
        let (_, values, _) = third.into_parts();
        assert!(values.into_inner().into_mutable().is_err());
        // A nested borrow must neither block nor panic during allocation/drop.
        IDLE.with(|slot| {
            let held = slot.borrow_mut();
            drop(unary::<Int64Type, Decimal128Type>(&source, i128::from));
            assert_eq!(held.as_ref().unwrap().capacity(), 128 * 1024);
        });
        let wide = unary::<Int64Type, Decimal256Type>(&source, |v| {
            arrow_buffer::i256::from_i128(i128::from(v))
        });
        assert_eq!(wide.len(), source.len());
        assert!(
            wide.iter()
                .zip(source.iter())
                .all(|(a, b)| a == b.map(|v| arrow_buffer::i256::from_i128(i128::from(v))))
        );
        drop(wide);
        for rows in [0, 4095, 4096, 4097, 16384, 16385] {
            let input = Int64Array::from(vec![7; rows]);
            let out = unary::<Int64Type, Decimal128Type>(&input, i128::from);
            assert_eq!(out.len(), rows);
            assert!(out.values().iter().all(|v| *v == 7));
            drop(out);
            IDLE.with(|slot| {
                assert!(
                    slot.borrow()
                        .as_ref()
                        .is_none_or(|b| b.capacity() <= MAX_BYTES)
                )
            });
        }
        assert!(
            std::panic::catch_unwind(|| {
                unary::<Int64Type, Decimal128Type>(&source, |_| panic!("test unwind"))
            })
            .is_err()
        );
        drop(unary::<Int64Type, Decimal128Type>(&source, i128::from));
        IDLE.with(|slot| assert!(slot.borrow_mut().take().is_some()));

        // Drop a live output after its thread exits, then during TLS teardown.
        let output = std::thread::spawn(|| {
            unary::<Int64Type, Decimal128Type>(&Int64Array::from(vec![9; 8192]), i128::from)
        })
        .join()
        .unwrap();
        assert!(output.values().iter().all(|v| *v == 9));
        drop(output);
        std::thread::spawn(|| {
            thread_local! {
                static HELD: RefCell<Option<PrimitiveArray<Decimal128Type>>> = const { RefCell::new(None) };
            }
            // Initialize HELD before IDLE, so IDLE is destroyed first.
            HELD.with(|held| {
                *held.borrow_mut() = Some(unary::<Int64Type, Decimal128Type>(
                    &Int64Array::from(vec![11; 8192]), i128::from,
                ));
            });
        }).join().unwrap();
        IDLE.with(|slot| slot.borrow_mut().take());
    }
}

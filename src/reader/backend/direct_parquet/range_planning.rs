//! Planning and execution helpers for Parquet byte-range reads.

use std::{future::Future, ops::Range};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

const MAX_CONCURRENT_RANGE_READS: usize = 10;

/// Combines overlapping ranges and ranges separated by at most `max_gap` bytes.
///
/// The returned ranges are sorted, non-overlapping physical reads. Keeping this
/// step separate lets the caller choose a plan before any object-store I/O starts.
pub(super) fn merge_ranges(requested_ranges: &[Range<u64>], max_gap: u64) -> Vec<Range<u64>> {
    if requested_ranges.is_empty() {
        return Vec::new();
    }

    let mut requested_ranges = requested_ranges.to_vec();
    requested_ranges.sort_unstable_by_key(|range| range.start);

    let mut merged_ranges = Vec::with_capacity(requested_ranges.len());
    let mut start_index = 0;
    let mut end_index = 1;

    while start_index != requested_ranges.len() {
        let mut range_end = requested_ranges[start_index].end;
        while end_index != requested_ranges.len()
            && requested_ranges[end_index]
                .start
                .checked_sub(range_end)
                .is_none_or(|gap| gap <= max_gap)
        {
            range_end = range_end.max(requested_ranges[end_index].end);
            end_index += 1;
        }

        merged_ranges.push(requested_ranges[start_index].start..range_end);
        start_index = end_index;
        end_index += 1;
    }

    merged_ranges
}

/// Executes an already chosen physical plan and returns one result for each requested range.
///
/// Physical reads run with the same concurrency bound as the previous object-store
/// helper. Results are sliced back into the caller's original order, including duplicate
/// and overlapping requests.
pub(super) async fn execute_range_plan<F, E, Fut>(
    requested_ranges: &[Range<u64>],
    physical_ranges: &[Range<u64>],
    read: F,
) -> Result<Vec<Bytes>, E>
where
    F: Send + FnMut(Range<u64>) -> Fut,
    E: Send,
    Fut: Future<Output = Result<Bytes, E>> + Send,
{
    let bytes: Vec<_> = stream::iter(physical_ranges.iter().cloned())
        .map(read)
        .buffered(MAX_CONCURRENT_RANGE_READS)
        .try_collect()
        .await?;

    Ok(requested_ranges
        .iter()
        .map(|requested_range| {
            let physical_index =
                physical_ranges.partition_point(|range| range.start <= requested_range.start) - 1;
            let physical_range = &physical_ranges[physical_index];
            let physical_bytes = &bytes[physical_index];
            let start = (requested_range.start - physical_range.start) as usize;
            let end = (requested_range.end - physical_range.start) as usize;
            physical_bytes.slice(start..end.min(physical_bytes.len()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use bytes::Bytes;

    use super::{execute_range_plan, merge_ranges};

    #[test]
    fn merges_unsorted_overlapping_adjacent_and_nearby_ranges() {
        let requested_ranges = [20..25, 0..4, 4..8, 15..18, 10..12, 2..6];

        assert_eq!(merge_ranges(&requested_ranges, 2), vec![0..12, 15..25]);
        assert_eq!(
            merge_ranges(&requested_ranges, 0),
            vec![0..8, 10..12, 15..18, 20..25]
        );
        assert!(merge_ranges(&[], 2).is_empty());
    }

    #[tokio::test]
    async fn restores_unsorted_overlapping_duplicate_and_empty_requests() -> Result<(), Infallible>
    {
        let requested_ranges = [10..14, 0..4, 2..6, 10..14, 6..6];
        let physical_ranges = merge_ranges(&requested_ranges, 0);
        let data = Bytes::from_static(b"0123456789abcdef");

        let results =
            execute_range_plan(&requested_ranges, &physical_ranges, |range| {
                let data = data.clone();
                async move {
                    Ok::<Bytes, Infallible>(data.slice(range.start as usize..range.end as usize))
                }
            })
            .await?;

        assert_eq!(physical_ranges, vec![0..6, 10..14]);
        assert_eq!(results[0].as_ref(), b"abcd");
        assert_eq!(results[1].as_ref(), b"0123");
        assert_eq!(results[2].as_ref(), b"2345");
        assert_eq!(results[3].as_ref(), b"abcd");
        assert!(results[4].is_empty());
        Ok(())
    }
}

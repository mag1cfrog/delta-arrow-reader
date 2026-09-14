// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
mod formatting;
mod locale;
mod parser;
mod parsing;
mod pattern;

pub use formatting::{DateTimeFormatInput, TimeZoneDisplay};
pub use parsing::ParsedDateTime;
pub use pattern::DateTimeFormat;

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use chrono::{FixedOffset, NaiveDate};

    use super::*;

    #[test]
    fn datetime_patterns_keep_values_and_rejection_boundaries() -> datafusion_common::Result<()> {
        let datetime = NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap()
            .and_hms_nano_opt(13, 5, 9, 123_456_789)
            .unwrap();
        let input = DateTimeFormatInput {
            datetime,
            timezone: Some(TimeZoneDisplay {
                offset: FixedOffset::east_opt(0).unwrap(),
                name: Some("UTC"),
            }),
            zone_id: Some("UTC"),
        };
        for (pattern, expected) in [
            (
                "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
                "2024-02-29 13:05:09.123456789",
            ),
            ("G GG GGG GGGG", "AD AD AD Anno Domini"),
            (
                "y yy yyy yyyy yyyyy yyyyyy",
                "2024 24 2024 2024 02024 002024",
            ),
            (
                "M MM MMM MMMM L LL LLL LLLL",
                "2 02 Feb February 2 02 Feb February",
            ),
            ("E EE EEE EEEE", "Thu Thu Thu Thursday"),
            (
                "Q QQ QQQ QQQQ q qq qqq qqqq",
                "1 01 Q1 1st quarter 1 01 Q1 1st quarter",
            ),
            ("D DD DDD F", "60 60 060 1"),
            (
                "H HH k kk K KK h hh m mm s ss a",
                "13 13 13 13 1 01 1 01 5 05 9 09 PM",
            ),
            ("S SS SSS SSSSSSSSS", "1 12 123 123456789"),
            ("X XX XXX XXXX XXXXX", "Z Z Z Z Z"),
            ("yyyy-MM-dd['T'HH:mm:ss]", "2024-02-29T13:05:09"),
            ("'Y W w u e c A B n N p'", "Y W w u e c A B n N p"),
        ] {
            assert_eq!(
                DateTimeFormat::for_formatting(pattern)?.format(input)?,
                expected,
                "{pattern}"
            );
        }
        for (pattern, value) in [
            (
                "yyyy-MM-dd HH:mm:ss.SSSSSSSSS",
                "2024-02-29 13:05:09.123456789",
            ),
            (
                "yyyy-MM-dd['T'HH:mm:ss[.SSSSSSSSS]]",
                "2024-02-29T13:05:09.123456789",
            ),
        ] {
            assert_eq!(
                DateTimeFormat::for_parsing(pattern)?
                    .parse_datetime_value(value)?
                    .datetime,
                datetime
            );
        }
        assert_eq!(
            DateTimeFormat::for_parsing("yy-MMM-dd")?.parse_date_value("24-Feb-29")?,
            datetime.date()
        );
        let offset = DateTimeFormat::for_parsing("yyyy-MM-dd HH:mm:ssXXX")?
            .parse_datetime_value("2024-02-29 13:05:09+05:30")?;
        assert_eq!(offset.offset.unwrap().local_minus_utc(), 19_800);
        for symbol in ['Y', 'W', 'w', 'u', 'e', 'c', 'A', 'B', 'n', 'N', 'p'] {
            for width in 1..=10 {
                let pattern = symbol.to_string().repeat(width);
                let reason = if "YWwuec".contains(symbol) {
                    "week-based"
                } else {
                    "unsupported"
                };
                for result in [
                    DateTimeFormat::for_parsing(&pattern),
                    DateTimeFormat::for_formatting(&pattern),
                ] {
                    assert!(
                        result.unwrap_err().to_string().contains(reason),
                        "{pattern}"
                    );
                }
            }
        }
        for pattern in ["E", "F", "Q", "q"] {
            assert!(DateTimeFormat::for_parsing(pattern).is_err());
        }
        for pattern in ["MMMMM", "yyyyyyy", "SSSSSSSSSS", "[yyyy", "'unfinished"] {
            assert!(DateTimeFormat::for_parsing(pattern).is_err());
            assert!(DateTimeFormat::for_formatting(pattern).is_err());
        }
        for value in [
            "2024-02-30 13:05:09",
            "2024-02-29 25:05:09",
            "2024-02-29 13:05:09 trailing",
        ] {
            assert!(
                DateTimeFormat::for_parsing("yyyy-MM-dd HH:mm:ss")?
                    .parse_datetime_value(value)
                    .is_err()
            );
        }
        Ok(())
    }
}

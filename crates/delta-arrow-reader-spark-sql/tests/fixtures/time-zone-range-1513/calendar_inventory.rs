    #[test]
    fn inventory_range_panics_1513() {
        use chrono::Offset;
        use serde_json::json;
        const DAY: i64 = 86_400_000_000;
        let min = DateTime::<Utc>::MIN_UTC.timestamp_micros();
        let max = DateTime::<Utc>::MAX_UTC.timestamp_micros();
        let bounds = [min, max];
        let timestamps = [
            i64::MIN, min-1, min, min+DAY, min+2*DAY-1, min+2*DAY,
            -1, 0, 1, max-2*DAY, max-2*DAY+1, max-DAY, max, max+1, i64::MAX,
        ];
        let original_zones = ["UTC", "Z", "+05:45", "+18:00", "-18:00", "America/Los_Angeles"];
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let classic = convert_tz_classic as fn(i64, &ResolvedTimeZone, &ResolvedTimeZone) -> Option<i64>;
        let convert = [("classic", classic), ("non_classic", convert_tz_non_classic)];
        println!("TZ1513 {}", json!({"kind":"limits","min":min,"max":max}));
        let mut count = 0;
        for (mode, apply) in convert {
            for from in original_zones {
                for to in original_zones {
                    let from_zone = ResolvedTimeZone::parse(from).unwrap();
                    let to_zone = ResolvedTimeZone::parse(to).unwrap();
                    for micros in timestamps {
                        let raw = catch_unwind(|| apply(micros, &from_zone, &to_zone));
                        let batch = catch_unwind(|| {
                            let converted = convert_singleton_zones(
                                &Int64Array::from(vec![Some(micros)]),
                                Some(from_zone), Some(to_zone), apply,
                            );
                            converted.iter().next().unwrap()
                        });
                        assert_eq!(raw.is_err(), batch.is_err());
                        if raw.is_err() {
                            count += 1;
                            println!("TZ1513 {}", json!({"kind":"original_panic","mode":mode,
                                "from":from,"to":to,"micros":micros}));
                        }
                    }
                }
            }
        }
        let zones = chrono_tz::TZ_VARIANTS.iter().map(|zone| zone.name()).chain([
            "Z", "+05:45", "-08:00", "+18:00", "-18:00", "+00:00:01", "-00:00:01",
        ]);
        let utc = ResolvedTimeZone::parse("UTC").unwrap();
        for name in zones {
            let zone = ResolvedTimeZone::parse(name).unwrap();
            let offset_at = |micros| {
                let naive = DateTime::<Utc>::from_timestamp_micros(micros).unwrap().naive_utc();
                match zone {
                    ResolvedTimeZone::Region(tz) => tz.offset_from_utc_datetime(&naive).fix(),
                    ResolvedTimeZone::Fixed(offset) => offset,
                }.local_minus_utc()
            };
            let offsets = bounds.map(offset_at);
            assert_eq!(offset_at(min+DAY), offsets[0]);
            assert_eq!(offset_at(max-DAY), offsets[1]);
            for (index, boundary) in bounds.into_iter().enumerate() {
                let offset_us = i64::from(offsets[index]) * 1_000_000;
                let cutoff = boundary - offset_us;
                for micros in [boundary, cutoff - 1, cutoff, cutoff + 1] {
                    if !(min..=max).contains(&micros) {
                        continue;
                    }
                    let predicted = !(min..=max).contains(&(micros + offset_us));
                    for (mode, apply) in convert {
                        let panicked = catch_unwind(|| apply(micros, &utc, &zone)).is_err();
                        assert_eq!(panicked, predicted, "{name} {micros} {mode}");
                        if panicked {
                            println!("TZ1513 {}", json!({"kind":"range_panic","mode":mode,
                                "from":"UTC","to":name,"micros":micros,"offset_seconds":offsets[index]}));
                        }
                    }
                }
            }
        }
        std::panic::set_hook(hook);
        assert!(count > 0);
    }

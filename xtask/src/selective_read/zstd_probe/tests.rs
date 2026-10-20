use super::*;

#[test]
fn raw_headers_follow_returned_offsets_and_reject_truncation_compression_and_checksums()
-> Result<()> {
    let body: Vec<u8> = (0..32768).map(|n| n as u8).collect();
    let mut frame = b"\x28\xb5\x2f\xfd\xa0".to_vec();
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    let mut expected = Vec::new();
    for (index, part) in body.chunks(10001).enumerate() {
        expected.push(json!({"header_offset":frame.len(),"kind":"raw","stored_bytes":part.len(),"decoded_bytes":part.len()}));
        let header = ((part.len() as u32) << 3) | u32::from(index == 3);
        frame.extend_from_slice(&header.to_le_bytes()[..3]);
        frame.extend_from_slice(part);
    }
    let page =
        json!({"page_bytes":frame.len(),"body_bytes":body.len(),"zstd":{"page_header_bytes":0}});
    let mut requests = Vec::new();
    let actual = raw_headers(&page, |start, end| {
        require(end <= frame.len() as u64, "out-of-bounds request")?;
        requests.push((start, end));
        Ok(frame[start as usize..end as usize].to_vec())
    })?;
    assert_eq!(actual, json!(expected));
    assert_eq!(
        requests,
        [(0, 8192)]
            .into_iter()
            .chain(expected[1..].iter().map(|block| {
                let offset = block["header_offset"].as_u64().unwrap();
                (offset, offset + 3)
            }))
            .collect::<Vec<_>>()
    );
    for (key, value) in [
        ("body_bytes", body.len() + 1),
        ("page_bytes", frame.len() - 1),
        ("page_bytes", 8 * 1024 * 1024 + 1),
    ] {
        let mut changed = page.clone();
        changed[key] = json!(value);
        assert!(
            raw_headers(&changed, |start, end| Ok(frame
                [start as usize..end as usize]
                .to_vec()))
            .is_err()
        );
    }
    for index in [4, expected[1]["header_offset"].as_u64().unwrap() as usize] {
        let mut changed = frame.clone();
        changed[index] |= 4;
        assert!(
            raw_headers(&page, |start, end| Ok(changed
                [start as usize..end as usize]
                .to_vec()))
            .is_err()
        );
    }
    assert!(raw_headers(&page, |_, _| Ok(vec![0; 3])).is_err());
    let mut changed = page;
    changed["zstd"]["page_header_bytes"] = json!(9000);
    assert!(
        raw_headers(&changed, |start, end| Ok(frame
            [start as usize..end as usize]
            .to_vec()))
        .is_err()
    );
    Ok(())
}

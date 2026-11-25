use std::io::{self, Read};

use jiter::{Jiter, JiterResult, Peek};

fn scan(input: &[u8]) -> JiterResult<String> {
    let mut parser = Jiter::new(input);
    let result = match parser.peek()? {
        Peek::Object => {
            let mut keys = Vec::new();
            let mut key = parser.known_object()?;
            while let Some(value) = key {
                keys.push(value.to_owned());
                parser.next_skip()?;
                key = parser.next_key()?;
            }
            format!("{keys:?}")
        }
        Peek::Array => {
            let mut len = 0;
            let mut item = parser.known_array()?;
            while let Some(peek) = item {
                parser.known_skip(peek)?;
                len += 1;
                item = parser.array_step()?;
            }
            len.to_string()
        }
        Peek::String => format!("{:?}", parser.known_str()?),
        Peek::Null => {
            parser.known_null()?;
            String::from("null")
        }
        peek => {
            let start = parser.current_index();
            parser.known_skip(peek)?;
            format!("{:?}", parser.slice_to_current(start))
        }
    };
    parser.finish()?;
    Ok(result)
}

fn main() -> io::Result<()> {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    let mut remaining = input.as_slice();
    while !remaining.is_empty() {
        let len = u32::from_le_bytes(remaining[..4].try_into().unwrap()) as usize;
        let (document, rest) = remaining[4..].split_at(len);
        let mut parser = Jiter::new(document);
        let value = parser.next_value();
        println!("{value:?} {:?} {:?}", parser.finish(), scan(document));
        remaining = rest;
    }
    Ok(())
}

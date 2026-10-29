use std::{
    env,
    error::Error,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use flate2::{Compression, write::DeflateEncoder};
use rawzip::{CompressionMethod, ZipArchiveWriter, path::EntryPath};
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub const PLATFORMS: [(&str, &str); 3] = [
    ("linux-x86_64", "manylinux_2_28_x86_64"),
    ("windows-x86_64", "win_amd64"),
    ("macos-arm64", "macosx_12_0_arm64"),
];

pub struct WheelFixture {
    pub directory: PathBuf,
}

impl WheelFixture {
    pub fn new(name: &str) -> Result<Self> {
        let directory =
            env::temp_dir().join(format!("python-wheels-{name}-{}", std::process::id()));
        fs::create_dir(&directory)?;
        Ok(Self { directory })
    }

    pub fn create_artifacts(&self) -> Result<(PathBuf, Vec<PathBuf>)> {
        let directory = self.directory.join("wheel-artifacts");
        let mut paths = Vec::new();
        for (platform, tag) in PLATFORMS {
            let artifact = directory.join(format!("delta-arrow-reader-{platform}-wheel"));
            fs::create_dir_all(&artifact)?;
            let path = artifact.join(wheel_filename(tag));
            write_wheel(&path, &wheel_files(tag), true)?;
            paths.push(path);
        }
        for (platform, tag) in PLATFORMS {
            let path = directory
                .join(format!("delta-arrow-reader-{platform}-wheel"))
                .join(cli_wheel_filename(tag));
            write_wheel(&path, &cli_wheel_files(tag), true)?;
            paths.push(path);
        }
        Ok((directory, paths))
    }
}

impl Drop for WheelFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

pub fn wheel_filename(tag: &str) -> String {
    format!("delta_arrow_reader_py-1.2.3-cp310-abi3-{tag}.whl")
}

pub fn wheel_files(tag: &str) -> Vec<(String, Vec<u8>)> {
    combined_wheel_files(tag)
        .into_iter()
        .filter(|(name, _)| {
            !name.contains("/scripts/") && !name.ends_with("cli-THIRD_PARTY_NOTICES.txt")
        })
        .collect()
}

pub fn cli_wheel_filename(tag: &str) -> String {
    format!("delta_arrow_reader-1.2.3-py3-none-{tag}.whl")
}

pub fn cli_wheel_files(tag: &str) -> Vec<(String, Vec<u8>)> {
    combined_wheel_files(tag)
        .into_iter()
        .filter(|(name, _)| {
            name.contains(".dist-info/")
                || name.contains("/scripts/")
                || name.ends_with("cli-THIRD_PARTY_NOTICES.txt")
        })
        .map(|(name, data)| {
            let data = if name.ends_with("/METADATA") || name.ends_with("/WHEEL") {
                String::from_utf8_lossy(&data)
                    .replace(
                        "Name: delta-arrow-reader-py\n",
                        "Name: delta-arrow-reader\n",
                    )
                    .replace("cp310-abi3", "py3-none")
                    .into_bytes()
            } else {
                data
            };
            (
                name.replace("delta_arrow_reader_py-", "delta_arrow_reader-"),
                data,
            )
        })
        .collect()
}

pub fn combined_wheel_files(tag: &str) -> Vec<(String, Vec<u8>)> {
    let (extension, command, signature): (&str, &str, &[u8]) = match tag {
        "win_amd64" => ("pyd", "dar.exe", b"MZfixture"),
        "macosx_12_0_arm64" => ("abi3.so", "dar", b"\xcf\xfa\xed\xfefixture"),
        _ => ("abi3.so", "dar", b"\x7fELFfixture"),
    };
    vec![
        (
            "delta_arrow_reader_py-1.2.3.dist-info/METADATA".into(),
            b"Metadata-Version: 2.4\nName: delta-arrow-reader-py\nVersion: 1.2.3\n".to_vec(),
        ),
        (
            "delta_arrow_reader_py-1.2.3.dist-info/WHEEL".into(),
            format!("Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: cp310-abi3-{tag}\n").into_bytes(),
        ),
        ("delta_arrow_reader/__init__.py".into(), b"# fixture\n".to_vec()),
        ("delta_arrow_reader/__init__.pyi".into(), b"# fixture\n".to_vec()),
        ("delta_arrow_reader/py.typed".into(), Vec::new()),
        (format!("delta_arrow_reader/delta_arrow_reader.{extension}"), signature.to_vec()),
        (format!("delta_arrow_reader_py-1.2.3.data/scripts/{command}"), signature.to_vec()),
        ("delta_arrow_reader_py-1.2.3.dist-info/licenses/LICENSE".into(), b"license\n".to_vec()),
        ("delta_arrow_reader_py-1.2.3.dist-info/licenses/NOTICE".into(), b"notice\n".to_vec()),
        ("delta_arrow_reader_py-1.2.3.data/data/share/delta-arrow-reader/python-THIRD_PARTY_NOTICES.txt".into(), b"Python notices\n".to_vec()),
        ("delta_arrow_reader_py-1.2.3.data/data/share/delta-arrow-reader/cli-THIRD_PARTY_NOTICES.txt".into(), b"CLI notices\n".to_vec()),
    ]
}

pub fn record_contents(files: &[(String, Vec<u8>)]) -> Result<Vec<u8>> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    for (name, contents) in files.iter().filter(|(name, _)| !name.ends_with('/')) {
        writer.write_record([
            name,
            &format!(
                "sha256={}",
                URL_SAFE_NO_PAD.encode(Sha256::digest(contents))
            ),
            &contents.len().to_string(),
        ])?;
    }
    let info = if files
        .iter()
        .any(|(name, _)| name.starts_with("delta_arrow_reader-"))
    {
        "delta_arrow_reader-1.2.3.dist-info/"
    } else {
        "delta_arrow_reader_py-1.2.3.dist-info/"
    };
    writer.write_record([&format!("{info}RECORD"), "", ""])?;
    Ok(writer.into_inner()?)
}

pub fn write_wheel(path: &Path, files: &[(String, Vec<u8>)], compressed: bool) -> Result<()> {
    let mut files = files.to_vec();
    if !files.iter().any(|(name, _)| name.ends_with("/RECORD")) {
        let info = if files
            .iter()
            .any(|(name, _)| name.starts_with("delta_arrow_reader-"))
        {
            "delta_arrow_reader-1.2.3.dist-info/"
        } else {
            "delta_arrow_reader_py-1.2.3.dist-info/"
        };
        files.push((format!("{info}RECORD"), record_contents(&files)?));
    }
    write_zip(path, &files, compressed)
}

pub fn write_zip(
    path: &Path,
    files: &[(String, impl AsRef<[u8]>)],
    compressed: bool,
) -> Result<()> {
    write_zip_with_permissions(path, files, compressed, |name| {
        if name.ends_with('/') {
            0o040755
        } else if name.ends_with("/dar") {
            0o100755
        } else {
            0o100644
        }
    })
}

pub fn write_zip_with_permissions(
    path: &Path,
    files: &[(String, impl AsRef<[u8]>)],
    compressed: bool,
    permissions: impl Fn(&str) -> u32,
) -> Result<()> {
    let mut archive = ZipArchiveWriter::new(fs::File::create(path)?);
    for (name, contents) in files {
        let method = if compressed {
            CompressionMethod::DEFLATE
        } else {
            CompressionMethod::STORE
        };
        let (mut entry, config) = archive
            .new_file(EntryPath::verbatim(name.as_bytes()))
            .compression_method(method)
            .unix_permissions(permissions(name))
            .start()?;
        let descriptor = if compressed {
            let encoder = DeflateEncoder::new(&mut entry, Compression::default());
            let mut writer = config.wrap(encoder);
            writer.write_all(contents.as_ref())?;
            let (encoder, descriptor) = writer.finish()?;
            encoder.finish()?;
            descriptor
        } else {
            let mut writer = config.wrap(&mut entry);
            writer.write_all(contents.as_ref())?;
            writer.finish()?.1
        };
        entry.finish(descriptor)?;
    }
    archive.finish()?;
    Ok(())
}

use std::{
    env,
    error::Error,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use flate2::{Compression, write::DeflateEncoder};
use rawzip::{CompressionMethod, ZipArchiveWriter, path::EntryPath};

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
            write_zip(&path, &metadata_files(tag), true)?;
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
    format!("delta_arrow_reader-1.2.3-cp310-abi3-{tag}.whl")
}

pub fn metadata_files(tag: &str) -> Vec<(String, String)> {
    vec![
        (
            "delta_arrow_reader-1.2.3.dist-info/METADATA".into(),
            "Metadata-Version: 2.4\nName: delta-arrow-reader\nVersion: 1.2.3\n".into(),
        ),
        (
            "delta_arrow_reader-1.2.3.dist-info/WHEEL".into(),
            format!("Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: cp310-abi3-{tag}\n"),
        ),
    ]
}

pub fn write_zip(
    path: &Path,
    files: &[(String, impl AsRef<[u8]>)],
    compressed: bool,
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

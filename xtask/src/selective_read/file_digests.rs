use super::require;
use crate::{Result, artifacts::reader_sha256};
use std::{
    collections::HashMap,
    fs::{self, File},
    os::unix::fs::MetadataExt,
    path::Path,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Identity([u64; 7]);
pub(super) fn identity(path: &Path) -> Result<Identity> {
    from_metadata(&fs::metadata(path)?)
}
fn from_metadata(m: &fs::Metadata) -> Result<Identity> {
    require(m.is_file(), "expected a regular file")?;
    Ok(Identity([
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime() as u64,
        m.mtime_nsec() as u64,
        m.ctime() as u64,
        m.ctime_nsec() as u64,
    ]))
}
#[derive(Default)]
pub(super) struct Digests(HashMap<Identity, String>);
impl Digests {
    pub(super) fn digest(&mut self, path: &Path) -> Result<String> {
        self.read(path, |file| Ok(reader_sha256(file)?))
    }
    fn read(
        &mut self,
        path: &Path,
        hash: impl FnOnce(&mut File) -> Result<String>,
    ) -> Result<String> {
        let mut file = File::open(path)?;
        let before = from_metadata(&file.metadata()?)?;
        let hash = match self.0.get(&before) {
            Some(hash) => hash.clone(),
            None => hash(&mut file)?,
        };
        require(
            from_metadata(&file.metadata()?)? == before && identity(path)? == before,
            "file changed during digest",
        )?;
        self.0.insert(before, hash.clone());
        Ok(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cache_tracks_hardlinks_restored_mtimes_replacements_and_mutations_during_reads() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let path = root.path().join("data");
        let link = root.path().join("link");
        fs::write(&path, b"first")?;
        fs::hard_link(&path, &link)?;
        let original_time = fs::metadata(&path)?.modified()?;
        let mut hashes = Digests::default();
        let old = hashes.digest(&path)?;
        assert_eq!(
            hashes.read(&link, |_| panic!("hard link was read twice"))?,
            old
        );
        assert_eq!(hashes.0.len(), 1);
        fs::write(&path, b"later")?;
        File::options()
            .write(true)
            .open(&path)?
            .set_modified(original_time)?;
        assert_ne!(hashes.digest(&link)?, old);
        fs::remove_file(&path)?;
        fs::write(&path, b"first")?;
        File::options()
            .write(true)
            .open(&path)?
            .set_modified(original_time)?;
        assert_eq!(hashes.digest(&path)?, old);
        let race = root.path().join("race");
        fs::write(&race, b"before")?;
        assert!(
            hashes
                .read(&race, |file| {
                    let value = reader_sha256(file)?;
                    fs::write(&race, b"after")?;
                    Ok(value)
                })
                .is_err()
        );
        assert_eq!(
            hashes.digest(&race)?,
            crate::selective_read_build::digest(&race)?
        );
        Ok(())
    }
}

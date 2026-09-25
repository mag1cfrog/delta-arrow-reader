//! Best-effort Linux host and cgroup memory hints for automatic scan planning.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use super::{MemoryHint, memory_hint, parse_linux_meminfo};

pub(super) fn local_memory_hint() -> Option<MemoryHint> {
    // SAFETY: sysconf reads a process configuration value and takes no pointers.
    let page_size = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(0);
    collect_memory_hint(|path| fs::read_to_string(path).ok(), page_size)
}

fn collect_memory_hint(
    mut read: impl FnMut(&Path) -> Option<String>,
    page_size: u64,
) -> Option<MemoryHint> {
    let host = read(Path::new("/proc/meminfo")).and_then(|text| parse_linux_meminfo(&text));
    let cgroup = cgroup_available_memory(&mut read, page_size);
    memory_hint(
        host.and_then(|hint| hint.total_bytes),
        host.and_then(|hint| hint.available_bytes)
            .into_iter()
            .chain(cgroup)
            .min(),
    )
}

#[derive(Clone, Copy)]
enum CgroupVersion {
    V1,
    V2,
}

fn cgroup_available_memory(
    read: &mut impl FnMut(&Path) -> Option<String>,
    page_size: u64,
) -> Option<u64> {
    let membership = read(Path::new("/proc/self/cgroup"))?;
    let (version, group) = memory_cgroup(&membership)?;
    let mounts = read(Path::new("/proc/self/mountinfo"))?;
    let mut available = None;
    for line in mounts.lines() {
        let Some((root, mount)) = cgroup_mount(line, version) else {
            continue;
        };
        let Ok(relative) = group.strip_prefix(root) else {
            continue;
        };
        let leaf = mount.join(relative);
        // Each ancestor's own usage includes its other descendants. Subtracting
        // only the leaf's usage would overstate the shared parent's headroom.
        for directory in leaf.ancestors().take_while(|path| path.starts_with(&mount)) {
            if matches!(version, CgroupVersion::V1)
                && directory != leaf
                && read(&directory.join("memory.use_hierarchy"))
                    .as_deref()
                    .map(str::trim)
                    != Some("1")
            {
                continue;
            }
            let (limit_file, usage_file) = match version {
                CgroupVersion::V1 => ("memory.limit_in_bytes", "memory.usage_in_bytes"),
                CgroupVersion::V2 => ("memory.max", "memory.current"),
            };
            let limit = read(&directory.join(limit_file))
                .and_then(|value| finite_limit(&value, version, page_size));
            let Some(limit) = limit else { continue };
            let Some(usage) = read(&directory.join(usage_file))
                .and_then(|value| value.trim().parse::<u64>().ok())
            else {
                continue;
            };
            let headroom = limit.saturating_sub(usage);
            available = Some(available.map_or(headroom, |value: u64| value.min(headroom)));
        }
    }
    available
}

fn memory_cgroup(contents: &str) -> Option<(CgroupVersion, &Path)> {
    let mut unified = None;
    for line in contents.lines() {
        let Some((id, rest)) = line.split_once(':') else {
            continue;
        };
        let Some((controllers, path)) = rest.split_once(':') else {
            continue;
        };
        let Ok(id) = id.parse::<u32>() else { continue };
        if id != 0
            && controllers
                .split(',')
                .any(|controller| controller == "memory")
        {
            return absolute_path(path).map(|path| (CgroupVersion::V1, path));
        }
        if id == 0 && controllers.is_empty() {
            unified = absolute_path(path).map(|path| (CgroupVersion::V2, path));
        }
    }
    unified
}

fn cgroup_mount(line: &str, version: CgroupVersion) -> Option<(PathBuf, PathBuf)> {
    let (paths, filesystem) = line.split_once(" - ")?;
    let mut filesystem = filesystem.split_ascii_whitespace();
    let kind = filesystem.next()?;
    filesystem.next()?; // Mount source.
    let options = filesystem.next()?;
    match version {
        CgroupVersion::V1
            if kind == "cgroup" && options.split(',').any(|value| value == "memory") => {}
        CgroupVersion::V2 if kind == "cgroup2" => {}
        _ => return None,
    }
    let mut paths = paths.split_ascii_whitespace();
    let root = unescape_mount_path(paths.nth(3)?)?;
    let mount = unescape_mount_path(paths.next()?)?;
    paths.next()?; // Mount options precede any optional fields.
    Some((
        absolute_path(&root)?.to_owned(),
        absolute_path(&mount)?.to_owned(),
    ))
}

fn absolute_path(value: &str) -> Option<&Path> {
    let path = Path::new(value);
    (path.is_absolute()
        && path
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_))))
    .then_some(path)
}

fn unescape_mount_path(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    let mut decoded = Vec::with_capacity(value.len());
    while let Some(byte) = bytes.next() {
        decoded.push(if byte == b'\\' {
            match [bytes.next()?, bytes.next()?, bytes.next()?] {
                [b'0', b'4', b'0'] => b' ',
                [b'0', b'1', b'1'] => b'\t',
                [b'0', b'1', b'2'] => b'\n',
                [b'1', b'3', b'4'] => b'\\',
                _ => return None,
            }
        } else {
            byte
        });
    }
    String::from_utf8(decoded).ok()
}

fn finite_limit(value: &str, version: CgroupVersion, page_size: u64) -> Option<u64> {
    let limit = value.trim().parse::<u64>().ok()?;
    if matches!(version, CgroupVersion::V1) {
        // v1 exposes PAGE_COUNTER_MAX * PAGE_SIZE instead of v2's "max".
        let unlimited = if cfg!(target_pointer_width = "32") {
            (i32::MAX as u64).checked_mul(page_size)?
        } else {
            (i64::MAX as u64).checked_div(page_size)? * page_size
        };
        if limit >= unlimited {
            return None;
        }
    }
    Some(limit)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use super::super::{
        DeltaScanPartitionTargetDiagnosticInput, derive_delta_scan_partition_target_diagnostic,
    };
    use super::*;

    fn fixture(version: u8) -> HashMap<PathBuf, String> {
        let (membership, mount, limit, usage) = match version {
            1 => (
                "7:cpu,memory:/team/worker\n",
                "41 39 0:28 / /cg rw - cgroup cgroup rw,cpu,memory\n",
                "memory.limit_in_bytes",
                "memory.usage_in_bytes",
            ),
            _ => (
                "0::/team/worker\n",
                "41 39 0:28 / /cg rw shared:7 - cgroup2 cgroup rw\n",
                "memory.max",
                "memory.current",
            ),
        };
        [
            (
                "/proc/meminfo".into(),
                "MemTotal: 33554432 kB\nMemAvailable: 16777216 kB\n".into(),
            ),
            ("/proc/self/cgroup".into(), membership.into()),
            ("/proc/self/mountinfo".into(), mount.into()),
            (
                Path::new("/cg/team/worker").join(limit),
                "1073741824\n".into(),
            ),
            (
                Path::new("/cg/team/worker").join(usage),
                "536870912\n".into(),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn hint(files: &HashMap<PathBuf, String>) -> Option<MemoryHint> {
        collect_memory_hint(|path| files.get(path).cloned(), 4096)
    }

    #[test]
    fn container_headroom_limits_automatic_targets_but_preserves_explicit_overrides()
    -> Result<(), Box<dyn std::error::Error>> {
        for version in [1, 2] {
            let memory = hint(&fixture(version)).ok_or("missing hint")?;
            assert_eq!(memory.available_bytes, Some(512 * 1024 * 1024));
            assert_eq!(memory.total_bytes, Some(32 * 1024 * 1024 * 1024));
            let mut input = DeltaScanPartitionTargetDiagnosticInput {
                available_parallelism: Some(32),
                available_memory_bytes: memory.available_bytes,
                unix_soft_file_descriptor_limit: Some(4096),
                ..Default::default()
            };
            assert_eq!(
                derive_delta_scan_partition_target_diagnostic(input)?.target_partitions,
                2
            );
            input.explicit_target_partitions = Some(32);
            assert_eq!(
                derive_delta_scan_partition_target_diagnostic(input)?.target_partitions,
                32
            );
        }
        Ok(())
    }

    #[test]
    fn ancestors_use_their_own_usage_and_do_not_raise_a_tighter_leaf_limit() {
        for (version, limit, usage) in [
            (1, "memory.limit_in_bytes", "memory.usage_in_bytes"),
            (2, "memory.max", "memory.current"),
        ] {
            let mut files = fixture(version);
            files.insert("/cg/team/memory.use_hierarchy".into(), "1\n".into());
            files.insert(Path::new("/cg/team").join(limit), "2147483648".into());
            for (used, expected) in [(1_073_741_824, 536_870_912), (2_013_265_920, 134_217_728)] {
                files.insert(Path::new("/cg/team").join(usage), used.to_string());
                assert_eq!(
                    hint(&files).and_then(|hint| hint.available_bytes),
                    Some(expected)
                );
            }
            // A visible mount root can itself be a constrained namespace root.
            files.insert("/cg/memory.use_hierarchy".into(), "1".into());
            files.insert(Path::new("/cg").join(limit), "67108864".into());
            files.insert(Path::new("/cg").join(usage), "0".into());
            assert_eq!(
                hint(&files).and_then(|hint| hint.available_bytes),
                Some(67_108_864)
            );
        }
    }

    #[test]
    fn v1_ancestor_limits_require_hierarchical_accounting() {
        let mut files = fixture(1);
        files.insert("/cg/team/memory.limit_in_bytes".into(), "100".into());
        files.insert("/cg/team/memory.usage_in_bytes".into(), "99".into());
        for hierarchy in [None, Some("0"), Some("garbage"), Some("1\n")] {
            files.remove(Path::new("/cg/team/memory.use_hierarchy"));
            if let Some(value) = hierarchy {
                files.insert("/cg/team/memory.use_hierarchy".into(), value.into());
            }
            let expected = if hierarchy == Some("1\n") {
                1
            } else {
                536_870_912
            };
            assert_eq!(
                hint(&files).and_then(|hint| hint.available_bytes),
                Some(expected)
            );
        }
    }

    #[test]
    fn zero_headroom_is_preserved_and_planning_still_produces_one_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        for (version, limit, usage) in [
            (1, "memory.limit_in_bytes", "memory.usage_in_bytes"),
            (2, "memory.max", "memory.current"),
        ] {
            for (maximum, used) in [(0, 0), (100, 100), (100, 101), (100, u64::MAX)] {
                let mut files = fixture(version);
                files.insert(
                    Path::new("/cg/team/worker").join(limit),
                    maximum.to_string(),
                );
                files.insert(Path::new("/cg/team/worker").join(usage), used.to_string());
                let available = hint(&files).and_then(|hint| hint.available_bytes);
                assert_eq!(available, Some(0));
                let output = derive_delta_scan_partition_target_diagnostic(
                    DeltaScanPartitionTargetDiagnosticInput {
                        available_parallelism: Some(32),
                        available_memory_bytes: available,
                        ..Default::default()
                    },
                )?;
                assert_eq!(output.target_partitions, 1);
            }
        }
        Ok(())
    }

    #[test]
    fn unavailable_values_fall_back_without_discarding_readable_ancestors() {
        for (version, limit, usage, unlimited) in [
            (
                1,
                "memory.limit_in_bytes",
                "memory.usage_in_bytes",
                "9223372036854771712",
            ),
            (2, "memory.max", "memory.current", "max"),
        ] {
            for field in [limit, usage] {
                for invalid in [
                    None,
                    Some(""),
                    Some("-1"),
                    Some("bad"),
                    Some("1 2"),
                    Some("18446744073709551616"),
                    Some(unlimited),
                ] {
                    let mut files = fixture(version);
                    let path = Path::new("/cg/team/worker").join(field);
                    files.remove(&path);
                    if let Some(value) = invalid {
                        files.insert(path, value.into());
                    }
                    // The numeric v1 sentinel is a valid (very high) usage value.
                    let expected = if version == 1 && field == usage && invalid == Some(unlimited) {
                        0
                    } else {
                        16 * 1024 * 1024 * 1024
                    };
                    assert_eq!(
                        hint(&files).and_then(|hint| hint.available_bytes),
                        Some(expected),
                        "v{version}: {field}={invalid:?}"
                    );
                    files.insert("/cg/team/memory.use_hierarchy".into(), "1".into());
                    files.insert(Path::new("/cg/team").join(limit), "200".into());
                    files.insert(Path::new("/cg/team").join(usage), "100".into());
                    assert_eq!(
                        hint(&files).and_then(|hint| hint.available_bytes),
                        Some(expected.min(100))
                    );
                }
            }
        }
    }

    #[test]
    fn host_and_cgroup_hints_each_work_when_the_other_is_missing() {
        for version in [1, 2] {
            for (host, expected) in [
                (None, Some(536_870_912)),
                (Some("MemTotal: 33554432 kB\n"), Some(536_870_912)),
                (Some("MemAvailable: 131072 kB\n"), Some(134_217_728)),
                (Some("MemAvailable: 0 kB\n"), Some(0)),
                (Some("MemAvailable: invalid kB\n"), Some(536_870_912)),
            ] {
                let mut files = fixture(version);
                files.remove(Path::new("/proc/meminfo"));
                if let Some(value) = host {
                    files.insert("/proc/meminfo".into(), value.into());
                }
                assert_eq!(hint(&files).and_then(|hint| hint.available_bytes), expected);
            }
            for missing in ["/proc/self/cgroup", "/proc/self/mountinfo"] {
                let mut files = fixture(version);
                files.remove(Path::new(missing));
                assert_eq!(
                    hint(&files).and_then(|hint| hint.available_bytes),
                    Some(16 * 1024 * 1024 * 1024)
                );
                files.remove(Path::new("/proc/meminfo"));
                assert_eq!(hint(&files), None);
            }
        }
    }

    #[test]
    fn mount_root_offsets_and_namespace_roots_use_only_visible_ancestors() {
        for (membership, root, relative) in [
            ("/team/worker", "/team", "worker"),
            ("/team/worker", "/team/worker", ""),
            ("/", "/", ""),
        ] {
            let mut files = fixture(2);
            files.insert("/proc/self/cgroup".into(), format!("0::{membership}\n"));
            files.insert(
                "/proc/self/mountinfo".into(),
                format!(
                    "41 39 0:28 {root} /different/mount rw shared:7 master:1 - cgroup2 cgroup rw\n"
                ),
            );
            let leaf = Path::new("/different/mount").join(relative);
            files.insert(leaf.join("memory.max"), "200".into());
            files.insert(leaf.join("memory.current"), "50".into());
            files.insert("/different/memory.max".into(), "0".into());
            files.insert("/different/memory.current".into(), "0".into());
            let mut reads = Vec::new();
            let actual = collect_memory_hint(
                |path| {
                    reads.push(path.to_owned());
                    files.get(path).cloned()
                },
                4096,
            );
            assert_eq!(actual.and_then(|hint| hint.available_bytes), Some(150));
            assert!(!reads.contains(&PathBuf::from("/different/memory.max")));
        }
    }

    #[test]
    fn escaped_mount_paths_and_colons_in_membership_are_resolved() {
        let mut files = fixture(2);
        files.insert(
            "/proc/self/cgroup".into(),
            "0::/team name/worker:job\n".into(),
        );
        files.insert("/proc/self/mountinfo".into(), "41 39 0:28 /team\\040name /cg\\040space\\011tab\\012newline\\134040 rw - cgroup2 cgroup rw\n".into());
        let directory = Path::new("/cg space\ttab\nnewline\\040/worker:job");
        files.insert(directory.join("memory.max"), "300".into());
        files.insert(directory.join("memory.current"), "100".into());
        assert_eq!(
            hint(&files).and_then(|hint| hint.available_bytes),
            Some(200)
        );
    }

    #[test]
    fn all_visible_mount_aliases_contribute_ancestor_limits() {
        let mut files = fixture(2);
        let mounts = [
            "41 39 0:28 /team /short rw - cgroup2 cgroup rw\n",
            "42 39 0:28 / /full rw - cgroup2 cgroup rw\n",
        ];
        for directory in ["/short/worker", "/full/team/worker"] {
            files.insert(Path::new(directory).join("memory.max"), "200".into());
            files.insert(Path::new(directory).join("memory.current"), "50".into());
        }
        files.insert("/full/memory.max".into(), "100".into());
        files.insert("/full/memory.current".into(), "75".into());
        for order in [
            format!("{}{}", mounts[0], mounts[1]),
            format!("{}{}", mounts[1], mounts[0]),
        ] {
            files.insert("/proc/self/mountinfo".into(), order);
            assert_eq!(hint(&files).and_then(|hint| hint.available_bytes), Some(25));
        }
    }

    #[test]
    fn hybrid_hierarchies_use_the_v1_memory_controller_regardless_of_line_order() {
        let mut files = fixture(1);
        let mount = files[Path::new("/proc/self/mountinfo")].clone();
        files.insert(
            "/proc/self/mountinfo".into(),
            format!("1 0 0:27 / /unified rw - cgroup2 cgroup rw\n{mount}"),
        );
        files.insert("/unified/memory.max".into(), "0".into());
        files.insert("/unified/memory.current".into(), "0".into());
        for membership in [
            "0::/\n7:cpu,memory:/team/worker\n",
            "7:cpu,memory:/team/worker\n0::/\n",
        ] {
            files.insert("/proc/self/cgroup".into(), membership.into());
            assert_eq!(
                hint(&files).and_then(|hint| hint.available_bytes),
                Some(536_870_912)
            );
        }
    }

    #[test]
    fn malformed_or_unrelated_memberships_and_mounts_are_ignored() {
        for membership in [
            "",
            "bad",
            "0::relative",
            "0::/../team/worker",
            "0::/team/../team/worker",
            "1:cpu:/team/worker",
            "1:memory_extra:/team/worker",
            "invalid::/team/worker",
            "0:memory:/team/worker",
        ] {
            let mut files = fixture(2);
            files.insert("/proc/self/cgroup".into(), membership.into());
            assert_eq!(
                hint(&files).and_then(|hint| hint.available_bytes),
                Some(16 * 1024 * 1024 * 1024),
                "{membership}"
            );
        }
        for mount in [
            "",
            "bad",
            "1 0 0:28 / /cg rw",
            "1 0 0:28 / /cg rw - ext4 root rw",
            "1 0 0:28 /team2 /cg rw - cgroup2 cgroup rw",
            "1 0 0:28 relative /cg rw - cgroup2 cgroup rw",
            "1 0 0:28 / relative rw - cgroup2 cgroup rw",
            "1 0 0:28 / /cg/../cg rw - cgroup2 cgroup rw",
            "1 0 0:28 / /cg\\999 rw - cgroup2 cgroup rw",
            "1 0 0:28 / /cg\\ rw - cgroup2 cgroup rw",
        ] {
            let mut files = fixture(2);
            files.insert("/proc/self/mountinfo".into(), mount.into());
            assert_eq!(
                hint(&files).and_then(|hint| hint.available_bytes),
                Some(16 * 1024 * 1024 * 1024),
                "{mount}"
            );
        }
        let mut files = fixture(1);
        files.insert(
            "/proc/self/mountinfo".into(),
            "1 0 0:28 / /cg rw - cgroup cgroup rw,notmemory".into(),
        );
        assert_eq!(
            hint(&files).and_then(|hint| hint.available_bytes),
            Some(16 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn v1_unlimited_sentinels_follow_the_kernel_page_size() {
        for (page_size, sentinel) in [
            (4096, 9_223_372_036_854_771_712_u64),
            (65536, 9_223_372_036_854_710_272),
        ] {
            assert_eq!(
                finite_limit(&sentinel.to_string(), CgroupVersion::V1, page_size),
                None
            );
            assert_eq!(
                finite_limit(&u64::MAX.to_string(), CgroupVersion::V1, page_size),
                None
            );
            assert_eq!(
                finite_limit("1024", CgroupVersion::V1, page_size),
                Some(1024)
            );
        }
        assert_eq!(finite_limit("1024", CgroupVersion::V1, 0), None);
        assert_eq!(finite_limit("1024", CgroupVersion::V2, 0), Some(1024));
    }

    #[test]
    fn detection_reads_current_membership_and_never_uses_swap_or_soft_limits() {
        let mut files = fixture(2);
        for name in [
            "memory.high",
            "memory.swap.max",
            "memory.swap.current",
            "memory.memsw.limit_in_bytes",
        ] {
            files.insert(Path::new("/cg/team/worker").join(name), "0".into());
        }
        assert_eq!(
            hint(&files).and_then(|hint| hint.available_bytes),
            Some(536_870_912)
        );
        files.insert("/proc/self/cgroup".into(), "0::/another\n".into());
        files.insert("/cg/another/memory.max".into(), "200".into());
        files.insert("/cg/another/memory.current".into(), "150".into());
        assert_eq!(hint(&files).and_then(|hint| hint.available_bytes), Some(50));
    }
}

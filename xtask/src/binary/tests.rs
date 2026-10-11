use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn readelf_output(architecture: Architecture) -> String {
    format!(
        "\n [Requesting program interpreter: {}]\n 0x1 (NEEDED) Shared library: [libc.so.6]\n Name: GLIBC_2.17 Flags: none Version: 2\n Name: GLIBC_2.36 Flags: none Version: 3\n",
        architecture.interpreter()
    )
}

#[test]
fn supported_headers_and_runtime_requirements() -> TestResult {
    for architecture in Architecture::ALL {
        let mut header = [0; 20];
        header[..7].copy_from_slice(b"\x7fELF\x02\x01\x01");
        header[16..18].copy_from_slice(&3u16.to_le_bytes());
        header[18..20].copy_from_slice(&architecture.elf_machine().to_le_bytes());
        check_elf_header(&header, architecture)?;
        let metadata = check_readelf_output(&readelf_output(architecture), architecture)?;
        assert_eq!(metadata.interpreter, architecture.interpreter());
        assert_eq!(metadata.runtime_dependencies, ["libc.so.6"]);
        assert_eq!(metadata.max_glibc, "2.36");
        assert!(check_elf_header(&header[..19], architecture).is_err());
        for (index, value) in [(0, 0), (4, 1), (5, 2), (16, 1), (18, 0)] {
            let mut invalid = header;
            invalid[index] = value;
            assert!(check_elf_header(&invalid, architecture).is_err());
        }
    }
    Ok(())
}

#[test]
fn unsafe_loader_paths_dependencies_and_symbol_requirements_fail() {
    let architecture = Architecture::Amd64;
    let original = readelf_output(architecture);
    for output in [
        original.replace(architecture.interpreter(), "/tmp/loader"),
        original.replace("Requesting program interpreter", "Other"),
        format!(
            "{original}[Requesting program interpreter: {}]",
            architecture.interpreter()
        ),
        format!("{original}0x001d (RPATH) Library rpath: [/tmp]"),
        format!("{original}0x001d (RUNPATH) Library runpath: [$ORIGIN]"),
        original.replace("libc.so.6", "libssl.so.3"),
        original.replace("libc.so.6", "/tmp/libc.so.6"),
        original.replace("(NEEDED)", "(UNUSED)"),
        format!("{original}(NEEDED) Shared library: [libc.so.6]"),
        original.replace("GLIBC_2.36", "GLIBC_2.39"),
        original.replace("GLIBC_2.36", "GLIBC_2.36.1"),
        original.replace("GLIBC_2.36", "GLIBC_PRIVATE"),
        original.replace("GLIBC_2.36", "GLIBC_FUTURE"),
        original.replace("Name:", "Other:"),
        format!("{original}Properties: x86 ISA needed: x86-64-v2"),
    ] {
        assert!(
            check_readelf_output(&output, architecture).is_err(),
            "accepted {output}"
        );
    }
}

#[test]
fn glibc_versions_are_compared_numerically() -> TestResult {
    for version in ["2.9", "2.2.5", "2.35", "2.36.0"] {
        check_readelf_output(
            &readelf_output(Architecture::Amd64).replace("2.36", version),
            Architecture::Amd64,
        )?;
    }
    Ok(())
}

#[test]
fn release_builds_require_the_pinned_debian_baseline() -> TestResult {
    for architecture in Architecture::ALL {
        let release = "ID=debian\nVERSION_ID=\"12\"\n";
        check_build_requirements(
            release,
            "glibc 2.36",
            architecture.uname_machine(),
            architecture,
        )?;
        for (release, glibc, machine) in [
            (
                "ID=ubuntu\nVERSION_ID=\"24.04\"",
                "glibc 2.36",
                architecture.uname_machine(),
            ),
            (
                "ID=debian\nVERSION_ID=\"13\"",
                "glibc 2.36",
                architecture.uname_machine(),
            ),
            (release, "glibc 2.39", architecture.uname_machine()),
            (release, "glibc 2.36", "unsupported"),
        ] {
            assert!(check_build_requirements(release, glibc, machine, architecture).is_err());
        }
    }
    Ok(())
}

#[test]
fn runtime_requires_compatible_glibc_and_matching_architecture() -> TestResult {
    for architecture in Architecture::ALL {
        for version in ["2.36", "2.36.0", "2.39", "2.43", "2.100"] {
            check_runtime_requirements(
                &format!("glibc {version}"),
                architecture.uname_machine(),
                architecture,
            )?;
        }
        for glibc in [
            "glibc 2.9",
            "glibc 2.35.99",
            "musl 1.2.5",
            "glibc 2",
            "glibc 2.36.preview",
            "glibc 2.36.0.1",
            "glibc 2.18446744073709551616",
        ] {
            assert!(
                check_runtime_requirements(glibc, architecture.uname_machine(), architecture)
                    .is_err(),
                "accepted {glibc}"
            );
        }
        let other_machine = if architecture == Architecture::Amd64 {
            "aarch64"
        } else {
            "x86_64"
        };
        assert!(check_runtime_requirements("glibc 2.36", other_machine, architecture).is_err());
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod executable {
    use super::*;
    use std::{io::Write, os::unix::fs::PermissionsExt, process::Stdio};

    struct Fixture {
        root: tempfile::TempDir,
        binary: PathBuf,
        architecture: Architecture,
    }

    impl Fixture {
        fn new() -> std::result::Result<Self, Box<dyn std::error::Error>> {
            let root = tempfile::tempdir()?;
            let binary = root.path().join("dar");
            let mut compiler = Command::new("cc")
                .args(["-x", "c", "-", "-o"])
                .arg(&binary)
                .stdin(Stdio::piped())
                .spawn()?;
            let marker = serde_json::to_string(&root.path().join("executed"))?;
            compiler.stdin.take().ok_or("missing compiler stdin")?.write_all(
                format!("#include <stdio.h>\nint main(void) {{ FILE *f = fopen({marker}, \"w\"); if (f) fclose(f); puts(\"dar 1.2.3\"); return 0; }}\n").as_bytes()
            )?;
            assert!(compiler.wait()?.success());
            // Drop distro-specific compiler notes from this synthetic test executable.
            assert!(
                Command::new("objcopy")
                    .args([
                        "--remove-section=.gnu.build.attributes",
                        "--remove-section=.note.gnu.property"
                    ])
                    .arg(&binary)
                    .status()?
                    .success()
            );
            let architecture = if cfg!(target_arch = "x86_64") {
                Architecture::Amd64
            } else {
                Architecture::Arm64
            };
            Ok(Self {
                root,
                binary,
                architecture,
            })
        }
    }

    #[test]
    fn real_elf_inspection_and_version_execution() -> TestResult {
        let fixture = Fixture::new()?;
        let expected = file_sha256(&fixture.binary)?;
        let metadata =
            inspect_and_execute_binary(&fixture.binary, fixture.architecture, "1.2.3", &expected)?;
        assert_eq!(metadata.dar_sha256, expected);
        assert!(fixture.root.path().join("executed").exists());
        assert!(
            metadata
                .elf
                .runtime_dependencies
                .iter()
                .any(|name| name == "libc.so.6")
        );
        assert!(
            inspect_and_execute_binary(&fixture.binary, fixture.architecture, "1.2.4", &expected)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn hash_mismatch_prevents_execution() -> TestResult {
        let fixture = Fixture::new()?;
        assert!(
            inspect_and_execute_binary(
                &fixture.binary,
                fixture.architecture,
                "1.2.3",
                &"0".repeat(64)
            )
            .is_err()
        );
        assert!(!fixture.root.path().join("executed").exists());
        Ok(())
    }

    #[test]
    fn unexpected_permission_bits_and_symlinks_fail() -> TestResult {
        let fixture = Fixture::new()?;
        for mode in [0o644, 0o4755] {
            fs::set_permissions(&fixture.binary, fs::Permissions::from_mode(mode))?;
            assert!(inspect_elf(&fixture.binary, fixture.architecture).is_err());
        }
        fs::set_permissions(&fixture.binary, fs::Permissions::from_mode(0o755))?;
        let link = fixture.root.path().join("link");
        std::os::unix::fs::symlink(&fixture.binary, &link)?;
        assert!(inspect_elf(&link, fixture.architecture).is_err());
        Ok(())
    }
}

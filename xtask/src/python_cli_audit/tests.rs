use super::*;

const MACOS_COMMANDS: &str = "delta-arrow-reader:
Load command 0
          cmd LC_LOAD_DYLINKER
      cmdsize 32
         name /usr/lib/dyld (offset 12)
Load command 1
       cmd LC_BUILD_VERSION
   cmdsize 32
  platform 1
     minos 12.0
       sdk 15.0
    ntools 1
      tool 3
   version 1115.7.3
Load command 2
          cmd LC_LOAD_DYLIB
      cmdsize 56
         name /usr/lib/libSystem.B.dylib (offset 24)
   time stamp 2 Thu Jan  1 00:00:02 1970
      current version 1351.0.0
compatibility version 1.0.0
Load command 3
          cmd LC_LOAD_WEAK_DYLIB
      cmdsize 104
         name /System/Library/Frameworks/Security.framework/Versions/A/Security (offset 24)
   time stamp 2 Thu Jan  1 00:00:02 1970
      current version 0.0.0
compatibility version 1.0.0
";

const WINDOWS_IMPORTS: &str = "
File: delta-arrow-reader.exe
Format: COFF-x86-64
Arch: x86_64
AddressSize: 64bit
ImageFileHeader {
  Machine: IMAGE_FILE_MACHINE_AMD64 (0x8664)
  SectionCount: 2
  Characteristics [ (0x22)
    IMAGE_FILE_EXECUTABLE_IMAGE (0x2)
    IMAGE_FILE_LARGE_ADDRESS_AWARE (0x20)
  ]
}
Import {
  Name: KERNEL32.dll
  ImportLookupTableRVA: 0x2000
  ImportAddressTableRVA: 0x2010
  Symbol: GetCurrentProcessId (0)
}
DelayImport {
  Name: VCRUNTIME140.dll
  Attributes: 0x1
  Import {
    Symbol: memcpy (0)
    Address: 0x1000
  }
}
";

#[test]
fn macos_requires_an_arm64_executable() -> Result<()> {
    let header = b"\xcf\xfa\xed\xfe\x0c\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00";
    check_macos_header(header)?;
    assert!(check_macos_header(&header[..15]).is_err());
    // Wrong format, x86_64, arm64e, and a dylib must all be rejected.
    for (byte, value) in [(0, 0), (4, 7), (8, 2), (12, 6)] {
        let mut invalid = *header;
        invalid[byte] = value;
        assert!(check_macos_header(&invalid).is_err());
    }
    Ok(())
}

#[test]
fn macos_checks_deployment_target_and_every_library_load_kind() -> Result<()> {
    let report = check_macos_load_commands(MACOS_COMMANDS)?;
    assert_eq!(report["minimum_macos"], "12.0");
    assert_eq!(report["runtime_dependencies"].as_array().unwrap().len(), 2);
    for version in ["11", "11.9", "12.0.0"] {
        check_macos_load_commands(
            &MACOS_COMMANDS.replace("minos 12.0", &format!("minos {version}")),
        )?;
    }
    check_macos_load_commands(&MACOS_COMMANDS.replace("platform 1", "platform MACOS"))?;
    check_macos_load_commands(&MACOS_COMMANDS.replace("platform 1", "platform macos"))?;
    for kind in [
        "LC_LOAD_DYLIB",
        "LC_LOAD_WEAK_DYLIB",
        "LC_REEXPORT_DYLIB",
        "LC_LOAD_UPWARD_DYLIB",
        "LC_LAZY_LOAD_DYLIB",
    ] {
        let command = MACOS_COMMANDS.replace("LC_LOAD_DYLIB", kind);
        check_macos_load_commands(&command)?;
        assert!(
            check_macos_load_commands(
                &command.replace("/usr/lib/libSystem.B.dylib", "@rpath/libSystem.B.dylib")
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn macos_rejects_missing_newer_or_foreign_platforms_and_search_paths() {
    for invalid in [
        MACOS_COMMANDS.replace("minos 12.0", "minos 12.0.1"),
        MACOS_COMMANDS.replace("minos 12.0", "minos 13.0"),
        MACOS_COMMANDS.replace("minos 12.0", "minos 10.15"),
        MACOS_COMMANDS.replace("minos 12.0", "minos 12.invalid"),
        MACOS_COMMANDS.replace("minos 12.0", "minos +12.0"),
        MACOS_COMMANDS.replace("minos 12.0", "minos 12.0.0.1"),
        MACOS_COMMANDS.replace("minos 12.0", "minos 4294967296.0"),
        MACOS_COMMANDS.replace("minos 12.0", ""),
        MACOS_COMMANDS.replace("minos 12.0", "minos 12.0\nminos 11.0"),
        MACOS_COMMANDS.replace("platform 1", "platform 2"),
        MACOS_COMMANDS.replace("LC_BUILD_VERSION", "LC_VERSION_MIN_IPHONEOS"),
        MACOS_COMMANDS.replace("/usr/lib/dyld", "/build/dyld"),
        MACOS_COMMANDS.replace("/usr/lib/libSystem.B.dylib", "/usr/local/lib/private.dylib"),
        MACOS_COMMANDS.replace(
            "/usr/lib/libSystem.B.dylib",
            "/usr/lib/../../build/private.dylib",
        ),
        MACOS_COMMANDS.replace(
            "/usr/lib/libSystem.B.dylib",
            "@executable_path/private.dylib",
        ),
        format!("{MACOS_COMMANDS}\nLoad command 4\ncmd LC_RPATH\npath /build (offset 12)\n"),
        format!("{MACOS_COMMANDS}\nLoad command 4\ncmd LC_BUILD_VERSION\nplatform 1\nminos 11.0\n"),
        String::new(),
    ] {
        assert!(
            check_macos_load_commands(&invalid).is_err(),
            "accepted {invalid}"
        );
    }
}

#[test]
fn windows_checks_regular_and_delayed_dll_imports() -> Result<()> {
    assert_eq!(
        check_windows_imports(WINDOWS_IMPORTS)?["runtime_dependencies"],
        json!(["kernel32.dll", "vcruntime140.dll"])
    );
    for library in [
        "ucrtbase.dll",
        "api-ms-win-crt-runtime-l1-1-0.dll",
        "api-ms-win-core-synch-l1-2-0.dll",
    ] {
        check_windows_imports(&WINDOWS_IMPORTS.replace("VCRUNTIME140.dll", library))?;
    }
    for library in [
        "build-only.dll",
        "python310.dll",
        "vcruntime140d.dll",
        "C:\\build\\vcruntime140.dll",
        "api-ms-win-crt-bad/path.dll",
    ] {
        for imported in ["KERNEL32.dll", "VCRUNTIME140.dll"] {
            let invalid = WINDOWS_IMPORTS.replace(imported, library);
            assert!(
                check_windows_imports(&invalid).is_err(),
                "accepted {library}"
            );
        }
    }
    Ok(())
}

#[test]
fn windows_rejects_wrong_architectures_dlls_and_incomplete_imports() {
    for invalid in [
        WINDOWS_IMPORTS.replace("COFF-x86-64", "COFF-ARM64"),
        WINDOWS_IMPORTS.replace("Arch: x86_64", "Arch: aarch64"),
        WINDOWS_IMPORTS.replace("AMD64 (0x8664)", "I386 (0x14C)"),
        WINDOWS_IMPORTS.replace(
            "IMAGE_FILE_EXECUTABLE_IMAGE (0x2)",
            "IMAGE_FILE_DLL (0x2000)",
        ),
        WINDOWS_IMPORTS.replace(
            "IMAGE_FILE_LARGE_ADDRESS_AWARE (0x20)",
            "IMAGE_FILE_DLL (0x2000)",
        ),
        WINDOWS_IMPORTS.replace("  Name: VCRUNTIME140.dll", ""),
        WINDOWS_IMPORTS.replace(
            "  Name: VCRUNTIME140.dll",
            "  Name: VCRUNTIME140.dll\n  Name: KERNEL32.dll",
        ),
        WINDOWS_IMPORTS.split("Import {").next().unwrap().into(),
        String::new(),
    ] {
        assert!(
            check_windows_imports(&invalid).is_err(),
            "accepted {invalid}"
        );
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[test]
fn native_audit_checks_a_real_elf_without_executing_it()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("binary.c");
    let binary = directory.path().join("delta-arrow-reader");
    std::fs::write(
        &source,
        "#include <unistd.h>\nvoid start(void) { _exit(99); }\n",
    )?;
    let status = Command::new("cc")
        .args(["-nostartfiles", "-Wl,-e,start"])
        .arg(&source)
        .arg("-o")
        .arg(&binary)
        .status()?;
    assert!(status.success());
    AuditPythonCliArgs {
        binary: binary.clone(),
        platform_tag: "manylinux_2_28_x86_64".into(),
    }
    .run()?;
    AuditPythonCliArgs {
        binary: binary.clone(),
        platform_tag: "macosx_12_0_arm64".into(),
    }
    .run()
    .expect_err("accepted an ELF executable as Mach-O");
    Ok(())
}

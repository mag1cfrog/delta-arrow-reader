fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        // Wheel dependencies share Maturin's flags; the CLI must still resolve every symbol.
        println!("cargo::rustc-link-arg-bin=dar=-Wl,-undefined,error");
    }
}

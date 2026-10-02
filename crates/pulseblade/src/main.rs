use clap::Parser;

/// Agent-first infrastructure monitor.
#[derive(Parser)]
#[command(name = "pulseblade", version, about)]
struct Cli {}

fn main() {
    let _ = Cli::parse();
}

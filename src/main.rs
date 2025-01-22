use clap::{Parser, Subcommand};
use rat::{commands, Result};
use std::path::PathBuf;
use xdg::BaseDirectories;

fn setup_directories(base: &BaseDirectories) -> Result<()> {
    let data_home = base.get_data_home();
    let config_home = base.get_config_home();
    let cache_home = base.get_cache_home();
    let state_home = base.get_state_home();

    let dirs = vec![data_home, config_home, cache_home, state_home];
    for dir in dirs {
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }
    }
    Ok(())
}

fn do_main() -> Result<()> {
    let args = Rat::parse();

    let base = BaseDirectories::with_prefix("rat")?;
    setup_directories(&base)?;
    match args.commands {
        Commands::List(list) => list.run(base)?,
        Commands::Add(add) => add.run(base)?,
        Commands::Run(run) => run.run(base)?,
        Commands::Log(log) => log.run(base)?,
    };
    Ok(())
}

#[derive(Parser, Debug)]
struct Rat {
    #[clap(short, long, default_value = "/tmp/rat.db")]
    directory: PathBuf,

    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    List(commands::List),
    Add(commands::Add),
    Run(commands::Run),
    Log(commands::Log),
}

fn main() {
    env_logger::init();
    if let Err(e) = do_main() {
        eprintln!("Error: {:?}", e);
    }
}

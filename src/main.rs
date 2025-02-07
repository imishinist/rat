use clap::{Parser, Subcommand};
use rat::{commands, JobManager, Result};
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

    let data_home = base.get_data_home();
    let job_manager = JobManager::new(data_home)?;
    match args.commands {
        Commands::List(list) => list.run(job_manager)?,
        Commands::Add(add) => add.run(job_manager)?,
        Commands::Cancel(cancel) => cancel.run(job_manager)?,
        Commands::Delete(delete) => delete.run(job_manager)?,
        Commands::Run(run) => run.run(job_manager)?,
        Commands::Log(log) => log.run(job_manager)?,
    };
    Ok(())
}

#[derive(Parser, Debug)]
struct Rat {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    List(commands::List),
    Add(commands::Add),
    Cancel(commands::Cancel),
    Delete(commands::Delete),
    Run(commands::Run),
    Log(commands::Log),
}

fn main() {
    env_logger::init();
    if let Err(e) = do_main() {
        eprintln!("error: {}", e);

        for err in e.chain().skip(1) {
            eprintln!("\tcaused by: {:?}", err);
        }
    }
}

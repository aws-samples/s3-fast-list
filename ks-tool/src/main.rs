mod utils;
mod arn;
mod data_map;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {

    /// Read from local ks file, split to target number of prefix segments
    Split {
        /// ks input file
        #[arg(short, long)]
        ks: String,

        /// target count of prefix segments to split
        #[arg(short, long)]
        count: usize,

        /// output ks hints file
        /// expected file name convention: {region}_{bucket}_ks_hints.input
        #[arg(short, long, verbatim_doc_comment)]
        output: String,
    },

    /// Parse inventory manifest at S3, download inventory files and export to local ks file
    Inventory {
        /// region of S3 inventory
        #[arg(short, long)]
        region: String,

        /// S3 URI of inventory manifest.json
        /// - format S3://{bucket}/{dest_prefix}/{src_bucket}/{config_id}/{YYYY-MM-DDTHH-MMZ}/manifest.json
        #[arg(short, long, verbatim_doc_comment)]
        manifest: String,

        /// output ks input file [default: {region}_{bucket}.ks]
        #[arg(short, long, verbatim_doc_comment)]
        ks: Option<String>,

        /// max concurrency for download and process inventory files
        #[arg(short, long, default_value_t = 1)]
        concurrency: usize

    },
}

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {

    let cli = Cli::parse();

    match &cli.cmd {
        Commands::Split { ks, count, output } => {
            utils::handle_ks_input(ks, *count, &output).await?;
        },
        Commands::Inventory { region, manifest, ks, concurrency } => {
            utils::inventory_to_ks(region, manifest, ks.as_ref(), *concurrency).await?;
        },
    }
    Ok(())
}

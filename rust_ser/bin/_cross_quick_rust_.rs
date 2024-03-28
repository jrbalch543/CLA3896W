use rust_scripts::fq::freq_cross_methods::*;
use clap::{value_parser, Arg, ArgAction, ArgMatches, Args, Command, Parser};
use polars::prelude::*;
use std::error::Error;
use std::fs::File;

pub fn main() {
    let args = build_the_parser();
    let (final_freqed, n) = run_freq_quick(&args);
    write_results(final_freqed, &args, n);
}

#[derive(Parser)]
#[command(
    name = "cross_quick",
    version = "1.0",
    about = "
-----------------
Quick crosstab
-----------------
This program will provide a crosstabulation of two variables from a fixed-column-width data file.

Output:
    tab-delimited crosstabulation       Written to the screen by default.
"
)]
struct DerivedArgs {
    #[arg(short = 'q', help = "Explicitly quote values in the output")]
    quote: bool,

    #[arg(
        short = 'v',
        help = "Verbose output (includes file and variable information)"
    )]
    verbose: bool,

    #[arg(
        short = 'c',
        help = "Specify columns you want to do the frequency on, key value pairs, consisting of start and width",
        num_args = 2,
        required = false,
        value_parser = value_parser!(String)
    )]
    collate: Vec<String>,

    #[arg(
        short = 'o',
        help = "Specify rows you want to do the frequency on, key value pairs, consting of start and width",
        num_args = 2,
        required = false,
        value_parser = value_parser!(String)
    )]
    only: Vec<String>,
}

fn build_the_parser() -> MyArgs {
    let cmd = Command::new("cross_tab")
    .arg(Arg::new("file_path")
        .action(ArgAction::Set)
        .num_args(1)
        .help("Path to a fixed-width data file")
    )
    .arg(Arg::new("starts_and_widths")
        .action(ArgAction::Set)
        .value_parser(value_parser!(usize))
        .num_args(4)
        .help("Start and Width of variables (e.g. 4 1 is the variable starting at position 4 with width 1). Should be 4 numbers given here (start width start width).")
    )
    .arg(Arg::new("rectype")
        .required(false)
        .action(ArgAction::Set)
        .short('r')
        .help("Collect frequencies of only record type 'X'")
    )
    .arg(Arg::new("rectype_start")
        .value_parser(value_parser!(usize))
        .action(ArgAction::Set)
        .required(false)
        .short('s')
        .requires("rectype")
        .default_value("1")
        .help("Start column for record type variable")
    )
    .arg(Arg::new("rectype_width")
        .value_parser(value_parser!(usize))
        .action(ArgAction::Set)
        .required(false)
        .short('w')
        .requires("rectype")
        .default_value("1")
        .help("Width of record type variable")
    );
    let cmd = DerivedArgs::augment_args(cmd);
    let args = cmd.get_matches();
    let a = define_args(&args);
    a
}

fn define_args(args: &ArgMatches) -> MyArgs {
    MyArgs {
        file_path: args
            .get_one::<String>("file_path")
            .expect("ERROR: No file path given. Please supply a file path to run the program.")
            .to_owned(),
        vars: define_vars(args),
        rec: define_rt(args),
        weight: None,
        q: args.get_flag("quote"),
        v: args.get_flag("verbose"),
        nf: false,
        c: None,
        o: None,
    }
}



fn write_results(df: DataFrame, args: &MyArgs, n: usize) -> Result<(), Box<dyn Error>> {
    let mut df = df;
    if args.q {
        fn add_quote(var_ser: &Series) -> Series {
            var_ser
                .utf8()
                .unwrap()
                .into_iter()
                .map(|x| match x {
                    Some(x) => Some(format!("'{}'", x)),
                    None => None,
                })
                .collect::<Utf8Chunked>()
                .into_series()
        }

        for idx in (0..df.shape().1 - 1) {
            let _ = df.apply_at_idx(idx, add_quote);
        }
    }

    if args.v {
        println!("File: {}", args.file_path);
        for var in &args.vars {
            println!("Start -- Width: {} -- {}", var.start, var.width);
        }
        let rt = match &args.rec.value {
            Some(i) => i,
            None => "all records",
        };
        println!("Record type: {}", rt);
        println!("Record type start: {}", args.rec.start);
        println!("Record type width: {}", args.rec.width);
        let (ws, ww, wd) = match &args.weight {
            Some(w) => (
                w.start.to_string(),
                w.width.to_string(),
                w.divisor.to_string(),
            ),
            None => {
                let s = String::from("unweighted");
                (s.clone(), s.clone(), s.clone())
            }
        };
        println!("Weight start: {}", ws);
        println!("Weight width: {}", ww);
        println!("Weight divisor: {}", wd);
        println!("Lines examined: {}", n);
        println!("Unique values: {:?}", df.shape().0);
    }

    let mut writer = CsvWriter::new(File::create("crosstab_output.csv")?)
        .has_header(true)
        .with_delimiter(b',');
    
    let w = writer.finish(&mut df)?;

    // use pyo3::prelude::*;
    // pyo3::prepare_freethreaded_python();
    // // #[cfg(Py_3_7)]
    // // pyo3_build_config::use_pyo3_cfgs();

    // let v: PyResult<()> = Python::with_gil(|py| {
    //     let fun: Py<PyAny> = PyModule::from_code(
    //         py,
    //         "def pivot(*args, **kwargs):
    //                 import pandas as pd
    //                 df = pd.read_csv('crosstab_output.csv')
    //                 print(pd.crosstab(df['column_0'], df['column_1'], values = df['Frequency'], aggfunc = 'sum', margins=True, margins_name='*TOT*').fillna(0).convert_dtypes())",
    //             "",
    //             "",
    //     )
    //     .unwrap()
    //     .getattr("pivot")
    //     .unwrap()
    //     .into();
        
    //     fun.call0(py).unwrap();
    //     Ok(())
    // });
    // std::fs::remove_file("crosstab_output.csv");
    Ok(())
}
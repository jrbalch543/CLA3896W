use rust_scripts::fq::freq_cross_methods::*;
use clap::{value_parser, Arg, ArgAction, ArgMatches, Args, Command, Parser};
use polars::prelude::*;
use std::error::Error;


#[derive(Parser)]
#[command(
    name = "freq_quick",
    version = "1.0",
    about = "
-----------------
Quick frequencies
-----------------
This program will provide a frequency distribution for a variable in a fixed-
width data file. A variable can be defined by multiple start/width pairs,
in which case frequencies will be collected on the concatenated string.
Optionally, frequencies can be collected only for certain
record types (see options -r -s -w), and frequency counts can be weighted (see
options -ws -ww -wd).

Output:
    frequency distribution    Written to the screen by default.
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
        long = "nf",
        help = "No frequencies: use to obtain just a list of the unique values"
    )]
    no_freq: bool,
}

fn main() {
    let args: MyArgs = build_the_parser();
    let (final_freqed, n) = run_freq_quick(&args);
    write_results(final_freqed, &args, n);
}

fn define_wt(args: &ArgMatches) -> Option<Weight> {
    let st: Option<&usize> = args.get_one::<usize>("weight_start");
    let wid = *args
        .get_one("weight_width")
        .expect("ERROR: Weight start given was not a number. Please supply this as a number to signify position if you choose to use weights.");
    let wd = *args
        .get_one("weight_divisor")
        .expect("ERROR: Weight divisor given not a number. Cannot divide by a non-number.");
    let ret = match st {
        None => None,
        Some(i) => Some(Weight {
            start: i.to_owned(),
            width: wid,
            divisor: wd,
        }),
    };
    ret
}

fn define_args(args: &ArgMatches) -> MyArgs {
    let fp = args.try_get_one::<String>("file_path").expect("Failed");
    let fp = match fp {
        None => {
            println!("ERROR: No arguments given. Please supply a file path and variables to run the program.");
            std::process::exit(1)
        },
        Some(i) => i
    };
    MyArgs {
        file_path: fp.to_owned(),
        vars: define_vars(args),
        rec: define_rt(args),
        weight: define_wt(args),
        q: args.get_flag("quote"),
        v: args.get_flag("verbose"),
        nf: args.get_flag("no_freq"),
        c: None,
        o: None,
    }
}

fn build_the_parser() -> MyArgs {
    let cmd = Command::new("freq-quick")
    .arg(Arg::new("file_path")
        .action(ArgAction::Set)
        .num_args(1)
        .help("Path to a fixed-width data file")
    )
    .arg(Arg::new("starts_and_widths")
        .action(ArgAction::Set)
        .value_parser(value_parser!(usize))
        .num_args(2..)
        .help("Start and Widths of variables (e.g. 4 1 is the variable starting at position 4 with width 1).
Can do multiple (e.g. 4 1 5 2 => [Variable(start: 4, width: 1), Variable(start: 5, width: 2)]")
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
    )
    .arg(Arg::new("weight_start")
        .value_parser(value_parser!(usize))
        .required(false)
        .action(ArgAction::Set)
        .long("ws")
        .help("Start column for weight variabvle (default = unweighted frequencies)")
    )
    .arg(Arg::new("weight_width")
        .value_parser(value_parser!(usize))
        .action(ArgAction::Set)
        .required(false)
        .long("ww")
        .requires("weight_start")
        .default_value("1")
        .help("Width for weight variable (default = 1, if using weights")
    )
    .arg(Arg::new("weight_divisor")
        .value_parser(value_parser!(i16))
        .action(ArgAction::Set)
        .required(false)
        .long("wd")
        .requires("weight_start")
        .default_value("1")
        .help("Divisor to use with weights (default = 1, if using weights).
Every weighted frequency will be divided by N. NOTE: results
might be innaccurate if you do not use this option and if the
non-divided tallies exceed Rust's numerical limits.")
    );
    let cmd = DerivedArgs::augment_args(cmd);
    let args = cmd.get_matches();
    let a = define_args(&args);
    a
}

fn write_results(df: DataFrame, args: &MyArgs, n: usize) -> Result<(), Box<dyn Error>> {
    let mut df = df;
    let mut sort_cols = df.get_column_names();
    sort_cols.pop();
    df = df.sort(sort_cols, false, true)?;
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
    let mut writer = CsvWriter::new(std::io::stdout())
        .has_header(false)
        .with_delimiter(b'\t');
    if args.nf {
        writer.finish(&mut df.drop("Frequency")?)?;
    } else {
        writer.finish(&mut df)?;
    }
    Ok(())
}


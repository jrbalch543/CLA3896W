use clap::Parser;
use flate2::read::MultiGzDecoder;
use std::convert::Infallible;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

use bstr::{ByteSlice, BString, BStr, ByteVec};
use std::vec::Vec;
use std::str;

use std::path::PathBuf;
use std::str::FromStr;

use regex::{Regex, Matches};
use polars::prelude::*;
use either::*;
use itertools::multiunzip;

#[derive(Parser, Debug)]
#[command(
    name = "char_find",
    version = "1.0",
    about = "
-----------------
Find characters in variables of data files
-----------------
This program searches one or more data files for any Perl regular
expression pattern and reports on variables in which that pattern is found.

Command-line arguments:
    pattern
    data source(s)

A data source can be given as a path to a .dat or .dat.gz fixed width file.
Alternatively, if working in a workspace, a data source can be referred to with
a sample, e.g. cps1963_03s, or a sample/version, e.g. cps1963_03s/3.
The program will find the data in the workspace matching that sample(/version).

The default output is tab-delimited and contains the following fields:
    file name
    text found
    record type
    column location
    number of records containing the text at this location

Detailed output can be requested with option -d. It documents every
occurence of text that matches the pattern. The detailed output
contains the following fields:
    file name
    record type
    line number
    column number
    text found

Output:
    frequency distribution    Written to the screen by default.
"
)]
struct Args {
    /// Pattern to search for.
    #[arg(required = true)]
    pattern: Regex,

    /// Data Source files. Can be .dat, .dat.gz fixed wifth file.
    /// This is currently not supportng the workspaces
    #[arg(required = true, num_args = (1..))]
    data_sources: Vec<PathBuf>,

    ///  Treat the pattern as a literal string rather than a Rust regular expression.
    #[arg(short = 'r', long = "noregex")]
    noregex: bool,

    /// Ignore record types.
    #[arg(short = 'i', long = "ignorert")]
    ignorert: bool,

    /// Start column for record type variable.
    #[arg(short = 's', long = "rtstart", default_value = "1")]
    start: usize,

    /// Width of record type variable.
    #[arg(short = 'w', long = "rtwidth", default_value = "1")]
    width: usize,

    /// Request detailed output in addition to the default output.
    #[arg(short = 'd', long = "detail")]
    detail: bool,

    /// Ignore source vars (you must also specify a project with -p).
    #[arg(short = 'x', long = "nosvars", requires = "project")]
    nosvars: bool,

    /// Report on variables not column locations (svars will be ignored) (specify a metadata file after the flag, otherwise defaults to variables.xml).
    /// This should be a file with variable/rectype/start/width. Find the variables the locations occur within (i.e. if occur in position 40, find the var starting at 34 with width 11 that it ends in)
    #[arg(short = 'v', long = "findvars", requires = "project", required = false, default_missing_value = "variables.xml", num_args = (0..=1), require_equals = true, default_value = "variables.xml")]
    findvars: PathBuf,

    /// Specify project. Required for -x and/or -v (e.g. ihis, dhs, usa, ipumsi).
    #[arg(short = 'p', long = "project", required = false)]
    project: Option<String>

}

#[derive(Debug, PartialEq, Eq)]
enum FileType {
    PlainText,
    GZipped,
}

impl FileType {
    fn infer_from<S>(filename: S) -> Self
    where
        S: AsRef<str>,
    {
        let filename = filename.as_ref();
        if filename.ends_with(".gz") {
            FileType::GZipped
        } else {
            FileType::PlainText
        }
    }
}

enum ReturnTypes {
    String,
    usize
}

fn main() {
    let args = Args::parse();

    // This is really only useful for detailed runs, but need to init here regardless.
    let mut all_matches: Vec<DataFrame> = Vec::new();

    for f in &args.data_sources {
        // If there is only one file or no -d flag, this is kinda trivial, but looping through a potential vector of len 1 feels fine if that happens.
        all_matches.push(char_find(&args, f));
    }

    if args.detail {
        println!("");
        for df in all_matches {
            /// Have to re-order the columns for detailed runs, but these are the specified columns I know it will have to have. No way it doesn't, so it should never error.
            let reordered = df.select(["File", "RecType", "LineNum", "Column", "Text"]).expect("Should never error.");
            write_results(&args, reordered, false);
        }
    }

}

fn write_results(args: &Args, df: DataFrame, print_cols: bool) {
    /// It's way easier to just make a mut copy than try to deal with making sure things are mut coming in, especially when it ends up as &df a lot in testing
    let mut df = df;

    /// Only do this 'wrap in quotes'
    if print_cols {
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
        let _ = df.apply("Text", add_quote);
        let _ = df.apply("RecType", add_quote);
    }
    
    let mut writer = CsvWriter::new(std::io::stdout())
        .has_header(print_cols)
        .with_delimiter(b'\t');
    writer.finish(&mut df);
}

fn char_find(args: &Args, f: &PathBuf) -> DataFrame {
    let input_file = match File::open(&f) {
        Ok(input_file) => input_file,
        Err(_) => {
            println!("The input file does not exist. Please check the file path.");
            std::process::exit(1);
        }
    };
    let file_type = FileType::infer_from(&f.to_string_lossy());
    let buf_reader: Box<dyn BufRead> = match file_type {
        FileType::PlainText => Box::new(BufReader::new(input_file)),
        FileType::GZipped => Box::new(BufReader::new(MultiGzDecoder::new(input_file))),
    };
    let re = &args.pattern;

    let mut file_values: Vec<String> = vec![];
    let mut text_values: Vec<String> = vec![];
    let mut rectype_values: Vec<String> = vec![];
    let mut column_values: Vec<u64> = vec![];
    let mut linenum_values: Vec<u64> = vec![];

    for (idx, line) in buf_reader.lines().enumerate() {
        let line = line.unwrap_or_else(|_| {
                println!("Error while reading input data file.");
                std::process::exit(1)
            }
        );

        let rectype = if args.ignorert {
            ""
        } else {
            if *&line.len() < args.start + args.width {
                println!("Attempted to index record type variable out of range. Line is of length {}, attempted to start index at {} and width {}.", line.len(), args.start, args.width);
                std::process::exit(1);
            }
            &line[(args.start - 1)..(&args.start+args.width - 1)]
        }.to_string();

        if args.noregex {
            let non_re = re.as_str();
            let mut line = line;
            while !line.is_empty() {
                if line.contains(non_re) {
                    let index = line.find(non_re);

                    file_values.push(f.to_string_lossy().to_string());
                    text_values.push(non_re.to_owned());
                    rectype_values.push(rectype.to_owned());
                    column_values.push((index.unwrap() + non_re.len()).try_into().unwrap());
                    linenum_values.push((idx + 1).try_into().unwrap());

                    line.drain(..(index.unwrap() + non_re.len()));
                } else {
                    line.clear();
                }
            }
            
        } else {
            let matches: Matches = re.find_iter(&line);
            for m in matches {
                file_values.push(f.to_string_lossy().to_string());
                text_values.push(m.as_str().to_owned());
                rectype_values.push(rectype.to_owned());
                column_values.push(m.end().try_into().unwrap());
                linenum_values.push((idx + 1).try_into().unwrap());
            }
        }
    }

    let file_ser = Series::new("File", file_values);
    let text_ser = Series::new("Text", text_values);
    let rectype_ser = Series::new("RecType", rectype_values);
    let col_ser = Series::new("Column", column_values);
    let linenum_ser = Series::new("LineNum", linenum_values);
    
    let mut df = DataFrame::new(vec![file_ser, text_ser, rectype_ser, col_ser, linenum_ser]).expect("Error: Could not build dataframe.");

    let mut out = df
        .groupby(["File", "Text", "RecType", "Column"])
        .expect("uhhhhh")
        .select(["LineNum"])
        .count()
        .expect("Error counting group.");

    out.rename("LineNum_count", "N");

    out = out.sort(["File", "Text", "RecType", "Column"], false, false).expect("Unable to sort.");

    write_results(&args, out, true);

    df

}
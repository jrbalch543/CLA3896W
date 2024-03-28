use clap::Parser;
use flate2::read::MultiGzDecoder;
use std::convert::Infallible;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

use bstr::{ByteSlice, BString};
use std::vec::Vec;
use std::str;

use std::path::PathBuf;
use std::str::FromStr;

/// record.append.pl rewrite prototype
///
/// Append data to each record in a file
#[derive(Parser, Debug)]
struct Args {
    /// Path to the file on disk you want to append to
    #[arg(long = "input_file")]
    input_file: PathBuf,

    /// Path where you want the results of the program to be written to
    #[arg(long = "output_file", default_value = "/tmp/output")]
    output_file: PathBuf,

    /// A space-separated list of data to append to each line.
    ///
    /// The string LINENUM is special and will be replaced with the current
    /// line number, padded so that each line number has the same width.
    #[arg(long="append", num_args=1.., required=true)]
    data: Vec<String>,

    /// The line position at which to append the data
    #[arg(long = "append_position")]
    position: usize,
}

#[derive(Debug)]
enum InsertData {
    /// Insert this String
    Literal(BString),
    /// Insert the current line number. Note that the width of the line number
    /// depends on the total number of lines in the file.
    LineNumber,
}

impl FromStr for InsertData {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "LINENUM" {
            Ok(InsertData::LineNumber)
        } else {
            Ok(InsertData::Literal(s.to_string().into_bytes().into()))
        }
    }
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

fn get_linenum_width(path: &PathBuf) -> std::io::Result<usize> {
    let linenum_width = BufReader::new(File::open(path)?)
        .split(b'\n')
        .count()
        .to_string()
        .len();
    Ok(linenum_width)
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let input_file = match File::open(&args.input_file) {
        Ok(input_file) => input_file,
        Err(_) => {
            println!("The input file does not exist. Please check the file path.");
            std::process::exit(1);
        }
    };

    let file_type = FileType::infer_from(&args.input_file.to_string_lossy());
    let buf_reader: Box<dyn BufRead> = match file_type {
        FileType::PlainText => Box::new(BufReader::new(input_file)),
        FileType::GZipped => Box::new(BufReader::new(MultiGzDecoder::new(input_file))),
    };
    let insert_data: Vec<InsertData> = args.data.iter().map(|s| s.parse().unwrap()).collect();
    let linenum_width = get_linenum_width(&args.input_file).unwrap();
    let mut linenum_arg = false;
    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.output_file);

    for (index, line) in buf_reader.split(b'\n').enumerate() {
        let write_offset = args.position - 1;
        let mut line = line.expect("Error while reading input data file.");
        let linenum = &BString::from((index + 1).to_string());
        if line.is_empty() {
            output_file.as_ref().unwrap().write_all(b"\n")?;
            println!();
            continue;
        }

        let mut insertion:Vec<&[u8]> = vec![];
        for data in &insert_data {
            match data {
                InsertData::Literal(value) => {
                    insertion.push(value.as_bytes());
                }
                InsertData::LineNumber => {
                    linenum_arg = true;
                    insertion.push(linenum.as_bytes());
                }
            };
        }
        line.splice(write_offset..write_offset, insertion.concat());
        println!("{}", line.as_bstr());

        output_file.as_ref().unwrap().write_all(&line)?;
        output_file.as_ref().unwrap().write_all(b"\n")?;
    }
    if linenum_arg {
        println!("LINENUM width is set to {}", linenum_width)
    }
    Ok(())
}

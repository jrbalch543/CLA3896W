pub mod freq_cross_methods {  
    use bstr::ByteSlice;
    use clap::{value_parser, Arg, ArgAction, ArgMatches, Args, Command, Parser};
    use flate2::read::MultiGzDecoder;
    use polars::prelude::*;
    use std::fs::File;
    use std::io::{BufRead, BufReader};  
    
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

    

    #[derive(Debug)]
    pub struct Variable {
        pub start: usize,
        pub width: usize,
    }
    #[derive(Debug)]
    pub struct RecType {
        pub value: Option<String>,
        pub start: usize,
        pub width: usize,
    }
    #[derive(Debug)]
    pub struct Weight {
        pub start: usize,
        pub width: usize,
        pub divisor: i16,
    }

    #[derive(Debug)]
    pub struct MyArgs {
        pub file_path: String,
        pub vars: Vec<Variable>,
        pub rec: RecType,
        pub weight: Option<Weight>,
        pub q: bool,
        pub v: bool,
        pub nf: bool,
        pub c: Option<Vec<String>>,
        pub o: Option<Vec<String>>,
    }

    pub fn define_rt(args: &ArgMatches) -> RecType {
        let val: Option<_> = args.get_one::<String>("rectype");
        let st: usize = *args
            .get_one("rectype_start")
            .expect("ERROR: Rectype start given was not a number. Please supply a number to signify position of rectype start if you wish to use this feature.");
        let wd: usize = *args
            .get_one("rectype_width")
            .expect("ERROR: Rectype width given was not a number. Please supply a number to signify position of rectype width if you wish to use this feature.");
        RecType {
            value: match val {
                None => None,
                Some(i) => Some(String::from(i)),
            },
            start: st,
            width: wd,
        }
    }

    pub fn define_vars(args: &ArgMatches) -> Vec<Variable> {
        let mut values: Vec<_> = args
            .get_many::<usize>("starts_and_widths")
            .expect("ERROR: No variables given. Must supply variables in order to calculate frequencies.")
            .collect();
        if values.len() % 2 != 0 {
            panic!("ERROR: Unequal number of variable starts and widths given. Must supply a width for any given start position.")
        }
        let mut ret_vars: Vec<Variable> = Vec::new();
        while !values.is_empty() {
            let w = values.pop().unwrap().to_owned();
            let s = values.pop().unwrap().to_owned();
            let var: Variable = Variable { start: s, width: w };
            ret_vars.push(var);
        }
        ret_vars.reverse();
        ret_vars
    }

    pub fn run_freq_quick(args: &MyArgs) -> (DataFrame, usize) {
        let input_file = match File::open(&args.file_path) {
            Ok(input_file) => input_file,
            Err(_) => {
                println!("The input file does not exist. Please check the file path.");
                std::process::exit(1);
            }
        };

        let file_type = FileType::infer_from(&args.file_path);

        let buf_reader: Box<dyn BufRead> = match &file_type {
            FileType::PlainText => Box::new(BufReader::new(&input_file)),
            FileType::GZipped => Box::new(BufReader::new(MultiGzDecoder::new(&input_file))),
        };

        let values: Vec<_> = buf_reader
            .split(b'\n')
            .map(|line| {
                let line = line.expect("ERROR: Error while reading input data file. Unable to process line.");
                process_line(line, &args)
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();

        let n = values.len();

        let (found_vals, weight_vec): (Vec<Vec<String>>, Vec<f64>) =
            values.into_iter().map(|x| x).unzip();
        let sers = found_vals
            .into_iter()
            .enumerate()
            .map(|(i, x)| Series::new(&format!("Values_{}", i), x))
            .collect();
        let mut df: DataFrame = DataFrame::new(sers)
            .expect("ERROR: Unable to build frequency dataframe.")
            .transpose(None, None)
            .unwrap();
        let weights: Series = Series::new("Weights", weight_vec);
        let df = df.with_column(weights).unwrap();
        let mut names = df.get_column_names();
        let mut tabbed: DataFrame = df
            .groupby(names)
            .unwrap()
            .select(vec!["column_0"])
            .count()
            .unwrap();
        let freq: Series = Series::new(
            "Frequency",
            tabbed.column("Weights").unwrap() * tabbed.column("column_0_count").unwrap(),
        );
        let mut weighted = tabbed
            .with_column(freq)
            .unwrap()
            .drop_many(&["Weights", "column_0_count"])
            .sort(["Frequency"], true, false)
            .unwrap();
        let mut col_names = weighted.get_column_names();
        col_names.pop();
        weighted = weighted
            .groupby(col_names)
            .unwrap()
            .select(["Frequency"])
            .sum()
            .unwrap();

        let final_freqed = weighted.rename("Frequency_sum", "Frequency").unwrap();

        return (final_freqed.to_owned(), n)
    }

    pub fn process_line(line: Vec<u8>, args: &MyArgs) -> Option<(Vec<String>, f64)> {
        if args.rec.value.is_some() {
            if *&line.len() < args.rec.start + args.rec.width {
                println!("Attempted to index record type variable out of range. Line is of length {}, attempted to start index at {} and width {}.", line.len(), &args.rec.start, &args.rec.width);
                std::process::exit(1);
            }
            let rec = &line[(args.rec.start - 1)..(args.rec.start + args.rec.width - 1)];
            if rec.to_str().unwrap() != args.rec.value.as_ref().unwrap() {
                return None;
            }
        }

        let weight = match &args.weight {
            None => 1.0,
            Some(i) => {
                let w = &line[(i.start - 1)..(i.start + i.width - 1)];
                w.to_str()
                    .expect("Error: Could not parse weight.")
                    .parse::<f64>()
                    .expect(&format!(
                        "Weight at {} is not a number \nWeight: {}",
                        i.start,
                        w.to_str().expect("Error parsing weight")
                    ))
            }
        };

        let mut ret_vec: Vec<String> = Vec::new();
        for v in &args.vars {
            if *&line.len() < v.start + v.width {
                println!("Attempted to index out of range. Line is of length {}, attempted to start index at {} and width {}.", line.len(), v.start, v.width);
                std::process::exit(1);
            }
            let val: &[u8] = &line[(v.start - 1)..(v.start + v.width - 1)];
            if val.is_ascii() {
                // Since the data is ASCII it's 100% safe and you can use the to_string_from_utf8_unchecked()
                unsafe {
                    ret_vec.push(String::from_utf8_unchecked(val.to_owned()));
                }
            } else {
                let (encoded_cow, had_errors) =
                    encoding_rs::WINDOWS_1252.decode_without_bom_handling(val);
                if had_errors {
                    panic!(
                        "Can't add non-ASCII or Windows-1252 encoded character '{:?}' for variable {:?}",
                        val, &v
                    )
                }
                ret_vec.push(encoded_cow.to_string().to_owned());
            }
        }
        Some((ret_vec, weight))
    }
}


mod tests {

    use crate::fq::freq_cross_methods::*;

    const V0: Variable = Variable {
        start: 2,
        width: 9, // to test !San Jose
    };
    const V1: Variable = Variable { start: 1, width: 1 };
    const V2: Variable = Variable { start: 1, width: 2 };
    const V3: Variable = Variable { start: 3, width: 2 };
    const R1: RecType = RecType {
        value: None,
        start: 1,
        width: 1,
    };

    const W1: Weight = Weight {
        start: 2,
        width: 1,
        divisor: 1,
    };

    #[test]
    fn test_extended_ascii() {
        // I got my encoding info from https://eclecticgeek.com/dompdf/debug_tests/charsetsupport.htm
        // Try converting San Jose (where e is accented from Western European style 8-bit extended ASCII)
        // The 161 is for inverted !, and 233 is the lower-cased e acute.
        let line_with_extended_ascii: Vec<u8> = vec![102, 161, 83, 97, 110, 32, 74, 111, 115, 233];

        let r2: RecType = RecType {
            value: Some(String::from("d")),
            start: 5,
            width: 1,
        };

        let fake_args_1 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V0],
            rec: R1,
            weight: None,
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };

        let res_1 = process_line(line_with_extended_ascii.clone(), &fake_args_1);
        assert!(
            res_1.is_some(),
            "Should have some data from processing line."
        );
        if let Some(res) = res_1 {
            let encoded_to = res.0[0].clone();
            let should_encode_to = "¡San José";
            let should_encode_bytes: Vec<u8> = should_encode_to.bytes().collect();
            println!(
                "did encode: '{}', should encode: '{}'",
                encoded_to, should_encode_to
            );

            assert_eq!(encoded_to.bytes().len(), should_encode_to.bytes().len());
            // For extra context in case of failure, print every byte from each side (if they're not
            // the same length the above assertion will fail and skip this.))
            for (index, left_c) in encoded_to.bytes().enumerate() {
                let right_c = should_encode_bytes[index];
                println!("{} : {}", left_c, right_c);
            }
            assert_eq!(encoded_to, should_encode_to);
        }
    }

    #[test]
    fn line_processing() {
        let line: Vec<u8> = vec![
            102, 97, 107, 101, 32, 100, 97, 116, 97, 32, 115, 116, 114, 105, 110, 103, 32, 116,
            104, 97, 116, 32, 115, 104, 111, 117, 108, 100, 32, 119, 111, 114, 107, 32, 102, 111,
            114, 32, 97, 110, 121, 116, 104, 105, 110, 103,
        ]; // This is the u8 vector equivalent to b"fake data string that should work for anything"

        let r2: RecType = RecType {
            value: Some(String::from("d")),
            start: 5,
            width: 1,
        };

        let fake_args_1 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V1],
            rec: R1,
            weight: None,
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };
        let fake_args_2 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V2],
            rec: R1,
            weight: None,
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };
        let fake_args_3 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V2, V3],
            rec: R1,
            weight: None,
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };
        let fake_args_4 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V2, V3],
            rec: r2,
            weight: None,
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };

        let res_1 = process_line(line.clone(), &fake_args_1);
        assert_eq!(res_1, Some((vec![String::from("f")], 1.0)));

        let res_2 = process_line(line.clone(), &fake_args_2);
        assert_eq!(res_2, Some((vec![String::from("fa")], 1.0)));

        let res_3 = process_line(line.clone(), &fake_args_3);
        assert_eq!(
            res_3,
            Some((vec![String::from("fa"), String::from("ke")], 1.0))
        );

        let res_4 = process_line(line.clone(), &fake_args_4);
        assert_eq!(res_4, None);
    }
    #[test]
    #[should_panic(expected = "weight at 2 is not a number")]
    fn should_panic_line_process() {
        let line: Vec<u8> = vec![
            102, 97, 107, 101, 32, 100, 97, 116, 97, 32, 115, 116, 114, 105, 110, 103, 32, 116,
            104, 97, 116, 32, 115, 104, 111, 117, 108, 100, 32, 119, 111, 114, 107, 32, 102, 111,
            114, 32, 97, 110, 121, 116, 104, 105, 110, 103,
        ]; // This is the u8 vector equivalent to b"fake data string that should work for anything"

        let fake_args_5 = MyArgs {
            file_path: String::from("No file should be required for this part"),
            vars: vec![V1],
            rec: R1,
            weight: Some(W1),
            q: false,
            v: false,
            nf: false,
            c: None,
            o: None,
        };
        process_line(line.clone(), &fake_args_5);
    }
}

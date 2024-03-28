import argparse
from pathlib import Path
import subprocess
import os
import sys
from ipums.tools.local_lib import utils

from ipums.metadata import Variables, DataDictionary, Samples

# will suppress stacktrace and only print error message unless --debug set
import ipums.tools.local_lib.suppress_traceback


class FreqQuick(object):
    def __init__(self, parsed_args):
        self.opts = parsed_args["opts"]
        self.passthru = parsed_args["passthru"]
        self.locs = []
        if self.opts.project:
            self.variables = Variables(self.opts.project)
            self.samples = Samples(self.opts.project)
        if self.opts.rectype:
            self.rectype = self.opts.rectype
        self.sample = self.opts.sample

        # if user sends variable_names as args, this is output_data
        if self.opts.variable_names:
            self.opts.output_data = True

        if self.opts.variable_names:
            self.locs.extend(
                [self.variable_loc(var.upper()) for var in self.opts.variable_names]
            )
        if self.opts.svar or self.opts.wsvar:
            svar_sample = self.validate_svars()
            if self.sample:
                assert self.sample == svar_sample, "Svars do not match --sample"
            else:
                self.sample = svar_sample
            self.dd = DataDictionary(self.sample, self.opts.project)
        if self.opts.ovar:
            if not (self.opts.svar or self.opts.wsvar):
                if not self.sample:
                    f = Path(self.opts.data_source).name
                    self.sample = self.samples.datafile_to_sample(f)
                    assert self.sample is not None, "Sample not found for " + f
                self.dd = DataDictionary(self.sample, self.opts.project)
                svars = [self.dd.var_to_svar(ovar) for ovar in self.opts.ovar]
                self.opts.svar = svars
            else:
                # Don't need to find sample and need to append to opts.svar
                svars = [self.dd.var_to_svar(ovar) for ovar in self.opts.ovar]
                self.opts.svar = self.opts.svar.append(svars)
        if self.opts.svar:
            svar_locs = [self.svar_loc(svar.upper()) for svar in self.opts.svar]
            self.locs.extend(svar_locs)
        if hasattr(self, "rectype"):
            self.__addpassthru("-r", self.rectype)
        if self.opts.wsvar or self.opts.wvar or self.opts.wovar:
            if self.opts.wovar:
                self.opts.wsvar = self.dd.var_to_svar(self.opts.wovar)
            if self.opts.wsvar:
                start_wid = self.svar_loc(self.opts.wsvar)
                ws = start_wid[0]
                ww = start_wid[1]
                d = self.dd.svar_to_all_info(self.opts.wsvar)
                d["REC"] = d["RECORDTYPE"]
            elif self.opts.wvar:
                d = self.variables.variable_all_info(self.opts.wvar.upper())
                ws = d["START"]
                ww = d["WID"]
            self.__addpassthru("-ws", ws)
            self.__addpassthru("-ww", ww)

            if not hasattr(self, "rectype"):
                self.rectype = d["REC"]
            if d["REC"] != self.rectype:
                raise AssertionError("Weight not of rectype " + self.rectype)
            self.__resolve_weight_divisor(d)

    def __addpassthru(self, flag, value):
        self.passthru.insert(0, value)
        self.passthru.insert(0, flag)

    def __resolve_weight_divisor(self, d):
        if self.opts.wd:
            value = self.opts.wd
        else:
            if not d["DECIM"] == "":
                value = 10 ** int(d["DECIM"])
            else:
                value = "1"
        self.__addpassthru("-wd", value)

    def validate_svars(self):
        validate = []
        if self.opts.svar:
            validate.extend(self.opts.svar)
        if self.opts.wsvar:
            validate.append(self.opts.wsvar)
        for svar in validate:
            sample = self.samples.svar_to_sample(svar)
            if self.sample and self.sample != sample:
                raise AssertionError(
                    "svar sample "
                    + sample
                    + " unequal "
                    + "to given sample "
                    + self.sample
                )
            else:
                self.sample = sample
        return self.sample

    def variable_loc(self, var):
        if var not in self.variables.all_variables:
            raise AssertionError(var + " not an integrated variable")
        rt = self.variables.variable_to_rectype(var)
        if not hasattr(self, "rectype"):
            self.rectype = rt
        if self.rectype != rt:
            raise AssertionError("mismatched rectypes " + self.rectype + ", " + rt)
        return self.variables.variable_to_start_and_wid(var)

    def svar_loc(self, svar):
        rt = self.dd.svar_to_rectype(svar)
        if not rt.startswith("C"):
            if not hasattr(self, "rectype"):
                self.rectype = rt
            if self.rectype != rt:
                raise AssertionError("mismatched rectypes " + self.rectype + ", " + rt)
        d = self.dd.svar_to_start_and_wid(svar)
        if self.opts.output_data:
            d = {
                "start": self.dd.svar_to_absolute_output_start(svar),
                "wid": self.dd.svar_to_output_wid(svar),
            }
        return (d["start"], d["wid"])

    def run(self):
        rust_exec = "/pkg/ipums/programming/bin/freq_quick"
        # perl_exec = '/home/benklaas/git/mpc-script-emporium/freq.quick.pl'
        locations = [s[0] for s in self.locs]
        locations = []
        for s in self.locs:
            locations.append(str(s[0]))
            locations.append(str(s[1]))
        rust_call = [rust_exec, self.opts.data_source]
        rust_call.extend(locations)
        rust_call.extend([str(x) for x in self.passthru])
        subprocess.call(rust_call)


def parse_and_check_args():
    parser = argparse.ArgumentParser(
        description="Freq Quick: This program \
        constructs a call to freq_quick_rust.rs based on current project \
        metadata, deriving column start and width pairs for the \
        provided integrated variable or svar names and then returns \
        the result to the screen. \
        If multiple integrated or source variables are given frequencies \
        will be collected on the concatenated values. \
        This program will also pass through any freq_quick_rust.rs arguments not \
        listed below. You can see the help documentation for freq_quick_rust.rs \
        by calling `freq_quick_rust.rs -h`"
    )
    parser.add_argument(
        "data_source",
        help="Path to fixed-width data file, \
              or in workspaces context, sample or sample/version",
    )

    parser.add_argument(
        "-r",
        "--rectype",
        dest="rectype",
        help="Collect frequencies only on given RECTYPE",
    )
    optional = parser.add_argument_group("Metadata arguments")
    optional.add_argument(
        "-p",
        "--project",
        help="Valid IPUMS project (required if using --var, --svar, \
        --wvar, or --wsvar)",
    )
    optional.add_argument(
        "-s", "--sample", default=None, help="Valid sample in the given IPUMS project"
    )
    optional.add_argument(
        "--svar", nargs="*", help="List of one or more source variables"
    )
    optional.add_argument(
        "--var",
        dest="variable_names",
        nargs="*",
        help="List of one or more integrated variables",
    )
    optional.add_argument(
        "--ovar",
        nargs="*",
        help="List of one or more variable mnemonics from the data dictionary",
    )
    optional.add_argument(
        "--output_data",
        action="store_true",
        help="Flag indicating that the provided data file is output data. \
        If --svar is supplied with this flag, the start and width values \
        will represent the svar(s)'s location in an output data file.",
    )
    parser.add_argument("--debug", help=argparse.SUPPRESS, action="store_true")

    # only one of wvar and wsvar
    weightvars = optional.add_mutually_exclusive_group()
    weightvars.add_argument(
        "--wsvar", help="Source variable by which to weight the frequencies"
    )
    weightvars.add_argument(
        "--wvar", help="Variable by which to weight the frequencies"
    )
    weightvars.add_argument(
        "--wovar",
        help="Original mnemonic for variable by which to weight the \
        frequencies",
    )
    optional.add_argument(
        "--wd", type=int, help="Deviser for the provided wvar or wsvar"
    )

    opts, passthru = parser.parse_known_args()
    if opts.debug:
        sys.excepthook = sys.__excepthook__
    if (
        opts.wvar or opts.wsvar or opts.svar or opts.ovar or opts.variable_names
    ) and not opts.project:
        parser.error(
            "-p is required when any of "
            + "--wvar, --wsvar, --var, --svar, or --ovar are set."
        )
    if opts.wsvar:
        opts.wsvar = opts.wsvar.upper()
    if opts.svar:
        opts.svar = [svar.upper() for svar in opts.svar]
    if opts.ovar:
        opts.ovar = [ovar.upper() for ovar in opts.ovar]
    opts.data_source = utils.resolve_data_file(opts.data_source)
    return {"opts": opts, "passthru": passthru}


def main():
    fq = FreqQuick(parse_and_check_args())
    fq.run()


if __name__ == "__main__":
    main()

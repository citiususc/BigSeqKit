from typing import List

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitLocateOptions:

    def __init__(self):
        self.__inner = LocateOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def pattern(self, v: List[str]):
        self.__inner.Pattern = v

    def patternFile(self, v: str):
        self.__inner.PatternFile = v

    def degenerate(self, v: bool):
        self.__inner.Degenerate = v

    def useRegexp(self, v: bool):
        self.__inner.UseRegexp = v

    def useFmi(self, v: bool):
        self.__inner.UseFmi = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def onlyPositiveStrand(self, v: bool):
        self.__inner.OnlyPositiveStrand = v

    def validateSeqLength(self, v: int):
        self.__inner.ValidateSeqLength = v

    def nonGreedy(self, v: bool):
        self.__inner.NonGreedy = v

    def gtf(self, v: bool):
        self.__inner.Gtf = v

    def bed(self, v: bool):
        self.__inner.Bed = v

    def maxMismatch(self, v: bool):
        self.__inner.MaxMismatch = v

    def hideMatched(self, v: bool):
        self.__inner.HideMatched = v

    def circular(self, v: bool):
        self.__inner.Circular = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Locate").addParam("opts", _optionsToString(opts))
        
        return input.mapPartitionsWithIndex(libprepare)


class LocateOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Pattern = None  # list[str]
        self.PatternFile = None  # str
        self.Degenerate = None  # bool
        self.UseRegexp = None  # bool
        self.UseFmi = None  # bool
        self.IgnoreCase = None  # bool
        self.OnlyPositiveStrand = None  # bool
        self.ValidateSeqLength = None  # int
        self.NonGreedy = None  # bool
        self.Gtf = None  # bool
        self.Bed = None  # bool
        self.MaxMismatch = None  # int
        self.HideMatched = None  # bool
        self.Circular = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Pattern", [""])
        _setDefault(self, "PatternFile", "")
        _setDefault(self, "Degenerate", False)
        _setDefault(self, "UseRegexp", False)
        _setDefault(self, "UseFmi", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "OnlyPositiveStrand", False)
        _setDefault(self, "ValidateSeqLength", 10000)
        _setDefault(self, "NonGreedy", False)
        _setDefault(self, "Gtf", False)
        _setDefault(self, "Bed", False)
        _setDefault(self, "MaxMismatch", 0)
        _setDefault(self, "HideMatched", False)
        _setDefault(self, "Circular", False)


def locate(input: IDataFrame, o: SeqKitLocateOptions = None, **kwargs):
    if o is None:
        o = SeqKitLocateOptions()
    return o._run(input, **kwargs)

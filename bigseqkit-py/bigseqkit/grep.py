from typing import List

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitGrepOptions:

    def __init__(self):
        self.__inner = GrepOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def pattern(self, v: List[str]):
        self.__inner.Pattern = v

    def patternFile(self, v: str):
        self.__inner.PatternFile = v

    def useRegexp(self, v: bool):
        self.__inner.UseRegexp = v

    def deleteMatched(self, v: bool):
        self.__inner.DeleteMatched = v

    def invertMatch(self, v: bool):
        self.__inner.InvertMatch = v

    def byName(self, v: bool):
        self.__inner.ByName = v

    def bySeq(self, v: bool):
        self.__inner.BySeq = v

    def onlyPositiveStrand(self, v: bool):
        self.__inner.OnlyPositiveStrand = v

    def maxMismatch(self, v: bool):
        self.__inner.MaxMismatch = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def degenerate(self, v: bool):
        self.__inner.Degenerate = v

    def region(self, v: str):
        self.__inner.Region = v

    def circular(self, v: bool):
        self.__inner.Circular = v

    def count(self, v: bool):
        self.__inner.Count = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        grep = _libSource("GrepPairMatched").addParam("opts", _optionsToString(opts))
        results = input.mapPartitionsWithIndex(grep)

        if opts.Count:
            count = results.reduce(_libSource("GrepReduceCount"))
            return int(count)
        elif opts.BySeq and opts.MaxMismatch > 0:
            return results
        elif opts.DeleteMatched and not opts.InvertMatch:
            f1 = results.mapPartitions(_libSource("GrepPairMatched"))
            f2 = f1.toPair().reduceByKey(_libSource("GrepReducePairMatched"), localReduce=False)
            return f2.map(_libSource("GrepValueMatched"))
        else:
            return results


class GrepOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Pattern = None  # list[str]
        self.PatternFile = None  # str
        self.UseRegexp = None  # bool
        self.DeleteMatched = None  # bool
        self.InvertMatch = None  # bool
        self.ByName = None  # bool
        self.BySeq = None  # bool
        self.OnlyPositiveStrand = None  # bool
        self.MaxMismatch = None  # int
        self.IgnoreCase = None  # bool
        self.Degenerate = None  # bool
        self.Region = None  # str
        self.Circular = None  # bool
        self.Count = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Pattern", [""])
        _setDefault(self, "PatternFile", "")
        _setDefault(self, "UseRegexp", False)
        _setDefault(self, "DeleteMatched", False)
        _setDefault(self, "InvertMatch", False)
        _setDefault(self, "ByName", False)
        _setDefault(self, "BySeq", False)
        _setDefault(self, "OnlyPositiveStrand", False)
        _setDefault(self, "MaxMismatch", 0)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "Degenerate", False)
        _setDefault(self, "Region", "")
        _setDefault(self, "Circular", False)
        _setDefault(self, "Count", False)


def grep(input: IDataFrame, o: SeqKitGrepOptions = None, **kwargs):
    if o is None:
        o = SeqKitGrepOptions()
    return o._run(input, **kwargs)

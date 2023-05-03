from typing import List

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitFaidxOptions:

    def __init__(self):
        self.__inner = SeqOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def useRegexp(self, v: bool):
        self.__inner.UseRegexp = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def fullHead(self, v: bool):
        self.__inner.FullHead = v

    def regionFile(self, v: str):
        self.__inner.RegionFile = v

    def regions(self, v: List[str]):
        self.__inner.Regions = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        offsets = input.mapPartitions(_libSource("FaidxOffset"))
        offsetsArray = offsets.collect()
        for i in range(len(offsetsArray) - 1):
            offsetsArray[i + 1] = offsetsArray[i]
        offsetsArray[0] = 0

        libfaidx = _libSource("Faidx")\
            .addParam("offsets", offsetsArray)\
            .addParam("opts", _optionsToString(opts))

        faidx = input.mapPartitionsWithIndex(libfaidx)
        if len(opts.Regions) == 0 and opts.RegionFile == "":
            return faidx

        libqueries = _libSource("FaidxQuery").addParam("opts", _optionsToString(opts))

        return faidx, input.mapPartitions(libqueries)

class SeqOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.UseRegexp = None  # bool
        self.IgnoreCase = None  # bool
        self.FullHead = None  # bool
        self.RegionFile = None  # str
        self.Regions = None  # list[str]

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "UseRegexp", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "FullHead", False)
        _setDefault(self, "RegionFile", "")
        _setDefault(self, "Regions", [])


def faidx(input: IDataFrame, o: SeqKitFaidxOptions = None, **kwargs):
    if o is None:
        o = SeqKitFaidxOptions()
    return o._run(input, **kwargs)

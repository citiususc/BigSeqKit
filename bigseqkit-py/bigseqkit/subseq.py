from typing import List

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitSubseqOptions:

    def __init__(self):
        self.__inner = SubseqOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def chr(self, v: List[str]):
        self.__inner.Chr = v

    def region(self, v: bool):
        self.__inner.Region = v

    def gtf(self, v: bool):
        self.__inner.Gtf = v

    def feature(self, v: List[str]):
        self.__inner.Feature = v

    def upStream(self, v: int):
        self.__inner.UpStream = v

    def onlyFlank(self, v: bool):
        self.__inner.OnlyFlankOnlyFlank = v

    def bed(self, v: str):
        self.__inner.Bed = v

    def gtfTag(self, v: str):
        self.__inner.GtfTag = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("SubseqTransform").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)


class SubseqOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Chr = None  # list[str]
        self.Region = None  # str
        self.Gtf = None  # str
        self.Feature = None  # list[str]
        self.UpStream = None  # int
        self.DownStream = None  # int
        self.OnlyFlank = None  # bool
        self.Bed = None  # str
        self.GtfTag = None  # str

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Chr", [])
        _setDefault(self, "Region", "")
        _setDefault(self, "Gtf", "")
        _setDefault(self, "Feature", [])
        _setDefault(self, "UpStream", 0)
        _setDefault(self, "DownStream", 0)
        _setDefault(self, "OnlyFlank", False)
        _setDefault(self, "Bed", "")
        _setDefault(self, "GtfTag", "")


def subSeq(input: IDataFrame, o: SeqKitSubseqOptions = None, **kwargs):
    if o is None:
        o = SeqKitSubseqOptions()
    return o._run(input, **kwargs)

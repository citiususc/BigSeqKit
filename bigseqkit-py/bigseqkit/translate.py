from typing import List

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitTranslateOptions:

    def __init__(self):
        self.__inner = TranslateOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def translTable(self, v: int):
        self.__inner.TranslTable = v

    def frame(self, v: List[str]):
        self.__inner.Frame = v

    def trim(self, v: bool):
        self.__inner.Trim = v

    def clean(self, v: bool):
        self.__inner.Clean = v

    def allowUnknownCodon(self, v: bool):
        self.__inner.AllowUnknownCodon = v

    def initCodonAsM(self, v: bool):
        self.__inner.InitCodonAsM = v

    def listTranslTable(self, v: int):
        self.__inner.ListTranslTable = v

    def listTranslTableWithAmbCodons(self, v: int):
        self.__inner.ListTranslTableWithAmbCodons = v

    def appendFrame(self, v: bool):
        self.__inner.AppendFrame = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Translate").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)


class TranslateOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.TranslTable = None  # int
        self.Frame = None  # list[str]
        self.Trim = None  # bool
        self.Clean = None  # bool
        self.AllowUnknownCodon = None  # bool
        self.InitCodonAsM = None  # bool
        self.ListTranslTable = None  # int
        self.ListTranslTableWithAmbCodons = None  # int
        self.AppendFrame = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "TranslTable", 1)
        _setDefault(self, "Frame", ["1"])
        _setDefault(self, "Trim", False)
        _setDefault(self, "Clean", False)
        _setDefault(self, "AllowUnknownCodon", False)
        _setDefault(self, "InitCodonAsM", False)
        _setDefault(self, "ListTranslTable", -1)
        _setDefault(self, "ListTranslTableWithAmbCodons", -1)
        _setDefault(self, "AppendFrame", False)


def translate(input: IDataFrame, o: SeqKitTranslateOptions = None, **kwargs):
    if o is None:
        o = SeqKitTranslateOptions()
    return o._run(input, **kwargs)

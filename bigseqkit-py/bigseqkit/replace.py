from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitReplaceOptions:

    def __init__(self):
        self.__inner = SeqOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def pattern(self, v: str):
        self.__inner.Pattern = v

    def replacement(self, v: str):
        self.__inner.Replacement = v

    def nrWidth(self, v: int):
        self.__inner.NrWidth = v

    def bySeq(self, v: bool):
        self.__inner.BySeq = v

    def ignoreCase(self, v: bool):
        self.__inner.IgnoreCase = v

    def kvFile(self, v: str):
        self.__inner.KvFile = v

    def keepUntouch(self, v: bool):
        self.__inner.KeepUntouch = v

    def keepKey(self, v: bool):
        self.__inner.KeepKey = v

    def keyCaptIdx(self, v: int):
        self.__inner.KeyCaptIdx = v

    def keyMissRepl(self, v: str):
        self.__inner.KeyMissRepl = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Replace").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)

class SeqOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Pattern = None  # string
        self.Replacement = None  # string
        self.NrWidth = None  # int
        self.BySeq = None  # bool
        self.IgnoreCase = None  # bool
        self.KvFile = None  # string
        self.KeepUntouch = None  # bool
        self.KeepKey = None  # bool
        self.KeyCaptIdx = None  # int
        self.KeyMissRepl = None  # string

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Pattern", "")
        _setDefault(self, "Replacement", "")
        _setDefault(self, "NrWidth", 1)
        _setDefault(self, "BySeq", False)
        _setDefault(self, "IgnoreCase", False)
        _setDefault(self, "KvFile", "")
        _setDefault(self, "KeepUntouch", False)
        _setDefault(self, "KeepKey", False)
        _setDefault(self, "KeyCaptIdx", 1)
        _setDefault(self, "KeyMissRepl", "")


def replace(input: IDataFrame, o: SeqKitReplaceOptions = None, **kwargs):
    if o is None:
        o = SeqKitReplaceOptions()
    return o._run(input, **kwargs)

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitDuplicateOptions:

    def __init__(self):
        self.__inner = DuplicateOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def times(self, v: int):
        self.__inner.Times = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Duplicate").addParam("times", opts.Times)
        return input.mapPartitions(libprepare)

class DuplicateOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Times = None  # int

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Times", 1)


def duplicate(input: IDataFrame, o: SeqKitDuplicateOptions = None, **kwargs):
    if o is None:
        o = SeqKitDuplicateOptions()
    return o._run(input, **kwargs)

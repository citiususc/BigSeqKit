from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame
from bigseqkit.range import SeqKitRangeOptions, range

class SeqKitHeadOptions:

    def __init__(self):
        self.__inner = HeadOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def n(self, v: int):
        self.__inner.N = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        oRange = SeqKitRangeOptions().range("1:"+str(opts.N))
        oRange._SeqKitRangeOptions__inner = opts.Config

        return range(input, oRange)

class HeadOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.N = None  # int

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "N", 10)


def head(input: IDataFrame, o: SeqKitHeadOptions = None, **kwargs):
    if o is None:
        o = SeqKitHeadOptions()
    return o._run(input, **kwargs)

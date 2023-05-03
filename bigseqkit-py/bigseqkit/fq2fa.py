from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitFq2FaOptions:

    def __init__(self):
        self.__inner = Fq2FaOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()
        libprepare = _libSource("Fq2Fa").addParam("opts", _optionsToString(opts))
        return input.mapPartitions(libprepare)


class Fq2FaOptions:

    def __init__(self):
        self.Config = None  # KitConfig

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()


def fq2fa(input: IDataFrame, o: SeqKitFq2FaOptions = None, **kwargs):
    if o is None:
        o = SeqKitFq2FaOptions()
    return o._run(input, **kwargs)

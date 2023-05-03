from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitRenameOptions:

    def __init__(self):
        self.__inner = RenameOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def byName(self, v: bool):
        self.__inner.ByName = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        libprepare = _libSource("RenamePrepare").addParam("opts", _optionsToString(opts))
        prepared = input.mapPartitions(libprepare)

        ready = prepared.toPair().groupByKey()

        librename= _libSource("Rename").addParam("opts", _optionsToString(opts))
        return ready.flatmap(librename)

class RenameOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.ByName = None  # bool

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "ByName", False)


def rename(input: IDataFrame, o: SeqKitRenameOptions = None, **kwargs):
    if o is None:
        o = SeqKitRenameOptions()
    return o._run(input, **kwargs)

import re

from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitRangeOptions:

    def __init__(self):
        self.__inner = RangeOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def range(self, v: str):
        self.__inner.Range = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        if opts.Range == "":
            raise RuntimeError("flag -r (--range) needed")

        r = opts.Range.split(":")
        start = int(r[0])
        end = -1
        if len(r) > 1:
            end = int(r[1])

        if start == 0 or end == 0:
            raise RuntimeError("either start and end should not be 0")

        if start > 0:
            start -= 1

        if end == -1:
            end = 1 << 63 - 1

        if start < - 1 or end < -1:
            n = input.count()
            if start < 0:
                start += n
            if end < 0:
                end += n

        if start <= end:
            raise RuntimeError("start must be > than end")

        libprepare = _libSource("RangePrepare").addParam("start", start).addParam("end", end)
        prepared = input.mapWithIndex(libprepare)

        return prepared.filter(_libSource("RangeFilter"))

class RangeOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Range = None  # str

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Range", "")


def range(input: IDataFrame, o: SeqKitRangeOptions = None, **kwargs):
    if o is None:
        o = SeqKitRangeOptions()
    return o._run(input, **kwargs)

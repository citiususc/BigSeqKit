from bigseqkit.helper import _setDefault, _libSource, _config, _optionsToString, _parseKargs, SeqKitConfig, IDataFrame


class SeqKitSampleOptions:

    def __init__(self):
        self.__inner = SampleOptions()

    def config(self, v: SeqKitConfig):
        self.__inner.Config = _config(v)

    def seed(self, v: int):
        self.__inner.Seed = v

    def number(self, v: int):
        self.__inner.Number = v

    def proportion(self, v: float):
        self.__inner.Proportion = v

    def _run(self, input: IDataFrame, **kwargs):
        opts = self.__inner
        _parseKargs(opts, kwargs)
        opts.setDefaults()

        if opts.Number == 0 and opts.Proportion == 0:
            raise RuntimeError("one of flags -n (--number) and -p (--proportion) needed")

        if opts.Number < 0:
            raise RuntimeError("value of -n (--number) and should be greater than 0")

        if opts.Proportion < 0 or opts.Proportion > 1:
            raise RuntimeError(f"value of -p (--proportion) ({str(opts.Proportion)}) should be in range of (0, 1]")

        fraction = opts.Proportion
        if opts.Number > 0:
            n = input.count()
            fraction = opts.Number / n

        return input.sample(False, fraction, opts.Seed)

class SampleOptions:

    def __init__(self):
        self.Config = None  # KitConfig
        self.Seed = None  # int
        self.Number = None  # int
        self.Proportion = None  # float

    def setDefaults(self):
        _setDefault(self, "Config", _config(SeqKitConfig())).setDefaults()
        _setDefault(self, "Seed", 11)
        _setDefault(self, "Number", 0)
        _setDefault(self, "Proportion", 0)


def sample(input: IDataFrame, o: SeqKitSampleOptions = None, **kwargs):
    if o is None:
        o = SeqKitSampleOptions()
    return o._run(input, **kwargs)
